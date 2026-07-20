// server.js
//
// Single-file Node.js server that:
//   1. Accepts a POST request with { sourceUrl, driveId, apiKey, destPath }
//   2. Streams the download from sourceUrl (never buffers the whole file)
//   3. Streams it straight into Shade's multipart upload API
//      (https://fs.shade.inc), part by part, without ever writing
//      the file to disk.
//
// Requires Node.js >= 18 (built-in fetch / streams). No npm dependencies.
//
// Deploy target: Render (Web Service, plain Node). NOT Cloudflare Pages
// Functions — those have short wall-clock/CPU limits and no long-lived
// process, so a multi-GB transfer will get killed partway through.
//
// ---------------------------------------------------------------------
// Run locally:
//   PORT=3000 SERVER_SECRET=changeme node server.js
//
// Call it:
//   curl -N -X POST http://localhost:3000/transfer \
//     -H "content-type: application/json" \
//     -H "x-server-secret: changeme" \
//     -d '{
//           "sourceUrl": "https://example.com/big-video.mp4",
//           "driveId": "your-drive-id",
//           "apiKey": "your-shade-api-key",
//           "destPath": "/videos/big-video.mp4"
//         }'
//
// The response is streamed back as newline-delimited JSON progress events.
// ---------------------------------------------------------------------

'use strict';

const http = require('http');

// Never let an unhandled rejection or exception die silently.
process.on('unhandledRejection', (reason) => {
  console.error('[FATAL] Unhandled rejection:', reason);
});
process.on('uncaughtException', (err) => {
  console.error('[FATAL] Uncaught exception:', err);
});

const PORT = process.env.PORT || 3000;
// Set this on Render as an env var and require it on every request so
// randos on the internet can't use your server as a free bandwidth relay.
const SERVER_SECRET = process.env.SERVER_SECRET || null;

const DEFAULT_PART_SIZE = 64 * 1024 * 1024; // 64MB, per Shade's own recommendation
const MUST_BE_LONGER_THAN_TO_BE_VALID = 240; // seconds, per Shade docs
const MAX_PART_UPLOAD_RETRIES = 5;

// ---------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------

function jwtDecode(token) {
  const parts = token.split('.');
  if (parts.length < 2) throw new Error('Malformed JWT');
  const payload = Buffer.from(parts[1], 'base64url').toString('utf8');
  return JSON.parse(payload);
}

async function jsonOrThrow(resp, label) {
  if (!resp.ok) {
    const body = await resp.text().catch(() => '');
    throw new Error(`${label} failed: ${resp.status} ${resp.statusText} ${body}`);
  }
  return resp.json();
}

function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

const DEFAULT_FETCH_TIMEOUT_MS = 2 * 60 * 1000; // 2 min for small API calls

async function fetchWithTimeout(label, url, options = {}, timeoutMs = DEFAULT_FETCH_TIMEOUT_MS) {
  const controller = new AbortController();
  const startedAt = Date.now();
  const timeoutHandle = setTimeout(() => {
    console.error(`[fetch] ${label} TIMED OUT after ${timeoutMs}ms, aborting: ${url}`);
    controller.abort(new Error(`${label} timeout`));
  }, timeoutMs);
  try {
    const resp = await fetch(url, { ...options, signal: controller.signal });
    console.log(`[fetch] ${label} -> status=${resp.status} elapsedMs=${Date.now() - startedAt}`);
    return resp;
  } catch (err) {
    console.error(`[fetch] ${label} threw after ${Date.now() - startedAt}ms: ${err.message}`);
    throw err;
  } finally {
    clearTimeout(timeoutHandle);
  }
}

// ---------------------------------------------------------------------
// ShadeFS token cache (mirrors tokenCacher.fetchToken() from the docs)
// ---------------------------------------------------------------------

class TokenCacher {
  constructor(driveId, apiKey) {
    this.driveId = driveId;
    this.apiKey = apiKey;
    this.token = null;
    this.exp = null;
  }

  async fetchToken() {
    if (this.token && this.exp && (this.exp - Date.now() / 1000) >= MUST_BE_LONGER_THAN_TO_BE_VALID) {
      return this.token;
    }

    const resp = await fetchWithTimeout(
      'shade-fs-token',
      `https://api.shade.inc/workspaces/drives/${encodeURIComponent(this.driveId)}/shade-fs-token`,
      { headers: { Authorization: this.apiKey } }
    );
    if (!resp.ok) {
      const body = await resp.text().catch(() => '');
      throw new Error(`shade-fs-token failed: ${resp.status} ${resp.statusText} ${body}`);
    }

    // The endpoint returns the raw JWT as plain text (not wrapped in JSON),
    // so read it as text rather than trying resp.json().
    const raw = (await resp.text()).trim();
    let token = raw;
    // Just in case it's ever double-quoted JSON-string or {"token":"..."}.
    if (raw.startsWith('"') || raw.startsWith('{')) {
      try {
        const parsed = JSON.parse(raw);
        token = typeof parsed === 'string' ? parsed : parsed.token;
      } catch {
        // not JSON after all, fall through with raw as token
      }
    }
    if (!token) throw new Error('shade-fs-token response missing token');

    const decoded = jwtDecode(token);
    if (!decoded.exp) throw new Error('No exp attribute in decoded jwt');

    this.token = token;
    this.exp = decoded.exp;
    return token;
  }
}

// ---------------------------------------------------------------------
// Shade API calls (all straight ports of the docs, using fetch instead
// of axios)
// ---------------------------------------------------------------------

async function makeDirectory(tokenCacher, drive, email, directory) {
  console.log(`[mkdir] requesting mkdir drive=${drive} email=${email} directory=${directory}`);
  const token = await tokenCacher.fetchToken();
  const url = new URL(`https://fs.shade.inc/${drive}/fs/mkdir`);
  url.searchParams.set('email', email);
  url.searchParams.set('path', directory);
  url.searchParams.set('drive', drive);

  const resp = await fetchWithTimeout('mkdir', url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!resp.ok && resp.status !== 409) {
    // 409/"already exists" style responses are fine; anything else isn't.
    const body = await resp.text().catch(() => '');
    throw new Error(`mkdir failed: ${resp.status} ${body}`);
  }
}

async function initiateMultipartUpload(tokenCacher, drive, path, partSize) {
  console.log(`[initiate] drive=${drive} path=${path} partSize=${partSize}`);
  const token = await tokenCacher.fetchToken();
  const resp = await fetchWithTimeout('initiate', `https://fs.shade.inc/${drive}/upload/multipart`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ path, partSize }),
  });
  const result = await jsonOrThrow(resp, 'initiateMultipartUpload');
  console.log(`[initiate] response: ${JSON.stringify(result)}`);
  if (!result.uploadId) {
    console.warn('[initiate] WARNING: response has no uploadId field — check API response shape above');
  }
  if (!result.token) {
    console.warn('[initiate] WARNING: response has no token (finishToken) field — subsequent calls will fail');
  }
  return result;
}

async function presignPart(tokenCacher, drive, partNumber, finishToken) {
  console.log(`[presign] requesting presign for part ${partNumber}`);
  const token = await tokenCacher.fetchToken();
  const url = new URL(`https://fs.shade.inc/${drive}/upload/multipart/part/${partNumber}`);
  url.searchParams.set('token', finishToken);

  const resp = await fetchWithTimeout(`presign(${partNumber})`, url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}` },
  });
  const result = await jsonOrThrow(resp, `presignPart(${partNumber})`);
  console.log(`[presign] part ${partNumber} presigned url host=${new URL(result.url).host}`);
  return result;
}

const PART_UPLOAD_TIMEOUT_MS = 5 * 60 * 1000; // 5 min per attempt, then abort + retry

async function uploadPart(presigned, buffer, partNumber) {
  let lastErr;
  for (let attempt = 1; attempt <= MAX_PART_UPLOAD_RETRIES; attempt++) {
    console.log(`[upload] part ${partNumber} attempt ${attempt}/${MAX_PART_UPLOAD_RETRIES}, ${buffer.length} bytes -> ${presigned.url}`);
    const startedAt = Date.now();
    const controller = new AbortController();
    const timeoutHandle = setTimeout(() => {
      console.error(`[upload] part ${partNumber} attempt ${attempt} TIMED OUT after ${PART_UPLOAD_TIMEOUT_MS}ms, aborting`);
      controller.abort(new Error('part upload timeout'));
    }, PART_UPLOAD_TIMEOUT_MS);

    try {
      const resp = await fetch(presigned.url, {
        method: 'PUT',
        headers: {
          'Content-Length': String(buffer.length),
          ...(presigned.headers || {}),
        },
        body: buffer,
        signal: controller.signal,
      });
      const elapsedMs = Date.now() - startedAt;
      console.log(`[upload] part ${partNumber} response status=${resp.status} elapsedMs=${elapsedMs}`);
      if (!(resp.status >= 200 && resp.status < 300)) {
        throw new Error(`UploadPart failed: ${resp.status} ${await resp.text().catch(() => '')}`);
      }
      const etag = resp.headers.get('etag');
      if (!etag) {
        const headerDump = [...resp.headers.entries()].map(([k, v]) => `${k}=${v}`).join(', ');
        throw new Error(`Missing ETag on UploadPart response. Headers received: ${headerDump}`);
      }
      console.log(`[upload] part ${partNumber} succeeded, etag=${etag}, elapsedMs=${elapsedMs}`);
      return etag;
    } catch (err) {
      const elapsedMs = Date.now() - startedAt;
      lastErr = err;
      console.error(`[upload] part ${partNumber} attempt ${attempt} failed after ${elapsedMs}ms: ${err.message}`);
      const backoff = Math.min(1000 * 2 ** (attempt - 1), 15000);
      console.log(`[upload] part ${partNumber} backing off ${backoff}ms before retry`);
      await sleep(backoff);
    } finally {
      clearTimeout(timeoutHandle);
    }
  }
  throw lastErr;
}

async function completeMultipart(tokenCacher, drive, finishToken, parts) {
  console.log(`[complete] finalizing with ${parts.length} parts: ${JSON.stringify(parts)}`);
  const token = await tokenCacher.fetchToken();
  const url = new URL(`https://fs.shade.inc/${drive}/upload/multipart/complete`);
  url.searchParams.set('token', finishToken);

  const resp = await fetchWithTimeout('complete', url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ parts }),
  });
  const bodyText = await resp.text().catch(() => '');
  console.log(`[complete] response status=${resp.status} body=${bodyText}`);
  if (!(resp.status >= 200 && resp.status < 300)) {
    throw new Error(`completeMultipart ${resp.status} ${bodyText}`);
  }
}

// ---------------------------------------------------------------------
// Streaming glue: read the source download stream, accumulate bytes
// until we have a full part, ship the part out, repeat. Never holds
// more than ~1 part's worth of bytes in memory (aside from small
// in-flight overlap), and never touches disk.
// ---------------------------------------------------------------------

async function* chunkStreamIntoParts(readableStream, partSize) {
  const reader = readableStream.getReader();
  let pending = [];
  let pendingLen = 0;
  let totalReadFromSource = 0;
  let readCount = 0;

  while (true) {
    const { value, done } = await reader.read();
    readCount += 1;
    if (value) {
      totalReadFromSource += value.length;
      pending.push(Buffer.from(value));
      pendingLen += value.length;
      if (readCount % 50 === 0 || readCount <= 5) {
        console.log(
          `[download] read#${readCount} chunkBytes=${value.length} totalReadFromSource=${totalReadFromSource} pendingLen=${pendingLen}`
        );
      }
    } else if (!done) {
      console.log(`[download] read#${readCount} produced no value and not done yet`);
    }

    while (pendingLen >= partSize) {
      const combined = pending.length === 1 ? pending[0] : Buffer.concat(pending, pendingLen);
      const part = combined.subarray(0, partSize);
      const rest = combined.subarray(partSize);
      pending = rest.length ? [rest] : [];
      pendingLen = rest.length;
      console.log(`[download] assembled full part of ${part.length} bytes, ${pendingLen} bytes carried over`);
      yield part;
    }

    if (done) {
      console.log(
        `[download] source stream done. totalReadFromSource=${totalReadFromSource} leftover pendingLen=${pendingLen}`
      );
      if (pendingLen > 0) {
        const finalPart = pending.length === 1 ? pending[0] : Buffer.concat(pending, pendingLen);
        console.log(`[download] yielding final partial part of ${finalPart.length} bytes`);
        yield finalPart;
      }
      return;
    }
  }
}

async function transferUrlToShade({ sourceUrl, driveId, apiKey, destPath, partSizeBytes }, onEvent) {
  const partSize = partSizeBytes || DEFAULT_PART_SIZE;
  const tokenCacher = new TokenCacher(driveId, apiKey);
  const log = (evt) => {
    console.log(`[transfer] ${JSON.stringify(evt)}`);
    onEvent(evt);
  };

  log({ stage: 'auth', message: 'fetching ShadeFS token' });
  const token = await tokenCacher.fetchToken();
  const decoded = jwtDecode(token);
  const drive = decoded.aud;
  const userEmail = decoded.sub;
  log({ stage: 'auth-ok', drive, userEmail, tokenExp: decoded.exp });
  if (!userEmail || !drive || Array.isArray(drive)) {
    throw new Error('Bad token: missing sub/aud');
  }

  const directory = destPath.substring(0, destPath.lastIndexOf('/'));
  if (directory && directory !== '/') {
    log({ stage: 'mkdir', message: `ensuring ${directory} exists` });
    await makeDirectory(tokenCacher, drive, userEmail, directory);
  } else {
    log({ stage: 'mkdir', message: 'destination is at drive root, skipping mkdir' });
  }

  log({ stage: 'download-start', message: `starting download of ${sourceUrl}` });
  const sourceResp = await fetchWithTimeout('download-connect', sourceUrl, {}, DEFAULT_FETCH_TIMEOUT_MS);
  log({
    stage: 'download-response',
    status: sourceResp.status,
    contentLength: sourceResp.headers.get('content-length'),
    contentType: sourceResp.headers.get('content-type'),
    contentEncoding: sourceResp.headers.get('content-encoding'),
    acceptRanges: sourceResp.headers.get('accept-ranges'),
  });
  if (!sourceResp.ok) {
    throw new Error(`Failed to fetch sourceUrl: ${sourceResp.status} ${sourceResp.statusText}`);
  }
  if (!sourceResp.body) {
    throw new Error('sourceUrl response had no readable body');
  }

  log({ stage: 'initiate', message: 'initiating multipart upload' });
  const init = await initiateMultipartUpload(tokenCacher, drive, destPath, partSize);
  const finishToken = init.token;
  const confirmedPartSize = init.partSize || partSize;
  if (!finishToken) {
    throw new Error('initiateMultipartUpload did not return a finish token — cannot continue');
  }
  log({ stage: 'initiate-ok', uploadId: init.uploadId, confirmedPartSize });

  const completed = [];
  let partNumber = 1;
  let bytesUploaded = 0;

  // Heartbeat so the client (and any proxy in front of this server) sees
  // this is still alive during long, quiet stretches between parts.
  const heartbeat = setInterval(() => {
    log({ stage: 'heartbeat', partsSoFar: completed.length, bytesUploaded });
  }, 15000);

  try {
    for await (const partBuffer of chunkStreamIntoParts(sourceResp.body, confirmedPartSize)) {
      log({ stage: 'part-ready', partNumber, bytes: partBuffer.length });
      const presigned = await presignPart(tokenCacher, drive, partNumber, finishToken);
      const etag = await uploadPart(presigned, partBuffer, partNumber);
      completed.push({ partNumber, etag });
      bytesUploaded += partBuffer.length;
      log({
        stage: 'part-uploaded',
        partNumber,
        bytesInPart: partBuffer.length,
        totalBytesUploaded: bytesUploaded,
      });
      partNumber += 1;
    }
  } finally {
    clearInterval(heartbeat);
  }

  if (completed.length === 0 || bytesUploaded === 0) {
    throw new Error(
      'No bytes were read from sourceUrl — the download stream ended without producing any data. ' +
      'Check that sourceUrl is directly downloadable (not an HTML landing page / redirect chain requiring a browser).'
    );
  }

  log({ stage: 'completing', message: `finalizing multipart upload with ${completed.length} parts` });
  await completeMultipart(tokenCacher, drive, finishToken, completed);

  log({
    stage: 'done',
    message: 'upload complete',
    totalParts: completed.length,
    totalBytesUploaded: bytesUploaded,
    destPath,
  });
}

// ---------------------------------------------------------------------
// HTTP server
// ---------------------------------------------------------------------

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let raw = '';
    req.on('data', (chunk) => (raw += chunk));
    req.on('end', () => {
      try {
        resolve(raw ? JSON.parse(raw) : {});
      } catch (err) {
        reject(new Error('Invalid JSON body'));
      }
    });
    req.on('error', reject);
  });
}

const server = http.createServer(async (req, res) => {
  if (req.method === 'GET' && req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  if (req.method !== 'POST' || req.url !== '/transfer') {
    res.writeHead(404, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: 'not found' }));
    return;
  }

  if (SERVER_SECRET && req.headers['x-server-secret'] !== SERVER_SECRET) {
    res.writeHead(401, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: 'unauthorized' }));
    return;
  }

  let body;
  try {
    body = await readJsonBody(req);
  } catch (err) {
    res.writeHead(400, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: err.message }));
    return;
  }

  const { sourceUrl, driveId, apiKey, destPath, partSizeMB } = body;
  console.log(`[request] /transfer sourceUrl=${sourceUrl} driveId=${driveId} destPath=${destPath} partSizeMB=${partSizeMB || 'default'}`);
  const missing = ['sourceUrl', 'driveId', 'apiKey', 'destPath'].filter((k) => !body[k]);
  if (missing.length) {
    res.writeHead(400, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: `missing fields: ${missing.join(', ')}` }));
    return;
  }

  // Stream progress back as NDJSON so you can watch it happen on a
  // multi-GB transfer instead of waiting on one giant response.
  res.writeHead(200, {
    'content-type': 'application/x-ndjson',
    'transfer-encoding': 'chunked',
    'cache-control': 'no-cache',
    'x-accel-buffering': 'no', // hint to nginx-style proxies: don't buffer this
  });
  if (typeof res.flushHeaders === 'function') res.flushHeaders();

  const write = (evt) => {
    try {
      res.write(JSON.stringify(evt) + '\n');
    } catch (err) {
      console.error(`[response] failed writing to client stream: ${err.message}`);
    }
  };

  try {
    await transferUrlToShade(
      {
        sourceUrl,
        driveId,
        apiKey,
        destPath,
        partSizeBytes: partSizeMB ? Math.round(partSizeMB * 1024 * 1024) : undefined,
      },
      write
    );
    console.log('[request] /transfer completed successfully');
  } catch (err) {
    console.error(`[request] /transfer failed: ${err.stack || err.message}`);
    write({ stage: 'error', message: err.message });
  } finally {
    res.end();
  }
});

server.listen(PORT, () => {
  console.log(`Shade transfer server listening on :${PORT}`);
  if (!SERVER_SECRET) {
    console.warn('WARNING: SERVER_SECRET is not set — /transfer is open to anyone who can reach this server.');
  }
});