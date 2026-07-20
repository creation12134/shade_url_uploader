import requests

def get_input(prompt, default_value):
    """Helper to get user input with a fallback default value."""
    user_val = input(f"{prompt} [{default_value}]: ").strip()
    return user_val if user_val else default_value

def trigger_transfer():
    print("--- Configure Transfer Details ---")
    
    # Gather dynamic inputs from the user
    url = get_input("Enter target URL", "https://your-render-app.onrender.com/transfer")
    server_secret = get_input("Enter server secret", "some-long-random-string")
    source_url = get_input("Enter source video URL", "https://example.com/big-video.mp4")
    drive_id = get_input("Enter Drive ID", "your-drive-id")
    api_key = get_input("Enter Shade API Key", "your-shade-api-key")
    dest_path = get_input("Enter destination path", "/videos/big-video.mp4")
    
    # Construct payload and headers
    data = {
        "sourceUrl": source_url,
        "driveId": drive_id,
        "apiKey": api_key,
        "destPath": dest_path
    }

    headers = {
        "content-type": "application/json",
        "x-server-secret": server_secret
    }

    print("\nInitiating stream transfer...")
    
    try:
        # stream=True mimics the curl -N flag for real-time console updates
        with requests.post(url, json=data, headers=headers, stream=True) as response:
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    print(chunk.decode('utf-8', errors='ignore'), end='', flush=True)
                    
    except requests.exceptions.RequestException as e:
        print(f"\n An error occurred: {e}")

if __name__ == "__main__":
    trigger_transfer()