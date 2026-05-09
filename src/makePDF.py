import os
import sys
import time
import json
import requests
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright
from scrapling.parser import Selector
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor

# --- Session Setup ---
session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"})
# Reduce retry attempts and use aggressive timeouts
retries = Retry(total=2, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))

def get_chrome_testing_user_data_dir():
    """Determines the path to the Chrome for Testing profile."""
    if sys.platform == "win32":
        return os.path.join(os.environ["LOCALAPPDATA"], "Google", "Chrome for Testing", "User Data")
    elif sys.platform == "darwin":
        return os.path.expanduser("~/Library/Application Support/Google/Chrome for Testing")
    else:
        return os.path.expanduser("~/.config/google-chrome-for-testing")

def download_media(url, save_path):
    """Downloads a media file via a direct link with a short timeout."""
    try:
        # timeout=(3, 7): 3s for connection (resolves dead DNS), 7s for download
        response = session.get(url, stream=True, timeout=(3, 7))
        response.raise_for_status()
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(8192):
                f.write(chunk)
    except Exception:
        # Ignore errors (broken links, dead CDNs) to avoid console spam
        pass

def save_json_data(scraped_msgs):
    """Dynamically save data to JSON."""
    sorted_msgs = sorted(scraped_msgs.values(), key=lambda x: int(x["id"]))
    with open("disc_msgs.json", "w", encoding="utf-8") as f:
        json.dump(sorted_msgs, f, ensure_ascii=False, indent=4)

def scrape_discord_messages():
    MEDIA_DIR = "./media"
    os.makedirs(MEDIA_DIR, exist_ok=True)

    TARGET_URL = "https://discord.com/channels/@me" 
    
    # Thread pool for background media downloading (up to 10 files simultaneously)
    executor = ThreadPoolExecutor(max_workers=10)
    
    with sync_playwright() as p:
        executable_path = p.chromium.executable_path
        user_data_dir = get_chrome_testing_user_data_dir()
        
        print(f"Launching Chrome with profile: {user_data_dir}")
        browser = p.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            executable_path=executable_path,
            headless=False,
            args=["--start-maximized"],
            no_viewport=True,
        )
        
        page = browser.pages[0] if browser.pages else browser.new_page()
        page.goto(TARGET_URL, wait_until="domcontentloaded")
        
        input("Press Enter once you have opened the desired chat to start collecting...")
        
        scraped_msgs = {} 
        no_new_msgs_count = 0
        previous_count = 0
        current_author = "Unknown"

        page.locator('[data-list-id="chat-messages"]').click()
        print("\n=== Collection started. Press CTRL+C to stop ===")

        try:
            while True:
                html_content = page.content()
                selector = Selector(html_content)
                messages = selector.css('li[class*="messageListItem_"]')
                
                new_messages_in_batch = False

                for msg in messages:
                    msg_id_attr = msg.css('::attr(id)').get()
                    if not msg_id_attr or "chat-messages-" not in msg_id_attr:
                        continue
                    
                    msg_id = msg_id_attr.split('-')[-1]

                    if msg_id in scraped_msgs:
                        continue

                    # Author detection
                    author_elem = msg.css('span[class*="username_"] ::text').getall()
                    if author_elem:
                        current_author = "".join(author_elem).strip()

                    text_parts = msg.css('div[class*="messageContent_"] ::text').getall()
                    full_text = "".join(text_parts).strip()
                    date = msg.css('time::attr(datetime)').get()

                    # Media detection
                    media_urls = []
                    media_urls.extend(msg.css('a[class*="originalLink_"]::attr(href)').getall())
                    media_urls.extend(msg.css('video::attr(src)').getall())
                    media_urls = list(set(media_urls))
                    
                    local_media_paths = []

                    for url in media_urls:
                        parsed_url = urlparse(url)
                        filename = os.path.basename(parsed_url.path)
                        if not filename:
                            filename = f"media_{msg_id}.dat"
                            
                        save_filename = f"{msg_id}_{filename}"
                        filepath = os.path.join(MEDIA_DIR, save_filename)
                        local_media_paths.append(os.path.abspath(filepath))
                        
                        # --- AVAILABILITY CHECK AND BACKGROUND DOWNLOAD ---
                        if not os.path.exists(filepath):
                            # Send to background; the script does not wait for the download to finish!
                            executor.submit(download_media, url, filepath)

                    scraped_msgs[msg_id] = {
                        "id": msg_id,
                        "author": current_author,
                        "date": date,
                        "text": full_text,
                        "local_media": local_media_paths
                    }
                    new_messages_in_batch = True

                # Scroll up and check limit
                if len(scraped_msgs) == previous_count:
                    no_new_msgs_count += 1
                    if no_new_msgs_count >= 500:
                        print("\n[!] 500 scrolls with no new messages. Limit reached (start of chat). Auto-stopping.")
                        break
                else:
                    no_new_msgs_count = 0
                    
                previous_count = len(scraped_msgs)
                
                # Dynamically save JSON if new messages were found
                if new_messages_in_batch:
                    save_json_data(scraped_msgs)
                
                print(f"Collected {len(scraped_msgs)} messages. Idle scrolls: {no_new_msgs_count}/500. Scrolling up...", end="\r")
                
                page.keyboard.press("PageUp")
                time.sleep(2)
                
        except KeyboardInterrupt:
            print("\n\n[!] Stop signal received (CTRL+C). Terminating collection...")
        finally:
            print("\nWaiting for background downloads to finish, please wait a few seconds...")
            # Wait for background download threads (to prevent file corruption)
            executor.shutdown(wait=True)
            
            print("Saving final JSON...")
            save_json_data(scraped_msgs)
            
            # Suppress browser closure errors (useful during CTRL+C)
            try:
                browser.close()
            except Exception:
                pass
            
            print("Process completed successfully!")

if __name__ == "__main__":
    scrape_discord_messages()
