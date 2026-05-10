import os
import sys
import time
import json
import requests
import subprocess
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from playwright.sync_api import sync_playwright
from scrapling.parser import Selector
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Session setup for stable image downloads ---
session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))

# Блокировка для безопасного сохранения JSON из фоновых потоков
json_lock = threading.Lock()

def get_chrome_testing_user_data_dir():
    """Determines the path to the Chrome for Testing profile directory."""
    if sys.platform == "win32":
        return os.path.join(os.environ["LOCALAPPDATA"], "Google", "Chrome for Testing", "User Data")
    elif sys.platform == "darwin":
        return os.path.expanduser("~/Library/Application Support/Google/Chrome for Testing")
    else:
        return os.path.expanduser("~/.config/google-chrome-for-testing")

def download_image(url, save_path):
    """Downloads an image in maximum quality (fullsize for Bluesky)."""
    url = url.replace('/feed_thumbnail/', '/feed_fullsize/')
    try:
        response = session.get(url, stream=True, timeout=30)
        response.raise_for_status()
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(8192):
                f.write(chunk)
        return True
    except Exception as e:
        print(f"Error downloading image {url}: {e}")
        return False

def save_json_data(scraped_posts):
    """Dynamically save data to JSON. Thread-safe."""
    with json_lock:
        with open("bsky_bookmarks.json", "w", encoding="utf-8") as f:
            json.dump(list(scraped_posts.values()), f, ensure_ascii=False, indent=4)

def load_json_data():
    """Loads existing data to prevent re-processing."""
    if os.path.exists("bsky_bookmarks.json"):
        try:
            with open("bsky_bookmarks.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                return {item["url"]: item for item in data}
        except Exception as e:
            print(f"Error loading existing JSON: {e}")
    return {}

def download_video_async(post_url, post_id, cookie_file, media_dir, scraped_posts):
    """Background task for downloading videos via yt-dlp."""
    video_filename_template = f"{media_dir}/{post_id}_video.%(ext)s"
    cmd =["yt-dlp", "--cookies", cookie_file, "-o", video_filename_template, post_url]
    try:
        subprocess.run(cmd, check=True, capture_output=True)
        # Ищем скачанный файл (yt-dlp мог задать ему расширение .mp4, .webm и т.д.)
        for file in os.listdir(media_dir):
            if file.startswith(f"{post_id}_video") and not file.endswith('.part') and not file.endswith('.ytdl'):
                file_path = os.path.abspath(os.path.join(media_dir, file))
                # Добавляем путь в общий словарь, если его там еще нет
                if file_path not in scraped_posts[post_url]["local_media"]:
                    scraped_posts[post_url]["local_media"].append(file_path)
        
        # Сохраняем обновленный JSON
        save_json_data(scraped_posts)
        print(f"\n[+] Background video download finished: {post_url}")
    except Exception as e:
        print(f"\n[!] Background yt-dlp error for {post_url}: {e}")

def scrape_bookmarks_media():
    MEDIA_DIR = "./media"
    COOKIE_FILE = "bsky_cookies.txt"
    os.makedirs(MEDIA_DIR, exist_ok=True)
    
    # Создаем пул потоков (максимум 3 одновременных загрузки видео)
    executor = ThreadPoolExecutor(max_workers=3)
    
    # Загружаем уже собранные посты
    scraped_posts = load_json_data()
    if scraped_posts:
        print(f"[*] Loaded {len(scraped_posts)} previously scraped posts from JSON.")

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
        
        # --- ШАГ 1: ОЖИДАНИЕ ПОЛЬЗОВАТЕЛЯ ---
        page.goto("https://bsky.app/saved")
        print("\n" + "="*50)
        print("STEPS: Go to the desired page in your browser (saved/bookmarks, profile, etc.)")
        print("Make sure you are logged in to Bluesky.")
        print("="*50)
        input("\nWhen you're ready to start collecting data, press ENTER on this terminal...")

        # --- ШАГ 2: ПРОВЕРКА URL ---
        current_url = page.url
        print(f"Current URL: {current_url}")
        
        if current_url.rstrip('/') in["https://bsky.app", "https://bsky.app/home"]:
            print("\n[!] WARNING: You are on the main page or in the feed (Home).")
            print("[!] The script will only collect what is visible in the feed, not your saved posts.")
            confirm = input("Continue anyway? (y/n): ")
            if confirm.lower() != 'y':
                browser.close()
                return

        # --- ШАГ 3: ЭКСПОРТ КУКИ ---
        print("Refreshing cookies for yt-dlp...")
        cookies = browser.cookies()
        with open(COOKIE_FILE, "w", encoding="utf-8") as f:
            f.write("# Netscape HTTP Cookie File\n")
            for c in cookies:
                domain = c['domain']
                flag = "TRUE" if domain.startswith('.') else "FALSE"
                path = c['path']
                secure = "TRUE" if c['secure'] else "FALSE"
                expires = str(int(c.get('expires', 0)))
                name = c['name']
                value = c['value']
                f.write(f"{domain}\t{flag}\t{path}\t{secure}\t{expires}\t{name}\t{value}\n")
        
        no_new_posts_count = 0
        previous_count = len(scraped_posts)

        print("\n=== Collection started. Press CTRL+C to stop ===")

        try:
            while True:
                # ВМЕСТО page.content() мы просим браузер найти только НОВЫЕ (еще не обработанные) посты
                new_posts_html = page.evaluate('''() => {
                    const posts = document.querySelectorAll('[data-testid^="feedItem-by-"]:not([data-scraped="true"])');
                    const results =[];
                    for (const p of posts) {
                        p.setAttribute('data-scraped', 'true'); // Помечаем, чтобы не брать повторно
                        results.push(p.outerHTML);
                    }
                    return results;
                }''')
                
                new_posts_in_batch = False

                for post_html in new_posts_html:
                    post = Selector(post_html)
                    
                    # Ищем ссылку на сам пост
                    links = post.css('a::attr(href)').getall()
                    post_path = next((link for link in links if '/post/' in link), None)
                    
                    if not post_path:
                        continue
                        
                    post_url = f"https://bsky.app{post_path}"
                    post_id = post_path.split('/')[-1]

                    # Пропуск, если пост уже сохранён
                    if post_url in scraped_posts:
                        continue

                    img_urls = post.css('img[src*="/feed_thumbnail/"]::attr(src), img[src*="/feed_fullsize/"]::attr(src)').getall()
                    has_video = bool(post.css('video,[aria-label*="video"], [aria-label*="видео"],[aria-label*="Видео"], [data-testid="playButton"]').get())
                    
                    text_parts = post.css('[data-testid="postText"] ::text').getall()
                    full_text = "".join(text_parts).strip()
                    
                    date_elem = post.css('a[href*="/post/"][data-tooltip], a[href*="/post/"][aria-label]')
                    date = date_elem.css('::attr(data-tooltip)').get()
                    if not date:
                        date = date_elem.css('::attr(aria-label)').get()

                    local_media_paths =[]

                    # ПРОВЕРКА: скачивали ли мы уже это видео (ищем файл начинающийся на {post_id}_video)
                    video_exists = False
                    if has_video:
                        for file in os.listdir(MEDIA_DIR):
                            if file.startswith(f"{post_id}_video") and not file.endswith('.part') and not file.endswith('.ytdl'):
                                video_exists = True
                                local_media_paths.append(os.path.abspath(os.path.join(MEDIA_DIR, file)))
                                break

                    for idx, img_url in enumerate(img_urls):
                        ext = "jpg"
                        filename = f"{post_id}_img_{idx}.{ext}"
                        filepath = os.path.join(MEDIA_DIR, filename)
                        
                        # ПРОВЕРКА: существует ли картинка, чтобы не качать заново
                        if os.path.exists(filepath):
                            local_media_paths.append(os.path.abspath(filepath))
                        else:
                            if download_image(img_url, filepath):
                                local_media_paths.append(os.path.abspath(filepath))

                    # Создаем запись в словаре СРАЗУ, чтобы фоновый поток мог добавить в нее скачанное видео позже
                    scraped_posts[post_url] = {
                        "url": post_url,
                        "date": date or "Unknown",
                        "text": full_text,
                        "local_media": local_media_paths
                    }
                    new_posts_in_batch = True

                    # Отправляем видео на скачивание в фоне (не блокирует скроллинг)
                    if has_video and not video_exists:
                        print(f"\n[+] Queueing background video download: {post_url}")
                        executor.submit(download_video_async, post_url, post_id, COOKIE_FILE, MEDIA_DIR, scraped_posts)

                # Логика выхода (100 скроллов безрезультатно)
                if len(scraped_posts) == previous_count:
                    no_new_posts_count += 1
                    if no_new_posts_count >= 100:
                        print("\n\n[!] 100 scrolls with no new posts. Limit reached (end of feed). Auto-stopping.")
                        break
                else:
                    no_new_posts_count = 0
                    
                previous_count = len(scraped_posts)
                
                if new_posts_in_batch:
                    save_json_data(scraped_posts)

                print(f"Collected {len(scraped_posts)} posts. Idle scrolls: {no_new_posts_count}/100. Scrolling down...", end="\r")
                page.keyboard.press("PageDown")
                time.sleep(0.2)

        except KeyboardInterrupt:
            print("\n\n[!] Stop signal received (CTRL+C). Terminating collection...")
        
        finally:
            print("\nSaving final JSON...")
            save_json_data(scraped_posts)
            
            if os.path.exists(COOKIE_FILE):
                try:
                    os.remove(COOKIE_FILE)
                except Exception:
                    pass
            
            try:
                browser.close()
            except Exception:
                pass
            
            print(f"Process completed successfully! Total saved: {len(scraped_posts)} posts.")
            print("Waiting for background video downloads to finish (if any)... Press CTRL+C again to force quit.")
            executor.shutdown(wait=True)

if __name__ == "__main__":
    scrape_bookmarks_media()
