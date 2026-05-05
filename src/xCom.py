import os
import sys
import time
import json
import requests
import subprocess
import re
from playwright.sync_api import sync_playwright
from scrapling.parser import Selector
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Настройка сессии для стабильной загрузки картинок ---
session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))

def get_chrome_testing_user_data_dir():
    """Определяет путь к профилю Chrome for Testing."""
    if sys.platform == "win32":
        return os.path.join(os.environ["LOCALAPPDATA"], "Google", "Chrome for Testing", "User Data")
    elif sys.platform == "darwin":
        return os.path.expanduser("~/Library/Application Support/Google/Chrome for Testing")
    else:
        return os.path.expanduser("~/.config/google-chrome-for-testing")

def download_image(url, save_path):
    """Скачивает картинку в максимальном качестве (orig)."""
    # Заменяем любой размер (small, medium, 240x240) на оригинальный
    url = re.sub(r'name=[^&]+', 'name=orig', url)
    try:
        response = session.get(url, stream=True, timeout=30)
        response.raise_for_status()
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(8192):
                f.write(chunk)
        return True
    except Exception as e:
        print(f"Ошибка загрузки картинки {url}: {e}")
        return False

def scrape_bookmarks_media():
    MEDIA_DIR = "./media"
    COOKIE_FILE = "x_cookies.txt"
    os.makedirs(MEDIA_DIR, exist_ok=True)

    with sync_playwright() as p:
        executable_path = p.chromium.executable_path
        user_data_dir = get_chrome_testing_user_data_dir()
        
        print(f"Запускаю Chrome с профилем: {user_data_dir}")
        browser = p.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            executable_path=executable_path,
            headless=False,
            args=["--start-maximized"],
            no_viewport=True,
        )
        
        page = browser.pages[0] if browser.pages else browser.new_page()
        
        # --- ЭКСПОРТ КУКИ ДЛЯ YT-DLP ---
        print("Подготавливаю куки...")
        page.goto("https://x.com", wait_until="domcontentloaded")
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
        
        target_url = "https://x.com/i/bookmarks"
        print(f"Перехожу в закладки: {target_url}")
        page.goto(target_url, wait_until="domcontentloaded")
        time.sleep(5) 
        
        scraped_posts = {} 
        no_new_posts_count = 0
        previous_count = 0

        while True:
            html_content = page.content()
            selector = Selector(html_content)
            tweets = selector.css('[data-testid="tweet"]')
            
            for tweet in tweets:
                links = tweet.css('a::attr(href)').getall()
                post_path = next((link for link in links if '/status/' in link and 'photo' not in link and 'video' not in link), None)
                
                if not post_path:
                    continue
                    
                post_url = f"https://x.com{post_path}"
                tweet_id = post_path.split('/')[-1]

                if post_url in scraped_posts:
                    continue

                # --- ОБНАРУЖЕНИЕ МЕДИА ---
                img_urls = tweet.css('[data-testid="tweetPhoto"] img::attr(src)').getall()
                # Ищем видео по кнопке Play или по тексту в aria-label (как в вашем HTML)
                has_video = bool(tweet.css('[data-testid="playButton"], [data-testid="videoPlayer"], [aria-label*="видео"], [aria-label*="Video"]').get())
                
                if not img_urls and not has_video:
                    continue

                text_parts = tweet.css('[data-testid="tweetText"] ::text').getall()
                full_text = "".join(text_parts).strip()
                date = tweet.css('time::attr(datetime)').get()

                local_media_paths = []

                # --- СКАЧИВАНИЕ ВИДЕО ---
                if has_video:
                    video_filename_template = f"{MEDIA_DIR}/{tweet_id}_video.%(ext)s"
                    print(f"Скачиваю видео через yt-dlp: {post_url}")
                    
                    cmd = [
                        "yt-dlp",
                        "--cookies", COOKIE_FILE,
                        "-o", video_filename_template,
                        post_url
                    ]
                    
                    try:
                        # Запускаем без подавления вывода, чтобы видеть ошибки если они будут
                        subprocess.run(cmd, check=True)
                        
                        # Проверяем, какой файл в итоге создался (mp4/mkv/etc)
                        for file in os.listdir(MEDIA_DIR):
                            if file.startswith(f"{tweet_id}_video"):
                                local_media_paths.append(os.path.abspath(os.path.join(MEDIA_DIR, file)))
                    except Exception as e:
                        print(f"Ошибка yt-dlp для {post_url}: {e}")

                # --- СКАЧИВАНИЕ КАРТИНОК ---
                for idx, img_url in enumerate(img_urls):
                    # Если это обложка видео, yt-dlp скачает само видео, 
                    # но мы также скачаем картинку на случай если это пост с фото.
                    ext = "png" if "format=png" in img_url else "jpg"
                    filename = f"{tweet_id}_img_{idx}.{ext}"
                    filepath = os.path.join(MEDIA_DIR, filename)
                    
                    if download_image(img_url, filepath):
                        local_media_paths.append(os.path.abspath(filepath))

                scraped_posts[post_url] = {
                    "url": post_url,
                    "date": date,
                    "text": full_text,
                    "local_media": local_media_paths
                }
                print(f"Сохранен пост {post_url} (Медиа файлов: {len(local_media_paths)})")

            # Логика прокрутки
            if len(scraped_posts) == previous_count:
                no_new_posts_count += 1
            else:
                no_new_posts_count = 0
                
            previous_count = len(scraped_posts)
            if no_new_posts_count >= 3:
                print("Новых закладок больше нет.")
                break
            
            print(f"Собрано {len(scraped_posts)}. Листаю вниз...")
            page.keyboard.press("PageDown")
            time.sleep(3)

        browser.close()
        if os.path.exists(COOKIE_FILE):
            os.remove(COOKIE_FILE)

        with open("bookmarks.json", "w", encoding="utf-8") as f:
            json.dump(list(scraped_posts.values()), f, ensure_ascii=False, indent=4)
        
        print(f"\nГотово! Постов: {len(scraped_posts)}. Данные в bookmarks.json")

if __name__ == "__main__":
    scrape_bookmarks_media()
