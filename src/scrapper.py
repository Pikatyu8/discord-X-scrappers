# src/scrapper.py
import os
import sys
import time
import json
import requests
import re
import html
import subprocess
import threading
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
from playwright.sync_api import sync_playwright
from scrapling.parser import Selector
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Настройка сессии запросов с заголовками во избежание 403 Forbidden со стороны серверов ВК
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://vk.com/"
})
retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))

json_lock = threading.Lock()
EXTENSION_PATH = os.path.abspath("./ext/InsensitiveX")
MEDIA_DIR = "./media"

def get_chrome_testing_user_data_dir():
    """Определяет путь к профилю Chrome for Testing."""
    if sys.platform == "win32":
        return os.path.join(os.environ["LOCALAPPDATA"], "Google", "Chrome for Testing", "User Data")
    elif sys.platform == "darwin":
        return os.path.expanduser("~/Library/Application Support/Google/Chrome for Testing")
    else:
        return os.path.expanduser("~/.config/google-chrome-for-testing")

def download_media_direct(url, save_path):
    """Скачивает медиафайл по прямой ссылке."""
    try:
        response = session.get(url, stream=True, timeout=(5, 15))
        response.raise_for_status()
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(8192):
                f.write(chunk)
    except Exception as e:
        print(f"\n[!] Error downloading {url}: {e}")

def download_image_twitter(url, save_path):
    """Скачивает изображение Twitter в максимальном качестве."""
    url = re.sub(r'name=[^&]+', 'name=orig', url)
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

def download_image_bluesky(url, save_path):
    """Скачивает изображение Bluesky в максимальном качестве."""
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

def download_video_async(post_url, post_id, cookie_file, scraped_posts, json_filename):
    """Фоновый запуск yt-dlp для загрузки видео."""
    video_filename_template = f"{MEDIA_DIR}/{post_id}_video.%(ext)s"
    cmd = ["yt-dlp", "--cookies", cookie_file, "-o", video_filename_template, post_url]
    try:
        subprocess.run(cmd, check=True, capture_output=True)
        for file in os.listdir(MEDIA_DIR):
            if file.startswith(f"{post_id}_video") and not file.endswith('.part') and not file.endswith('.ytdl'):
                file_path = os.path.abspath(os.path.join(MEDIA_DIR, file))
                if file_path not in scraped_posts[post_url]["local_media"]:
                    scraped_posts[post_url]["local_media"].append(file_path)
        
        save_json_data(scraped_posts, json_filename)
        print(f"\n[+] Background video download finished: {post_url}")
    except Exception as e:
        print(f"\n[!] Background yt-dlp error for {post_url}: {e}")

def save_json_data(scraped_data, filename):
    """Безопасное сохранение данных в JSON."""
    with json_lock:
        if "disc_msgs" in filename:
            sorted_msgs = sorted(scraped_data.values(), key=lambda x: int(x["id"]))
            data_to_save = sorted_msgs
        else:
            data_to_save = list(scraped_data.values())
            
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data_to_save, f, ensure_ascii=False, indent=4)

def load_json_data(filename):
    """Загрузка существующих записей для предотвращения дублирования."""
    if os.path.exists(filename):
        try:
            with open(filename, "r", encoding="utf-8") as f:
                data = json.load(f)
                if "disc_msgs" in filename:
                    return {item["id"]: item for item in data}
                return {item["url"]: item for item in data}
        except Exception as e:
            print(f"Error loading existing JSON: {e}")
    return {}

def export_cookies(browser, cookie_file):
    """Экспорт куки браузера в формате Netscape для yt-dlp."""
    cookies = browser.cookies()
    with open(cookie_file, "w", encoding="utf-8") as f:
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

# --- Скрейперы ---

def scrape_vk_profile_page(page, url, scraped_posts, executor, json_file):
    """Сбор публикаций со стены профиля или сообщества ВК."""
    profile_name = "VK Page"
    name_elem = page.locator("#owner_page_name, .OwnerPageName, title").first
    if name_elem.count() > 0:
        profile_name = name_elem.text_content().strip()
        profile_name = profile_name.split('\xa0')[0].split('\n')[0].strip()
        
    print(f"[*] Page Name: {profile_name}")
    print("\n=== Collection started. Press CTRL+C to stop ===")
    
    no_new_posts_count = 0
    previous_count = len(scraped_posts)
    
    try:
        while True:
            html_content = page.content()
            selector = Selector(html_content)
            posts = selector.css("[data-testid='post'], article[data-post-id]")
            
            new_posts_in_batch = False
            
            for post_locator in posts:
                post_id = post_locator.css('::attr(data-post-id)').get()
                if not post_id:
                    post_id_attr = post_locator.css('::attr(id)').get()
                    if post_id_attr and "_" in post_id_attr:
                        post_id = post_id_attr
                    else:
                        continue
                        
                post_url = f"https://vk.com/wall{post_id}"
                
                if post_url in scraped_posts:
                    continue
                    
                # Дата публикации
                date_text = post_locator.css("[data-testid='post_date_block_preview'] ::text, a[href*='/wall'] ::text").get() or "Unknown Date"
                date_text = date_text.strip()
                
                # Текст публикации
                text_content = post_locator.css("[data-testid='showmoretext'] ::text, .vkitFeedShowMoreText__text--0wZYb ::text, [id^='text-'] ::text").getall()
                text_content = "".join(text_content).strip()
                
                # Фотографии публикации
                img_urls = post_locator.css("img[data-testid='media-grid-image']::attr(src), .vkitMediaGridImage__image--60h5h::attr(src), a[href*='/photo'] img::attr(src)").getall()
                img_urls = list(set(img_urls))
                
                local_media_paths = []
                for idx, img_url in enumerate(img_urls):
                    filename = f"vk_{post_id}_img_{idx}.jpg"
                    filepath = os.path.join(MEDIA_DIR, filename)
                    local_path = os.path.abspath(filepath)
                    
                    decoded_img_url = html.unescape(img_url)
                    executor.submit(download_media_direct, decoded_img_url, filepath)
                    local_media_paths.append(local_path)
                    
                scraped_posts[post_url] = {
                    "url": post_url,
                    "date": date_text,
                    "text": text_content,
                    "author": profile_name,
                    "local_media": local_media_paths
                }
                new_posts_in_batch = True
                
            if len(scraped_posts) == previous_count:
                no_new_posts_count += 1
                if no_new_posts_count >= 500:
                    print("\n[!] Limit reached (500 idle scrolls). Auto-stopping.")
                    break
            else:
                no_new_posts_count = 0
                
            previous_count = len(scraped_posts)
            if new_posts_in_batch:
                save_json_data(scraped_posts, json_file)
                
            print(f"Collected {len(scraped_posts)} posts. Idle: {no_new_posts_count}/500. Scrolling down...", end="\r")
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(1.0)
            
    except KeyboardInterrupt:
        print("\n\n[!] Stop signal received. Terminating...")
    finally:
        save_json_data(scraped_posts, json_file)

def scrape_vk_album_logic(page, url, scraped_posts, executor, json_file):
    """Сбор изображений альбома ВК методом прокрутки страницы."""
    no_new_posts_count = 0
    previous_count = len(scraped_posts)
    
    album_title = "VK Album"
    title_elem = page.locator("h1").first
    if title_elem.count() > 0:
        album_title = title_elem.text_content().strip()

    print(f"[*] Album Title: {album_title}")
    print("\n=== Collection started. Press CTRL+C to stop ===")

    try:
        while True:
            # Парсинг страницы через Scrapling Selector
            html_content = page.content()
            selector = Selector(html_content)
            
            # Поиск блоков с фото в сетке альбома
            photo_elements = selector.css('div.photos_row, div[class*="photos_row"]')
            new_posts_in_batch = False

            for el in photo_elements:
                href = el.css('a::attr(href)').get()
                if not href:
                    continue
                
                # Создаем стандартизированный URL фотографии
                match = re.search(r'photo-?\d+_\d+', href)
                photo_id = match.group(0) if match else os.path.basename(href).split('?')[0]
                post_url = f"https://vk.com/{photo_id}"

                if post_url in scraped_posts:
                    continue

                # Извлечение URL изображения из инлайнового background-image
                style_attr = el.css('::attr(style)').get() or ""
                img_src = None
                if 'url(' in style_attr:
                    start = style_attr.find('url(') + 4
                    end = style_attr.find(')', start)
                    if end != -1:
                        img_src = style_attr[start:end].strip('\'"')

                if not img_src:
                    continue

                # Декодируем HTML-сущности (например, &amp; -> &) для корректного запроса к CDN
                img_src = html.unescape(img_src)

                # Подменяем размер превью (например, cs=240x0) на максимально качественный (cs=1280x0)
                img_src = re.sub(r'cs=\d+x\d+', 'cs=1280x0', img_src)

                filename = f"{photo_id}.jpg"
                filepath = os.path.join(MEDIA_DIR, filename)
                local_path = os.path.abspath(filepath)

                if not os.path.exists(filepath):
                    executor.submit(download_media_direct, img_src, filepath)

                scraped_posts[post_url] = {
                    "url": post_url,
                    "date": "Unknown Date",
                    "text": "",
                    "author": album_title,
                    "local_media": [local_path]
                }
                new_posts_in_batch = True

            # Проверка прогресса для остановки при отсутствии нового контента
            if len(scraped_posts) == previous_count:
                # Если кнопка «Показать больше» видна, кликаем по ней
                load_more = page.locator("#ui_photos_load_more, ._ui_photos_load_more").first
                if load_more.is_visible():
                    try:
                        load_more.click(timeout=2000)
                        time.sleep(1.5)
                    except Exception:
                        pass
                
                no_new_posts_count += 1
                if no_new_posts_count >= 500:
                    print("\n[!] Limit reached (500 idle scrolls). Auto-stopping.")
                    break
            else:
                no_new_posts_count = 0
                
            previous_count = len(scraped_posts)
            if new_posts_in_batch:
                save_json_data(scraped_posts, json_file)

            print(f"Collected {len(scraped_posts)} photos. Idle: {no_new_posts_count}/500. Scrolling down...", end="\r")
            
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\n\n[!] Stop signal received. Terminating...")
    finally:
        save_json_data(scraped_posts, json_file)

def scrape_vk():
    """Интерактивная точка входа с поддержкой автоматического определения типа открытой страницы."""
    os.makedirs(MEDIA_DIR, exist_ok=True)
    json_file = "vk_data.json"
    scraped_posts = load_json_data(json_file)
    executor = ThreadPoolExecutor(max_workers=5)

    with sync_playwright() as p:
        executable_path = p.chromium.executable_path
        user_data_dir = get_chrome_testing_user_data_dir()
        
        browser = p.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            executable_path=executable_path,
            headless=False,
            no_viewport=True,
            args=["--start-maximized"],
        )
        
        try:
            page = browser.pages[0] if browser.pages else browser.new_page()
            page.goto("https://vk.com")
            
            input("\n[!] Open the target VK Album or Profile/Group wall in the browser, then press Enter to start...")
            
            # Умный поиск активной вкладки, на которой пользователь открыл контент
            target_page = page
            for p_obj in browser.pages:
                if "vk.com" in p_obj.url:
                    # Если нашли вкладку, которая не является фидом новостей, переключаемся на нее
                    if "album" in p_obj.url or "photo" in p_obj.url or "wall" in p_obj.url:
                        target_page = p_obj
                        break
                    elif p_obj.locator(".photos_album_page, .photos_row, [data-testid='post']").count() > 0:
                        target_page = p_obj
                        break
            
            page = target_page
            target_url = page.url
            print(f"[*] Starting scraper on: {target_url}")
            
            # Автоматическое определение типа контента на базе URL и структуры DOM
            is_album = "/album" in target_url or "/photos" in target_url or "/photo" in target_url
            if not is_album:
                if page.locator(".photos_album_page, .photos_row").count() > 0:
                    is_album = True
                
            if not is_album:
                print("[+] Auto-detected content type: VK Profile or Page")
                scrape_vk_profile_page(page, target_url, scraped_posts, executor, json_file)
            else:
                print("[+] Auto-detected content type: VK Photo Album")
                scrape_vk_album_logic(page, target_url, scraped_posts, executor, json_file)
                
        except KeyboardInterrupt:
            print("\n[!] Stop signal received. Terminating...")
        finally:
            try:
                browser.close()
            except Exception:
                pass
            executor.shutdown(wait=True)

def scrape_discord_messages():
    os.makedirs(MEDIA_DIR, exist_ok=True)
    json_file = "disc_msgs.json"
    scraped_msgs = load_json_data(json_file)
    executor = ThreadPoolExecutor(max_workers=10)
    
    with sync_playwright() as p:
        executable_path = p.chromium.executable_path
        user_data_dir = get_chrome_testing_user_data_dir()
        
        browser = p.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            executable_path=executable_path,
            headless=False,
            args=["--start-maximized"],
            no_viewport=True,
        )
        
        page = browser.pages[0] if browser.pages else browser.new_page()
        page.goto("https://discord.com/channels/@me", wait_until="domcontentloaded")
        
        input("\n[!] Open the target chat in Discord, then press Enter to start collecting...")
        
        no_new_msgs_count = 0
        previous_count = len(scraped_msgs)
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

                    author_elem = msg.css('span[class*="username_"] ::text').getall()
                    if author_elem:
                        current_author = "".join(author_elem).strip()

                    text_parts = msg.css('div[class*="messageContent_"] ::text').getall()
                    full_text = "".join(text_parts).strip()
                    date = msg.css('time::attr(datetime)').get()

                    media_urls = []
                    media_urls.extend(msg.css('a[class*="originalLink_"]::attr(href)').getall())
                    media_urls.extend(msg.css('video::attr(src)').getall())
                    media_urls = list(set(media_urls))
                    
                    local_media_paths = []
                    for url in media_urls:
                        parsed_url = urlparse(url)
                        filename = os.path.basename(parsed_url.path) or f"media_{msg_id}.dat"
                        save_filename = f"{msg_id}_{filename}"
                        filepath = os.path.join(MEDIA_DIR, save_filename)
                        local_media_paths.append(os.path.abspath(filepath))
                        
                        if not os.path.exists(filepath):
                            executor.submit(download_media_direct, url, filepath)

                    scraped_msgs[msg_id] = {
                        "id": msg_id,
                        "author": current_author,
                        "date": date,
                        "text": full_text,
                        "local_media": local_media_paths
                    }
                    new_messages_in_batch = True

                if len(scraped_msgs) == previous_count:
                    no_new_msgs_count += 1
                    if no_new_msgs_count >= 500:
                        print("\n[!] 500 scrolls with no new messages. Limit reached. Auto-stopping.")
                        break
                else:
                    no_new_msgs_count = 0
                    
                previous_count = len(scraped_msgs)
                if new_messages_in_batch:
                    save_json_data(scraped_msgs, json_file)
                
                print(f"Collected {len(scraped_msgs)} messages. Idle: {no_new_msgs_count}/500. Scrolling up...", end="\r")
                page.keyboard.press("PageUp")
                time.sleep(2)
                
        except KeyboardInterrupt:
            print("\n\n[!] Stop signal received. Terminating...")
        finally:
            print("\nWaiting for background downloads to finish...")
            executor.shutdown(wait=True)
            save_json_data(scraped_msgs, json_file)
            try:
                browser.close()
            except Exception:
                pass

def scrape_twitter_bookmarks():
    os.makedirs(MEDIA_DIR, exist_ok=True)
    json_file = "bookmarks.json"
    cookie_file = "x_cookies.txt"
    scraped_posts = load_json_data(json_file)
    executor = ThreadPoolExecutor(max_workers=3)

    with sync_playwright() as p:
        executable_path = p.chromium.executable_path
        user_data_dir = get_chrome_testing_user_data_dir()
        
        browser = p.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            executable_path=executable_path,
            headless=False,
            no_viewport=True,
            args=[
                "--start-maximized",
                f"--disable-extensions-except={EXTENSION_PATH}",
                f"--load-extension={EXTENSION_PATH}",
            ],
            ignore_default_args=["--disable-extensions"]
        )        
        page = browser.pages[0] if browser.pages else browser.new_page()
        page.goto("https://x.com/i/bookmarks")
        
        input("\n[!] Open Twitter Bookmarks, then press Enter to start...")
        
        export_cookies(browser, cookie_file)
        no_new_posts_count = 0
        previous_count = len(scraped_posts)

        print("\n=== Collection started. Press CTRL+C to stop ===")

        try:
            while True:
                new_posts_html = page.evaluate('''() => {
                    const posts = document.querySelectorAll('[data-testid="tweet"]:not([data-scraped="true"])');
                    const results = [];
                    for (const p of posts) {
                        p.setAttribute('data-scraped', 'true');
                        results.push(p.outerHTML);
                    }
                    return results;
                }''')
                
                new_posts_in_batch = False

                for post_html in new_posts_html:
                    tweet = Selector(post_html)
                    links = tweet.css('a::attr(href)').getall()
                    post_path = next((link for link in links if '/status/' in link and 'photo' not in link and 'video' not in link), None)
                    
                    if not post_path:
                        continue
                        
                    post_url = f"https://x.com{post_path}"
                    tweet_id = post_path.split('/')[-1]

                    if post_url in scraped_posts:
                        continue

                    img_urls = tweet.css('[data-testid="tweetPhoto"] img::attr(src)').getall()
                    has_video = bool(tweet.css('[data-testid="playButton"],[data-testid="videoPlayer"], [aria-label*="video"], [aria-label*="видео"]').get())
                    text_parts = tweet.css('[data-testid="tweetText"] ::text').getall()
                    full_text = "".join(text_parts).strip()
                    date = tweet.css('time::attr(datetime)').get()

                    local_media_paths = []
                    video_exists = False
                    if has_video:
                        for file in os.listdir(MEDIA_DIR):
                            if file.startswith(f"{tweet_id}_video") and not file.endswith('.part') and not file.endswith('.ytdl'):
                                video_exists = True
                                local_media_paths.append(os.path.abspath(os.path.join(MEDIA_DIR, file)))
                                break

                    for idx, img_url in enumerate(img_urls):
                        ext = "png" if "format=png" in img_url else "jpg"
                        filename = f"{tweet_id}_img_{idx}.{ext}"
                        filepath = os.path.join(MEDIA_DIR, filename)
                        
                        if os.path.exists(filepath):
                            local_media_paths.append(os.path.abspath(filepath))
                        else:
                            if download_image_twitter(img_url, filepath):
                                local_media_paths.append(os.path.abspath(filepath))

                    scraped_posts[post_url] = {
                        "url": post_url,
                        "date": date,
                        "text": full_text,
                        "local_media": local_media_paths
                    }
                    new_posts_in_batch = True

                    if has_video and not video_exists:
                        executor.submit(download_video_async, post_url, tweet_id, cookie_file, scraped_posts, json_file)

                if len(scraped_posts) == previous_count:
                    no_new_posts_count += 1
                    if no_new_posts_count >= 500:
                        print("\n[!] Limit reached (500 idle scrolls). Auto-stopping.")
                        break
                else:
                    no_new_posts_count = 0
                    
                previous_count = len(scraped_posts)
                if new_posts_in_batch:
                    save_json_data(scraped_posts, json_file)

                print(f"Collected {len(scraped_posts)} posts. Idle: {no_new_posts_count}/500. Scrolling down...", end="\r")
                page.keyboard.press("PageDown")
                time.sleep(0.2)

        except KeyboardInterrupt:
            print("\n\n[!] Stop signal received. Terminating...")
        finally:
            save_json_data(scraped_posts, json_file)
            if os.path.exists(cookie_file):
                try:
                    os.remove(cookie_file)
                except Exception:
                    pass
            try:
                browser.close()
            except Exception:
                pass
            executor.shutdown(wait=True)

def scrape_bluesky_bookmarks():
    os.makedirs(MEDIA_DIR, exist_ok=True)
    json_file = "bsky_bookmarks.json"
    cookie_file = "bsky_cookies.txt"
    scraped_posts = load_json_data(json_file)
    executor = ThreadPoolExecutor(max_workers=3)

    with sync_playwright() as p:
        executable_path = p.chromium.executable_path
        user_data_dir = get_chrome_testing_user_data_dir()
        
        browser = p.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            executable_path=executable_path,
            headless=False,
            args=["--start-maximized"],
            no_viewport=True,
        )
        
        page = browser.pages[0] if browser.pages else browser.new_page()
        page.goto("https://bsky.app/saved")
        
        input("\n[!] Open Bluesky Bookmarks, then press Enter to start...")
        
        export_cookies(browser, cookie_file)
        no_new_posts_count = 0
        previous_count = len(scraped_posts)

        print("\n=== Collection started. Press CTRL+C to stop ===")

        try:
            while True:
                new_posts_html = page.evaluate('''() => {
                    const posts = document.querySelectorAll('[data-testid^="feedItem-by-"]:not([data-scraped="true"])');
                    const results = [];
                    for (const p of posts) {
                        p.setAttribute('data-scraped', 'true');
                        results.push(p.outerHTML);
                    }
                    return results;
                }''')
                
                new_posts_in_batch = False

                for post_html in new_posts_html:
                    post = Selector(post_html)
                    links = post.css('a::attr(href)').getall()
                    post_path = next((link for link in links if '/post/' in link), None)
                    
                    if not post_path:
                        continue
                        
                    post_url = f"https://bsky.app{post_path}"
                    post_id = post_path.split('/')[-1]

                    if post_url in scraped_posts:
                        continue

                    img_urls = post.css('img[src*="/feed_thumbnail/"]::attr(src), img[src*="/feed_fullsize/"]::attr(src)').getall()
                    has_video = bool(post.css('video,[aria-label*="video"], [aria-label*="видео"],[aria-label*="Видео"], [data-testid="playButton"]').get())
                    text_parts = post.css('[data-testid="postText"] ::text').getall()
                    full_text = "".join(text_parts).strip()
                    
                    date_elem = post.css('a[href*="/post/"][data-tooltip], a[href*="/post/"][aria-label]')
                    date = date_elem.css('::attr(data-tooltip)').get() or date_elem.css('::attr(aria-label)').get()

                    local_media_paths = []
                    video_exists = False
                    if has_video:
                        for file in os.listdir(MEDIA_DIR):
                            if file.startswith(f"{post_id}_video") and not file.endswith('.part') and not file.endswith('.ytdl'):
                                video_exists = True
                                local_media_paths.append(os.path.abspath(os.path.join(MEDIA_DIR, file)))
                                break

                    for idx, img_url in enumerate(img_urls):
                        filepath = os.path.join(MEDIA_DIR, f"{post_id}_img_{idx}.jpg")
                        if os.path.exists(filepath):
                            local_media_paths.append(os.path.abspath(filepath))
                        else:
                            if download_image_bluesky(img_url, filepath):
                                local_media_paths.append(os.path.abspath(filepath))

                    scraped_posts[post_url] = {
                        "url": post_url,
                        "date": date or "Unknown",
                        "text": full_text,
                        "local_media": local_media_paths
                    }
                    new_posts_in_batch = True

                    if has_video and not video_exists:
                        executor.submit(download_video_async, post_url, post_id, cookie_file, scraped_posts, json_file)

                if len(scraped_posts) == previous_count:
                    no_new_posts_count += 1
                    if no_new_posts_count >= 100:
                        print("\n[!] Limit reached (100 idle scrolls). Auto-stopping.")
                        break
                else:
                    no_new_posts_count = 0
                    
                previous_count = len(scraped_posts)
                if new_posts_in_batch:
                    save_json_data(scraped_posts, json_file)

                print(f"Collected {len(scraped_posts)} posts. Idle: {no_new_posts_count}/100. Scrolling down...", end="\r")
                page.keyboard.press("PageDown")
                time.sleep(0.2)

        except KeyboardInterrupt:
            print("\n\n[!] Stop signal received. Terminating...")
        finally:
            save_json_data(scraped_posts, json_file)
            if os.path.exists(cookie_file):
                try:
                    os.remove(cookie_file)
                except Exception:
                    pass
            try:
                browser.close()
            except Exception:
                pass
            executor.shutdown(wait=True)
