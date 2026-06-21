# src/scrapper.py
import os
import sys
import time
import json
import requests
import re
import subprocess
import threading
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
from playwright.sync_api import sync_playwright
from scrapling.parser import Selector
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Настройка сессии запросов
session = requests.Session()
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
        response = session.get(url, stream=True, timeout=(3, 7))
        response.raise_for_status()
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(8192):
                f.write(chunk)
    except Exception:
        pass

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
    
    print("[*] Scrolling wall to trigger lazy loading of posts...")
    for i in range(5):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(1.5)
        print(f"  Scroll {i+1}/5 completed...", end="\r")
    print()

    posts = page.locator("[data-testid='post'], article[data-post-id]")
    total_posts = posts.count()
    print(f"[+] Found {total_posts} wall posts on the page.")
    
    new_items_count = 0
    for i in range(total_posts):
        post_locator = posts.nth(i)
        post_id = post_locator.get_attribute("data-post-id")
        
        if not post_id:
            post_id_attr = post_locator.get_attribute("id")
            if post_id_attr and "_" in post_id_attr:
                post_id = post_id_attr
            else:
                post_id = f"post_{i}"
                
        post_url = f"https://vk.com/wall{post_id}"
        
        if post_url in scraped_posts:
            continue
            
        # Дата публикации
        date_elem = post_locator.locator("[data-testid='post_date_block_preview'], a[href*='/wall']").first
        date_text = date_elem.text_content().strip() if date_elem.count() > 0 else "Unknown Date"
        
        # Текст публикации
        text_elem = post_locator.locator("[data-testid='showmoretext'], .vkitFeedShowMoreText__text--0wZYb, [id^='text-']").first
        text_content = text_elem.text_content().strip() if text_elem.count() > 0 else ""
        
        # Фотографии публикации
        img_locators = post_locator.locator("img[data-testid='media-grid-image'], .vkitMediaGridImage__image--60h5h, a[href*='/photo'] img")
        img_count = img_locators.count()
        img_urls = []
        for j in range(img_count):
            src = img_locators.nth(j).get_attribute("src")
            if src and src not in img_urls:
                img_urls.append(src)
                
        local_media_paths = []
        for idx, img_url in enumerate(img_urls):
            filename = f"vk_{post_id}_img_{idx}.jpg"
            filepath = os.path.join(MEDIA_DIR, filename)
            local_path = os.path.abspath(filepath)
            
            executor.submit(download_media_direct, img_url, filepath)
            local_media_paths.append(local_path)
            
        scraped_posts[post_url] = {
            "url": post_url,
            "date": date_text,
            "text": text_content,
            "author": profile_name,
            "local_media": local_media_paths
        }
        new_items_count += 1
        
    if new_items_count > 0:
        save_json_data(scraped_posts, json_file)
        print(f"[+] Saved {new_items_count} new wall posts to {json_file}")
    else:
        print("[*] No new posts detected to save.")

def scrape_vk_album_logic(page, url, scraped_posts, executor, json_file):
    """Внутренняя логика парсинга фотографий альбома ВК."""
    print("[*] Scrolling and loading all photos in the album...")
    last_photo_count = 0
    no_change_count = 0
    while True:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(1)
        
        load_more = page.locator("#ui_photos_load_more, ._ui_photos_load_more")
        if load_more.is_visible():
            try:
                load_more.click(timeout=3000)
                time.sleep(1.5)
            except Exception:
                pass
        
        current_photos = page.locator(".photos_row")
        current_count = current_photos.count()
        print(f"Loaded thumbnails: {current_count}...", end="\r")
        
        if current_count == last_photo_count:
            no_change_count += 1
            if no_change_count >= 5:
                break
        else:
            no_change_count = 0
            last_photo_count = current_count
            
    total_photos = last_photo_count
    print(f"\n[+] Total photos in album found on page: {total_photos}")
    
    if total_photos == 0:
        print("[!] No photos found on page. Exiting.")
        return
        
    album_title = "VK Album"
    title_elem = page.locator("h1")
    if title_elem.count() > 0:
        album_title = title_elem.first.text_content().strip()
        
    print(f"[*] Album Title: {album_title}")
    
    # Открываем первое фото
    first_photo = page.locator(".photos_row a").first
    first_photo.click()
    page.wait_for_selector(".pv_photo_wrap, #pv_photo_wrap, .pv_img_area_wrap img", timeout=15000)
    
    print("\n=== Photoview overlay open. Collecting files and metadata... ===")
    
    last_img_src = None
    for idx in range(total_photos):
        time.sleep(0.5)
        
        img_selector = ".pv_img_area_wrap img, #pv_photo img"
        img_src = None
        for _ in range(15):
            img_elem = page.locator(img_selector).first
            if img_elem.count() > 0:
                temp_src = img_elem.get_attribute("src")
                if temp_src and temp_src != last_img_src:
                    img_src = temp_src
                    break
            time.sleep(0.2)
        
        if not img_src and img_elem.count() > 0:
            img_src = img_elem.get_attribute("src")
            
        last_img_src = img_src
        
        desc_text = ""
        desc_elem = page.locator("#pv_desc, .pv_desc, #pv_description").first
        if desc_elem.count() > 0:
            desc_text = desc_elem.text_content().strip()
            
        date_text = "Unknown Date"
        date_elem = page.locator("#pv_date, .pv_date, .pv_time").first
        if date_elem.count() > 0:
            date_text = date_elem.text_content().strip()
            
        current_url = page.url()
        photo_id = current_url.split('/')[-1].split('?')[0] if '/photo' in current_url else f"photo_{idx}"
        
        print(f"[{idx+1}/{total_photos}] URL: {current_url} | Img: {img_src[:50] if img_src else 'None'}...")
        
        if img_src and current_url not in scraped_posts:
            ext = "jpg"
            if ".png" in img_src.lower():
                ext = "png"
            elif ".gif" in img_src.lower():
                ext = "gif"
            elif ".webp" in img_src.lower():
                ext = "webp"
                
            filename = f"{photo_id}.{ext}"
            filepath = os.path.join(MEDIA_DIR, filename)
            local_path = os.path.abspath(filepath)
            
            executor.submit(download_media_direct, img_src, filepath)
            
            scraped_posts[current_url] = {
                "url": current_url,
                "date": date_text,
                "text": desc_text,
                "author": album_title,
                "local_media": [local_path]
            }
            
            save_json_data(scraped_posts, json_file)
        
        page.keyboard.press("ArrowRight")

def scrape_vk():
    """Единая точка входа для сбора данных ВК с автоматическим определением типа."""
    os.makedirs(MEDIA_DIR, exist_ok=True)
    json_file = "vk_data.json"
    scraped_posts = load_json_data(json_file)
    executor = ThreadPoolExecutor(max_workers=5)

    target_url = input("\n[!] Enter VK URL (Album or Profile/Group wall): ").strip()
    if not target_url:
        print("[!] URL cannot be empty.")
        return

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
        
        page = browser.pages[0] if browser.pages else browser.new_page()
        page.goto(target_url)
        
        print("\n[!] Loading VK... Waiting for page stability...")
        time.sleep(4)
        
        # Автоматическое определение типа контента
        is_profile = False
        if page.locator(".ProfileHeader, [data-testid='profile-header'], #profile_redesigned, #owner_page_name, #page-wall").count() > 0:
            is_profile = True
        elif "/album" not in target_url and "/photos" not in target_url:
            is_profile = True
            
        if is_profile:
            print("[+] Auto-detected content type: VK Profile or Page")
            scrape_vk_profile_page(page, target_url, scraped_posts, executor, json_file)
        else:
            print("[+] Auto-detected content type: VK Photo Album")
            scrape_vk_album_logic(page, target_url, scraped_posts, executor, json_file)
            
        browser.close()
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
            print("\n\n[!] Stop signal received. Terminating collection...")
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
                        
                    post_url = f"https://vk.com{post_path}"
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
