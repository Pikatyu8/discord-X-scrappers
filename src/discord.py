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

# --- Настройка сессии ---
session = requests.Session()
# Сокращаем количество попыток и делаем агрессивный таймаут
retries = Retry(total=2, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))

def get_chrome_testing_user_data_dir():
    """Определяет путь к профилю Chrome for Testing."""
    if sys.platform == "win32":
        return os.path.join(os.environ["LOCALAPPDATA"], "Google", "Chrome for Testing", "User Data")
    elif sys.platform == "darwin":
        return os.path.expanduser("~/Library/Application Support/Google/Chrome for Testing")
    else:
        return os.path.expanduser("~/.config/google-chrome-for-testing")

def download_media(url, save_path):
    """Скачивает медиафайл по прямой ссылке с коротким таймаутом."""
    try:
        # timeout=(3, 7): 3 секунды на подключение (решает проблему мертвых DNS), 7 секунд на скачивание
        response = session.get(url, stream=True, timeout=(3, 7))
        response.raise_for_status()
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(8192):
                f.write(chunk)
    except Exception:
        # Игнорируем ошибки (битые ссылки, мертвые CDN), чтобы не спамить в консоль
        pass

def save_json_data(scraped_msgs):
    """Динамическое сохранение данных в JSON."""
    sorted_msgs = sorted(scraped_msgs.values(), key=lambda x: int(x["id"]))
    with open("disc_msgs.json", "w", encoding="utf-8") as f:
        json.dump(sorted_msgs, f, ensure_ascii=False, indent=4)

def scrape_discord_messages():
    MEDIA_DIR = "./media"
    os.makedirs(MEDIA_DIR, exist_ok=True)

    TARGET_URL = "https://discord.com/channels/@me" 
    
    # Пул потоков для фоновой загрузки медиа (до 10 файлов одновременно)
    executor = ThreadPoolExecutor(max_workers=10)
    
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
        page.goto(TARGET_URL, wait_until="domcontentloaded")
        
        input("Нажмите Enter, когда откроете нужный чат для начала сбора...")
        
        scraped_msgs = {} 
        no_new_msgs_count = 0
        previous_count = 0
        current_author = "Unknown"

        page.locator('[data-list-id="chat-messages"]').click()
        print("\n=== Начинаю сбор. Для завершения работы нажмите CTRL+C ===")

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

                    # Обнаружение автора
                    author_elem = msg.css('span[class*="username_"] ::text').getall()
                    if author_elem:
                        current_author = "".join(author_elem).strip()

                    text_parts = msg.css('div[class*="messageContent_"] ::text').getall()
                    full_text = "".join(text_parts).strip()
                    date = msg.css('time::attr(datetime)').get()

                    # Обнаружение медиа
                    media_urls =[]
                    media_urls.extend(msg.css('a[class*="originalLink_"]::attr(href)').getall())
                    media_urls.extend(msg.css('video::attr(src)').getall())
                    media_urls = list(set(media_urls))
                    
                    local_media_paths =[]

                    for url in media_urls:
                        parsed_url = urlparse(url)
                        filename = os.path.basename(parsed_url.path)
                        if not filename:
                            filename = f"media_{msg_id}.dat"
                            
                        save_filename = f"{msg_id}_{filename}"
                        filepath = os.path.join(MEDIA_DIR, save_filename)
                        local_media_paths.append(os.path.abspath(filepath))
                        
                        # --- ПРОВЕРКА НАЛИЧИЯ И ФОНОВАЯ ЗАГРУЗКА ---
                        if not os.path.exists(filepath):
                            # Отправляем в фон, скрипт не ждет завершения загрузки!
                            executor.submit(download_media, url, filepath)

                    scraped_msgs[msg_id] = {
                        "id": msg_id,
                        "author": current_author,
                        "date": date,
                        "text": full_text,
                        "local_media": local_media_paths
                    }
                    new_messages_in_batch = True

                # Листаем вверх и проверяем лимит
                if len(scraped_msgs) == previous_count:
                    no_new_msgs_count += 1
                    if no_new_msgs_count >= 100:
                        print("\n[!] 100 прокруток без новых сообщений. Достигнут лимит (начало чата). Авто-остановка.")
                        break
                else:
                    no_new_msgs_count = 0
                    
                previous_count = len(scraped_msgs)
                
                # Динамически сохраняем JSON при наличии новых сообщений
                if new_messages_in_batch:
                    save_json_data(scraped_msgs)
                
                print(f"Собрано {len(scraped_msgs)} сообщений. Прокруток без результата: {no_new_msgs_count}/100. Листаю вверх...", end="\r")
                
                page.keyboard.press("PageUp")
                time.sleep(2)
                
        except KeyboardInterrupt:
            print("\n\n[!] Получен сигнал остановки (CTRL+C). Завершаю сбор...")
        finally:
            print("\nФоновые загрузки завершаются, подождите пару секунд...")
            # Ждем завершения фоновых потоков загрузки (чтобы файлы не побились)
            executor.shutdown(wait=True)
            
            print("Сохраняю финальный JSON...")
            save_json_data(scraped_msgs)
            
            # Гасим ошибку закрытия браузера (полезно при CTRL+C)
            try:
                browser.close()
            except Exception:
                pass
            
            print("Работа успешно завершена!")

if __name__ == "__main__":
    scrape_discord_messages()
