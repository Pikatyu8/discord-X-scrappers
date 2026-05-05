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

# --- Настройка сессии для стабильной загрузки ---
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

def download_media(url, save_path):
    """Скачивает медиафайл по прямой ссылке."""
    try:
        response = session.get(url, stream=True, timeout=30)
        response.raise_for_status()
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(8192):
                f.write(chunk)
        return True
    except Exception as e:
        print(f"Ошибка загрузки {url}: {e}")
        return False

def scrape_discord_messages():
    MEDIA_DIR = "./media"
    os.makedirs(MEDIA_DIR, exist_ok=True)

    # Укажите ссылку на нужный канал или чат
    TARGET_URL = "https://discord.com/channels/@me" 
    
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
        
        print(f"Перехожу в Discord: {TARGET_URL}")
        page.goto(TARGET_URL, wait_until="domcontentloaded")
        
        # Ждем, пока пользователь перейдет в нужный чат
        input("Нажмите Enter, когда откроете нужный чат для начала сбора...")
        
        scraped_msgs = {} 
        no_new_msgs_count = 0
        previous_count = 0
        current_author = "Unknown"

        # Кликаем по области сообщений, чтобы фокус был на чате для прокрутки
        page.locator('[data-list-id="chat-messages"]').click()

        print("\n=== Начинаю сбор. Для завершения работы и сохранения данных нажмите CTRL+C ===")

        try:
            while True:
                html_content = page.content()
                selector = Selector(html_content)
                
                # Ищем все элементы сообщений
                messages = selector.css('li[class*="messageListItem_"]')
                
                for msg in messages:
                    msg_id_attr = msg.css('::attr(id)').get()
                    if not msg_id_attr or "chat-messages-" not in msg_id_attr:
                        continue
                    
                    # Извлекаем уникальный ID сообщения (последний блок цифр)
                    msg_id = msg_id_attr.split('-')[-1]

                    if msg_id in scraped_msgs:
                        continue

                    # --- ОБНАРУЖЕНИЕ АВТОРА И ТЕКСТА ---
                    # Проверяем, есть ли в этом блоке никнейм (начало группы сообщений)
                    author_elem = msg.css('span[class*="username_"] ::text').getall()
                    if author_elem:
                        current_author = "".join(author_elem).strip()

                    text_parts = msg.css('div[class*="messageContent_"] ::text').getall()
                    full_text = "".join(text_parts).strip()
                    date = msg.css('time::attr(datetime)').get()

                    # --- ОБНАРУЖЕНИЕ МЕДИА ---
                    media_urls =[]
                    
                    # 1. Оригинальные вложения (картинки, файлы)
                    media_urls.extend(msg.css('a[class*="originalLink_"]::attr(href)').getall())
                    # 2. Встроенные видео и GIF-ки (включая Tenor)
                    media_urls.extend(msg.css('video::attr(src)').getall())
                    
                    # Убираем дубликаты
                    media_urls = list(set(media_urls))
                    local_media_paths =[]

                    for url in media_urls:
                        # Извлекаем чистое имя файла из URL, отсекая GET-параметры (?ex=...&is=...)
                        parsed_url = urlparse(url)
                        filename = os.path.basename(parsed_url.path)
                        
                        if not filename:
                            filename = f"media_{msg_id}.dat"
                            
                        # Добавляем ID сообщения, чтобы избежать перезаписи файлов с одинаковым именем
                        save_filename = f"{msg_id}_{filename}"
                        filepath = os.path.join(MEDIA_DIR, save_filename)
                        
                        print(f"Скачиваю медиа: {url}")
                        if download_media(url, filepath):
                            local_media_paths.append(os.path.abspath(filepath))

                    scraped_msgs[msg_id] = {
                        "id": msg_id,
                        "author": current_author,
                        "date": date,
                        "text": full_text,
                        "local_media": local_media_paths
                    }
                    print(f"Сохранено сообщение от {current_author}: {full_text[:30]}... (Медиа: {len(local_media_paths)})")

                # Логика прокрутки (листаем ВВЕРХ)
                if len(scraped_msgs) == previous_count:
                    no_new_msgs_count += 1
                    # Если новых сообщений нет 5 раз (или кратно 5), выводим уведомление, но не останавливаемся
                    if no_new_msgs_count > 0 and no_new_msgs_count % 5 == 0:
                        print(">> Возможно больше новых сообщений нет")
                else:
                    no_new_msgs_count = 0
                    
                previous_count = len(scraped_msgs)
                
                print(f"Собрано {len(scraped_msgs)} сообщений. Листаю вверх...")
                
                # Нажимаем PageUp для загрузки истории
                page.keyboard.press("PageUp")
                time.sleep(2)
                
        except KeyboardInterrupt:
            # Срабатывает, когда вы нажимаете CTRL+C
            print("\n[!] Получен сигнал остановки (CTRL+C). Завершаю сбор и сохраняю данные...")

        browser.close()

        # Сортируем сообщения по ID (он хронологический в Discord)
        sorted_msgs = sorted(scraped_msgs.values(), key=lambda x: int(x["id"]))

        with open("disc_msgs.json", "w", encoding="utf-8") as f:
            json.dump(sorted_msgs, f, ensure_ascii=False, indent=4)
        
        print(f"Готово! Всего собрано сообщений: {len(scraped_msgs)}. Данные сохранены в disc_msgs.json")

if __name__ == "__main__":
    scrape_discord_messages()
