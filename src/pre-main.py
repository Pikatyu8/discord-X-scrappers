import os
import sys
from playwright.sync_api import sync_playwright

def get_chrome_testing_user_data_dir():
    """Определяет путь к профилю Chrome for Testing в зависимости от ОС."""
    if sys.platform == "win32":
        # Именно эта папка используется Chrome for Testing на Windows
        return os.path.join(os.environ["LOCALAPPDATA"], "Google", "Chrome for Testing", "User Data")
    elif sys.platform == "darwin":  # macOS
        return os.path.expanduser("~/Library/Application Support/Google/Chrome for Testing")
    else:  # Linux
        return os.path.expanduser("~/.config/google-chrome-for-testing")

def check_my_profile():
    with sync_playwright() as p:
        # 1. Автоматически находим путь к установленному в системе Chrome for Testing
        executable_path = p.chromium.executable_path
        
        # 2. Получаем путь к папке профиля именно этого браузера
        user_data_dir = get_chrome_testing_user_data_dir()

        print(f"Запуск Chrome for Testing...")
        print(f"Путь к EXE: {executable_path}")
        print(f"Путь к профилю: {user_data_dir}")

        # Проверка на наличие браузера
        if not os.path.exists(executable_path):
            print("\nОШИБКА: Chrome for Testing не найден.")
            print("Выполни в терминале: playwright install chromium")
            return

        browser = p.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            executable_path=executable_path,
            headless=False,
            args=["--start-maximized"],
            no_viewport=True,
        )
        
        page = browser.pages[0] if browser.pages else browser.new_page()
        
        print("Перехожу на X.com...")
        page.goto("https://x.com/home")
        
        # Проверка: залогинены ли мы?
        print("\nПроверь окно браузера. Если сессия подтянулась — ты увидишь свою ленту.")
        input("Нажми ENTER, чтобы закрыть браузер...")
        browser.close()

if __name__ == "__main__":
    check_my_profile()