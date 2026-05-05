import os
import sys
from playwright.sync_api import sync_playwright

def get_chrome_testing_user_data_dir():
    """Determines the Chrome for Testing profile path depending on the OS."""
    if sys.platform == "win32":
        # This specific folder is used by Chrome for Testing on Windows
        return os.path.join(os.environ["LOCALAPPDATA"], "Google", "Chrome for Testing", "User Data")
    elif sys.platform == "darwin":  # macOS
        return os.path.expanduser("~/Library/Application Support/Google/Chrome for Testing")
    else:  # Linux
        return os.path.expanduser("~/.config/google-chrome-for-testing")

def check_my_profile():
    with sync_playwright() as p:
        # 1. Automatically find the path to the installed Chrome for Testing system-wide
        executable_path = p.chromium.executable_path
        
        # 2. Get the profile folder path specifically for this browser
        user_data_dir = get_chrome_testing_user_data_dir()

        print(f"Launching Chrome for Testing...")
        print(f"EXE Path: {executable_path}")
        print(f"Profile Path: {user_data_dir}")

        # Check if the browser exists
        if not os.path.exists(executable_path):
            print("\nERROR: Chrome for Testing not found.")
            print("Run in terminal: playwright install chromium")
            return

        browser = p.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            executable_path=executable_path,
            headless=False,
            args=["--start-maximized"],
            no_viewport=True,
        )
        
        page = browser.pages[0] if browser.pages else browser.new_page()
        
        print("Navigating to X.com...")
        page.goto("https://x.com/home")
        
        # Verification: are we logged in?
        print("\nCheck the browser window.")
        input("Press ENTER to close the browser...")
        browser.close()

if __name__ == "__main__":
    check_my_profile()
