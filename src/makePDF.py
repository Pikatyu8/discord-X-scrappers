import os
import json
import html
from pathlib import Path
from playwright.sync_api import sync_playwright

def generate_html_content(data):
    """Generates HTML markup based on JSON data."""
    
    html_parts =[
        "<!DOCTYPE html>",
        "<html><head><meta charset='utf-8'>",
        "<style>",
        "body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; background-color: #f5f6f8; }",
        ".card { background: white; border-radius: 12px; padding: 20px; margin-bottom: 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); page-break-inside: avoid; }",
        ".header { color: #65676b; font-size: 14px; margin-bottom: 12px; border-bottom: 1px solid #eee; padding-bottom: 8px; }",
        ".author { font-weight: bold; color: #1da1f2; margin-right: 8px; }",
        ".date { color: #888; }",
        ".text { font-size: 15px; line-height: 1.5; white-space: pre-wrap; margin-bottom: 15px; word-wrap: break-word; color: #0f1419; }",
        ".media-grid { display: flex; flex-wrap: wrap; gap: 10px; margin-top: 10px; }",
        ".media-grid img { max-width: 100%; max-height: 400px; border-radius: 8px; border: 1px solid #eee; object-fit: contain; }",
        ".video-label { display: inline-block; padding: 8px 12px; background: #e1e8ed; border-radius: 6px; font-size: 13px; color: #14171a; margin-top: 5px; }",
        "a { color: #1da1f2; text-decoration: none; } a:hover { text-decoration: underline; }",
        "</style></head><body>"
    ]

    for item in data:
        html_parts.append("<div class='card'>")
        
        # --- POST HEADER ---
        html_parts.append("<div class='header'>")
        # For Discord or items with explicit author
        if "author" in item:
            html_parts.append(f"<span class='author'>{html.escape(item['author'])}</span>")
        # Extract author for Bluesky posts from the URL
        elif "url" in item and "bsky.app/profile/" in item["url"]:
            parts = item["url"].split("/")
            try:
                # Извлекаем никнейм сразу после /profile/
                author = parts[parts.index("profile") + 1]
                html_parts.append(f"<span class='author'>@{html.escape(author)}</span>")
            except (ValueError, IndexError):
                pass
                
        # Date
        if "date" in item and item["date"]:
            html_parts.append(f"<span class='date'>{html.escape(item['date'])}</span>")
        # Link (for Twitter/Bluesky)
        if "url" in item:
            html_parts.append(f" | <a href='{item['url']}'>Original Link</a>")
        html_parts.append("</div>")

        # --- POST TEXT ---
        if "text" in item and item["text"]:
            # html.escape prevents layout breakage if the text contains < or >
            safe_text = html.escape(item["text"])
            html_parts.append(f"<div class='text'>{safe_text}</div>")

        # --- MEDIA FILES ---
        if "local_media" in item and item["local_media"]:
            html_parts.append("<div class='media-grid'>")
            for media_path in item["local_media"]:
                if not os.path.exists(media_path):
                    html_parts.append(f"<div class='video-label'>⚠️ File missing: {os.path.basename(media_path)}</div>")
                    continue
                
                # Convert absolute path to file:/// URI for the browser
                file_uri = Path(media_path).absolute().as_uri()
                ext = media_path.lower().split('.')[-1]

                if ext in['jpg', 'jpeg', 'png', 'gif', 'webp']:
                    html_parts.append(f"<img src='{file_uri}' loading='lazy'>")
                else:
                    html_parts.append(f"<div class='video-label'>🎥 Video/Attachment: {os.path.basename(media_path)}</div>")
            
            html_parts.append("</div>")

        html_parts.append("</div>")

    html_parts.append("</body></html>")
    return "".join(html_parts)


def convert_json_to_pdf(json_filename, pdf_filename):
    print(f"[*] Reading file: {json_filename}...")
    
    try:
        with open(json_filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"[!] Error: File {json_filename} not found.")
        return
    except json.JSONDecodeError:
        print(f"[!] Error: File {json_filename} is corrupted.")
        return

    if not data:
        print("[!] JSON is empty, no data to generate PDF.")
        return

    # 1. Generate HTML code
    print("[*] Generating HTML layout...")
    html_content = generate_html_content(data)
    
    temp_html_path = Path("temp_layout.html").absolute()
    with open(temp_html_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    # 2. Render PDF via Playwright
    print(f"[*] Rendering PDF ({len(data)} entries), this may take a moment...")
    try:
        with sync_playwright() as p:
            # Launch browser in headless mode
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            # Open local HTML file
            page.goto(temp_html_path.as_uri(), wait_until="networkidle")
            
            # Print to PDF
            page.pdf(
                path=pdf_filename,
                format="A4",
                print_background=True,
                margin={"top": "20px", "bottom": "20px", "left": "20px", "right": "20px"}
            )
            browser.close()
            
        print(f"[+] Success! File saved as: {pdf_filename}")
        
    finally:
        # Remove temporary HTML file
        if os.path.exists(temp_html_path):
            os.remove(temp_html_path)


if __name__ == "__main__":
    print("=== Collected Data to PDF Converter ===")
    print("1. Twitter (bookmarks.json -> twitter_bookmarks.pdf)")
    print("2. Discord (disc_msgs.json -> discord_messages.pdf)")
    print("3. Bluesky (bsky_bookmarks.json -> bsky_bookmarks.pdf)")
    
    choice = input("\nChoose a file for conversion (1, 2 or 3): ").strip()
    
    if choice == "1":
        convert_json_to_pdf("bookmarks.json", "twitter_bookmarks.pdf")
    elif choice == "2":
        convert_json_to_pdf("disc_msgs.json", "discord_messages.pdf")
    elif choice == "3":
        convert_json_to_pdf("bsky_bookmarks.json", "bsky_bookmarks.pdf")
    else:
        print("Invalid choice.")
