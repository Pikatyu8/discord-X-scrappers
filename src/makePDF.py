import os
import json
import html
from pathlib import Path
from playwright.sync_api import sync_playwright
from pypdf import PdfWriter  # Добавьте этот импорт (нужно установить: pip install pypdf)

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
        ".media-grid { display: flex; flex-direction: column; gap: 10px; margin-top: 10px; }",
        ".media-grid img { max-width: 100%; max-height: 500px; border-radius: 8px; border: 1px solid #eee; object-fit: contain; }",
        ".video-label { display: inline-block; padding: 8px 12px; background: #e1e8ed; border-radius: 6px; font-size: 13px; color: #14171a; margin-top: 5px; }",
        "a { color: #1da1f2; text-decoration: none; } a:hover { text-decoration: underline; }",
        "</style></head><body>"
    ]

    for item in data:
        html_parts.append("<div class='card'>")
        html_parts.append("<div class='header'>")
        if "author" in item:
            html_parts.append(f"<span class='author'>{html.escape(item['author'])}</span>")
        elif "url" in item and "bsky.app/profile/" in item["url"]:
            parts = item["url"].split("/")
            try:
                author = parts[parts.index("profile") + 1]
                html_parts.append(f"<span class='author'>@{html.escape(author)}</span>")
            except (ValueError, IndexError):
                pass
        if "date" in item and item["date"]:
            html_parts.append(f"<span class='date'>{html.escape(item['date'])}</span>")
        if "url" in item:
            html_parts.append(f" | <a href='{item['url']}'>Original Link</a>")
        html_parts.append("</div>")

        if "text" in item and item["text"]:
            safe_text = html.escape(item["text"])
            html_parts.append(f"<div class='text'>{safe_text}</div>")

        if "local_media" in item and item["local_media"]:
            html_parts.append("<div class='media-grid'>")
            for media_path in item["local_media"]:
                if not os.path.exists(media_path):
                    html_parts.append(f"<div class='video-label'>⚠️ File missing: {os.path.basename(media_path)}</div>")
                    continue
                file_uri = Path(media_path).absolute().as_uri()
                ext = media_path.lower().split('.')[-1]
                if ext in ['jpg', 'jpeg', 'png', 'gif', 'webp']:
                    html_parts.append(f"<img src='{file_uri}'>")
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
    except Exception as e:
        print(f"[!] Error: {e}")
        return

    if not data:
        print("[!] JSON is empty.")
        return

    # Настройки батчинга (по 400 записей за раз, чтобы не превысить лимит 512МБ)
    BATCH_SIZE = 400
    temp_pdfs = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        
        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i : i + BATCH_SIZE]
            batch_idx = (i // BATCH_SIZE) + 1
            print(f"[*] Processing batch {batch_idx} ({len(batch)} entries)...")
            
            html_content = generate_html_content(batch)
            temp_html = Path(f"temp_{batch_idx}.html").absolute()
            temp_pdf = f"temp_{batch_idx}.pdf"
            
            with open(temp_html, "w", encoding="utf-8") as f:
                f.write(html_content)
            
            page = browser.new_page()
            page.goto(temp_html.as_uri(), wait_until="networkidle")
            
            # Принудительная загрузка картинок
            page.evaluate("""
                async () => {
                    const images = document.querySelectorAll('img');
                    for (const img of images) {
                        if (!img.complete) {
                            await new Promise((r) => { img.onload = r; img.onerror = r; });
                        }
                        try { await img.decode(); } catch (e) {}
                    }
                }
            """)
            
            page.pdf(
                path=temp_pdf,
                format="A4",
                print_background=True,
                margin={"top": "20px", "bottom": "20px", "left": "20px", "right": "20px"}
            )
            page.close()
            
            temp_pdfs.append(temp_pdf)
            os.remove(temp_html)

        browser.close()

    # Склеивание всех частей в один файл
    print(f"[*] Merging {len(temp_pdfs)} PDF parts...")
    merger = PdfWriter()
    for pdf in temp_pdfs:
        merger.append(pdf)
    
    final_output = f"{pdf_filename}.pdf" if not pdf_filename.endswith(".pdf") else pdf_filename
    merger.write(final_output)
    merger.close()

    # Удаление временных PDF
    for pdf in temp_pdfs:
        if os.path.exists(pdf):
            os.remove(pdf)
            
    print(f"[+] Success! Final file: {final_output}")

if __name__ == "__main__":
    print("=== Collected Data to PDF Converter ===")
    print("1. Twitter (bookmarks.json -> twitter_bookmarks.pdf)")
    print("2. Discord (disc_msgs.json -> discord_messages.pdf)")
    print("3. Bluesky (bsky_bookmarks.json -> bsky_bookmarks.pdf)")
    
    choice = input("\nChoose a file for conversion (1, 2 or 3): ").strip()
    file_name = input("Enter the JSON filename (with .json extension): ").strip()

    convert_json_to_pdf(file_name, file_name)
