# src/makePDF.py
import os
import json
import html
import base64
from io import BytesIO
from pathlib import Path
from playwright.sync_api import sync_playwright
from pypdf import PdfWriter

try:
    from PIL import Image
except ImportError:
    print("[!] PIL is required. Install it: pip install Pillow")
    exit(1)

def get_compressed_image_b64(image_path, max_width=800):
    """Сжимает изображение и возвращает base64-строку."""
    try:
        with Image.open(image_path) as img:
            if img.mode in ("RGBA", "P", "LA"):
                background = Image.new("RGB", img.size, (255, 255, 255))
                if img.mode == "RGBA":
                    background.paste(img, mask=img.split()[3])
                else:
                    background.paste(img)
                img = background
            elif img.mode != "RGB":
                img = img.convert("RGB")
            
            if img.width > max_width:
                ratio = max_width / img.width
                new_size = (max_width, int(img.height * ratio))
                img = img.resize(new_size, Image.Resampling.LANCZOS)
            
            buffer = BytesIO()
            img.save(buffer, format="JPEG", quality=75, optimize=True)
            b64_str = base64.b64encode(buffer.getvalue()).decode("utf-8")
            return f"data:image/jpeg;base64,{b64_str}"
    except Exception as e:
        print(f"[!] Error compressing image {os.path.basename(image_path)}: {e}")
        return None

def generate_html_content(data):
    """Формирует HTML разметку на базе входного массива."""
    html_parts = [
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
            html_parts.append(f"<div class='text'>{html.escape(item['text'])}</div>")

        if "local_media" in item and item["local_media"]:
            html_parts.append("<div class='media-grid'>")
            for media_path in item["local_media"]:
                if not os.path.exists(media_path):
                    html_parts.append(f"<div class='video-label'>⚠️ File missing: {os.path.basename(media_path)}</div>")
                    continue
                
                ext = media_path.lower().split('.')[-1]
                if ext in ['jpg', 'jpeg', 'png', 'gif', 'webp']:
                    b64_img = get_compressed_image_b64(media_path)
                    if b64_img:
                        html_parts.append(f"<img src='{b64_img}'>")
                    else:
                        file_uri = Path(media_path).absolute().as_uri()
                        html_parts.append(f"<img src='{file_uri}'>")
                else:
                    html_parts.append(f"<div class='video-label'>🎥 Video/Attachment: {os.path.basename(media_path)}</div>")
            html_parts.append("</div>")
        html_parts.append("</div>")

    html_parts.append("</body></html>")
    return "".join(html_parts)

def convert_json_to_pdf(json_filename, pdf_filename, keep_temp=False):
    """Конвертирует JSON-файл в PDF."""
    print(f"[*] Reading file: {json_filename}...")
    try:
        with open(json_filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"[!] Error: {e}")
        return

    if not data:
        print("[!] JSON dataset is empty.")
        return

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

    print(f"[*] Merging {len(temp_pdfs)} PDF parts...")
    merger = PdfWriter()
    for pdf in temp_pdfs:
        merger.append(pdf)
    
    for page in merger.pages:
        page.compress_content_streams()
    
    final_output = f"{pdf_filename}.pdf" if not pdf_filename.endswith(".pdf") else pdf_filename
    merger.write(final_output)
    merger.close()

    if not keep_temp:
        for pdf in temp_pdfs:
            if os.path.exists(pdf):
                os.remove(pdf)
        print("[*] Temporary files cleaned up.")
            
    print(f"[+] PDF generation completed: {final_output}")
