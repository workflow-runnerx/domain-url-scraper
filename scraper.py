"""
URL Scraper — PULL-based worker (fresh Chrome per job via CDP).

Files:
  - scraper.py (this file) — queue poller + Chrome lifecycle
  - url-scraper.json — Workflow schema (reference only, JS is embedded here)

Model (matches ahref-local/ahrefs_checker.py):
  The worker POLLS the management service for a domain to scrape and POSTs the
  result back. No Flask server, no public tunnel, no self-registration.

  Loop (per worker thread):
    1. GET  {api-url}/url-scraper/         -> execution_record  (or 204 = idle)
    2. Launch fresh ungoogled-chromium (+ cf-autoclick), navigate, extract via CDP
    3. POST {api-url}/url-scraper/ {execution_record, result}
    4. Kill chrome, delete profile; repeat

API base (default): the domain-metrics management service behind b-domain.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import socket
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

import requests as http_requests

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #

SCRIPT_DIR = Path(__file__).parent.resolve()

# Paths — override with env vars
CHROME_BIN = os.getenv("CHROME_BIN", str(SCRIPT_DIR / "vendor" / "ungoogled-chromium" / "chrome"))
CF_AUTOCLICK_DIR = os.getenv("CF_AUTOCLICK_DIR", str(SCRIPT_DIR / "vendor" / "cf-autoclick"))
DISPLAY = os.getenv("DISPLAY", ":0")
SCRAPE_TIMEOUT = int(os.getenv("SCRAPE_TIMEOUT", "60"))

# Pull-based config
DEFAULT_API_URL = os.getenv(
    "SCRAPER_API_URL",
    "https://b-domain.articleinnovator.com/domain-metrics-management-service/api/v1",
)
# Endpoint path (GET to pop a job, POST to submit the result).
SCRAPER_ENDPOINT = "/url-scraper/"
# Seconds to sleep when the queue is empty (204) before polling again.
IDLE_POLL_INTERVAL = int(os.getenv("SCRAPER_IDLE_POLL_INTERVAL", "5"))
WAIT_FOR_TIMEOUT = int(os.getenv("SCRAPER_WAIT_FOR_TIMEOUT", "30"))

_stats = {"processed": 0, "errors": 0, "active": 0, "started_at": time.time()}
_stats_lock = threading.Lock()


# --------------------------------------------------------------------------- #
# Helper: find free port
# --------------------------------------------------------------------------- #

def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


# --------------------------------------------------------------------------- #
# Default selectors (when none provided)
# --------------------------------------------------------------------------- #

DEFAULT_SELECTORS = [
    {"name": "source_title", "selector": "title", "js_query": "document.title", "is_multiple_value": False, "remove_selector": []},
    {"name": "source_content", "selector": "article", "js_query": "(() => { let el = document.querySelector('article') || document.querySelector('.article-body, .post-content, .entry-content, [role=main], main'); return el ? el.innerText : document.body.innerText.substring(0,50000); })()", "is_multiple_value": False, "remove_selector": ["script","style","nav","header","footer","aside","ins","iframe"]},
    {"name": "source_author", "selector": "meta[name='author']", "js_query": "document.querySelector('meta[name=\"author\"]')?.content || ''", "is_multiple_value": False, "remove_selector": []},
    {"name": "source_published_date", "selector": "meta[property='article:published_time']", "js_query": "document.querySelector('meta[property=\"article:published_time\"]')?.content || document.querySelector('time[datetime]')?.getAttribute('datetime') || ''", "is_multiple_value": False, "remove_selector": []},
    {"name": "source_featured_image", "selector": "meta[property='og:image']", "js_query": "document.querySelector('meta[property=\"og:image\"]')?.content || ''", "is_multiple_value": False, "remove_selector": []},
    {"name": "source_excerpt", "selector": "meta[name='description']", "js_query": "document.querySelector('meta[name=\"description\"]')?.content || document.querySelector('meta[property=\"og:description\"]')?.content || ''", "is_multiple_value": False, "remove_selector": []},
]


# --------------------------------------------------------------------------- #
# Build extraction JS from selectors
# --------------------------------------------------------------------------- #

def build_extraction_js(selectors: list, remove_tags: list = None) -> str:
    """Build JavaScript that extracts data using the provided selectors."""
    selectors_json = json.dumps(selectors)
    # ponytail: remove_tags strips entire HTML elements globally before extraction
    remove_tags_js = ""
    if remove_tags:
        tag_selector = ", ".join(remove_tags)
        remove_tags_js = f"document.querySelectorAll('{tag_selector}').forEach(el => el.remove());"
    return f"""
    (() => {{
        {remove_tags_js}
        const selectors = {selectors_json};
        const results = [];
        for (const sel of selectors) {{
            const result = {{ name: sel.name, selector: sel.selector, value: null }};
            try {{
                if (sel.remove_selector && sel.remove_selector.length > 0) {{
                    sel.remove_selector.forEach(rs => {{
                        document.querySelectorAll(rs).forEach(el => el.remove());
                    }});
                }}
                if (sel.js_query) {{
                    try {{ result.value = eval(sel.js_query); }} catch(e) {{}}
                }}
                if (!result.value) {{
                    if (sel.is_multiple_value) {{
                        const els = document.querySelectorAll(sel.selector);
                        result.value = Array.from(els).map(el => 
                            el.getAttribute('content') || el.getAttribute('href') || el.getAttribute('src') || el.innerText.trim()
                        );
                    }} else {{
                        const el = document.querySelector(sel.selector);
                        if (el) result.value = el.getAttribute('content') || el.getAttribute('href') || el.getAttribute('src') || el.innerText.trim();
                    }}
                }}
            }} catch(e) {{ result.error = e.message; }}
            results.push(result);
        }}
        return JSON.stringify(results);
    }})()
    """


# --------------------------------------------------------------------------- #
# Core: scrape with fresh chrome via CDP
# --------------------------------------------------------------------------- #

def scrape_url(target_url: str, selectors: list, remove_tags: list = None,
               wait_for: str = None) -> dict:
    """Launch chrome, navigate, extract, kill. Returns parsed result.

    wait_for: optional CSS selector to poll for before extracting. Client-side
    frameworks (Angular, React) render after load, so a fixed sleep either
    wastes time or extracts an empty DOM. When given, we poll until the element
    appears (up to WAIT_FOR_TIMEOUT) instead of sleeping blind.
    """
    
    if not selectors:
        selectors = DEFAULT_SELECTORS
    
    profile_dir = tempfile.mkdtemp(prefix="scrape_")
    cdp_port = _free_port()
    chrome_proc = None
    
    try:
        # 1. Launch chrome
        chrome_args = [
            CHROME_BIN,
            f"--user-data-dir={profile_dir}",
            f"--remote-debugging-port={cdp_port}",
            "--remote-allow-origins=*",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-background-networking",
            "--disable-sync",
            "--disable-translate",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--headless=new",
        ]
        
        # Add cf-autoclick extension if exists
        if os.path.isdir(CF_AUTOCLICK_DIR):
            chrome_args.append(f"--load-extension={CF_AUTOCLICK_DIR}")
            # Can't use headless with extensions, switch to headed
            chrome_args = [a for a in chrome_args if a != "--headless=new"]
        
        env = os.environ.copy()
        env["DISPLAY"] = DISPLAY
        
        chrome_proc = subprocess.Popen(
            chrome_args, env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        
        # 2. Wait for CDP to be ready
        cdp_base = f"http://127.0.0.1:{cdp_port}"
        ready = False
        for _ in range(15):
            time.sleep(1)
            try:
                r = http_requests.get(f"{cdp_base}/json/version", timeout=2)
                if r.status_code == 200:
                    ready = True
                    break
            except Exception:
                pass
        
        if not ready:
            return {"success": False, "error": "Chrome CDP not ready after 15s"}
        
        # 3. Get a page target (or create new tab)
        tabs = http_requests.get(f"{cdp_base}/json", timeout=5).json()
        page_tabs = [t for t in tabs if t.get("type") == "page"]
        
        if not page_tabs:
            # Create a new tab
            r = http_requests.put(f"{cdp_base}/json/new?about:blank", timeout=5)
            tabs = http_requests.get(f"{cdp_base}/json", timeout=5).json()
            page_tabs = [t for t in tabs if t.get("type") == "page"]
        
        if not page_tabs:
            return {"success": False, "error": "No page target available"}
        
        ws_url = page_tabs[0]["webSocketDebuggerUrl"]
        
        # 4. Connect via WebSocket and navigate
        import websocket
        ws = websocket.create_connection(ws_url, timeout=SCRAPE_TIMEOUT)
        msg_id = 1
        
        def send_cdp(method, params=None):
            nonlocal msg_id
            msg = {"id": msg_id, "method": method, "params": params or {}}
            ws.send(json.dumps(msg))
            msg_id += 1
            # Wait for response with matching id
            while True:
                resp = json.loads(ws.recv())
                if resp.get("id") == msg_id - 1:
                    return resp
                # Also handle events (just skip)
        
        # Enable Page events
        send_cdp("Page.enable")
        
        # Navigate
        send_cdp("Page.navigate", {"url": target_url})
        
        # Wait for load. With wait_for, poll for the element instead of
        # sleeping blind — client-rendered pages populate late.
        if wait_for:
            deadline = time.time() + WAIT_FOR_TIMEOUT
            found = False
            while time.time() < deadline:
                probe = send_cdp("Runtime.evaluate", {
                    "expression": f"!!document.querySelector({json.dumps(wait_for)})",
                    "returnByValue": True,
                })
                if probe.get("result", {}).get("result", {}).get("value") is True:
                    found = True
                    break
                time.sleep(0.5)
            if not found:
                print(f"  ⚠️ wait_for {wait_for!r} never appeared "
                      f"after {WAIT_FOR_TIMEOUT}s; extracting anyway")
            # Let the rest of the view settle once the anchor is present.
            time.sleep(2)
        else:
            time.sleep(8)
        
        # 5. Extract data using JS
        extraction_js = build_extraction_js(selectors, remove_tags)
        result = send_cdp("Runtime.evaluate", {
            "expression": extraction_js,
            "returnByValue": True,
        })
        
        scraped_raw = result.get("result", {}).get("result", {}).get("value", "[]")
        
        # 6. Get page HTML
        html_result = send_cdp("Runtime.evaluate", {
            "expression": "document.documentElement.outerHTML",
            "returnByValue": True,
        })
        page_html = html_result.get("result", {}).get("result", {}).get("value", "")
        
        # 7. Get page info
        info_result = send_cdp("Runtime.evaluate", {
            "expression": "JSON.stringify({title: document.title, url: window.location.href, domain: window.location.hostname})",
            "returnByValue": True,
        })
        page_info_raw = info_result.get("result", {}).get("result", {}).get("value", "{}")
        
        ws.close()
        
        # 8. Parse results
        try:
            scraped_data = json.loads(scraped_raw) if isinstance(scraped_raw, str) else scraped_raw
        except Exception:
            scraped_data = []
        
        try:
            page_info = json.loads(page_info_raw) if isinstance(page_info_raw, str) else page_info_raw
        except Exception:
            page_info = {}
        
        # Build variables dict
        variables = {}
        for item in (scraped_data if isinstance(scraped_data, list) else []):
            if isinstance(item, dict) and item.get("name"):
                variables[item["name"]] = item.get("value")
        
        variables["page_html"] = page_html
        if page_info:
            variables["__page_title"] = page_info.get("title", "")
            variables["__page_url"] = page_info.get("url", target_url)
        
        return {
            "success": True,
            "data": {
                "variables": variables,
                "scraped_data": scraped_data,
                "page_info": page_info,
            }
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}
    
    finally:
        # ALWAYS cleanup
        if chrome_proc and chrome_proc.poll() is None:
            try:
                chrome_proc.terminate()
                chrome_proc.wait(timeout=5)
            except Exception:
                chrome_proc.kill()
        shutil.rmtree(profile_dir, ignore_errors=True)


# --------------------------------------------------------------------------- #
# Core: screenshot with fresh chrome via CDP
# --------------------------------------------------------------------------- #

def screenshot_url(target_url: str, full_page: bool = True, width: int = 1920, height: int = 1080, wait: int = 5) -> dict:
    """Launch chrome, navigate, take screenshot, kill. Returns base64 PNG."""
    
    profile_dir = tempfile.mkdtemp(prefix="screenshot_")
    cdp_port = _free_port()
    chrome_proc = None
    
    try:
        # 1. Launch chrome
        chrome_args = [
            CHROME_BIN,
            f"--user-data-dir={profile_dir}",
            f"--remote-debugging-port={cdp_port}",
            "--remote-allow-origins=*",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-background-networking",
            "--disable-sync",
            "--disable-translate",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            f"--window-size={width},{height}",
        ]
        
        # Add cf-autoclick extension if exists
        if os.path.isdir(CF_AUTOCLICK_DIR):
            chrome_args.append(f"--load-extension={CF_AUTOCLICK_DIR}")
        else:
            chrome_args.append("--headless=new")
        
        env = os.environ.copy()
        env["DISPLAY"] = DISPLAY
        
        chrome_proc = subprocess.Popen(
            chrome_args, env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        
        # 2. Wait for CDP
        cdp_base = f"http://127.0.0.1:{cdp_port}"
        ready = False
        for _ in range(15):
            time.sleep(1)
            try:
                r = http_requests.get(f"{cdp_base}/json/version", timeout=2)
                if r.status_code == 200:
                    ready = True
                    break
            except Exception:
                pass
        
        if not ready:
            return {"success": False, "error": "Chrome CDP not ready after 15s"}
        
        # 3. Get page target
        tabs = http_requests.get(f"{cdp_base}/json", timeout=5).json()
        page_tabs = [t for t in tabs if t.get("type") == "page"]
        
        if not page_tabs:
            http_requests.put(f"{cdp_base}/json/new?about:blank", timeout=5)
            tabs = http_requests.get(f"{cdp_base}/json", timeout=5).json()
            page_tabs = [t for t in tabs if t.get("type") == "page"]
        
        if not page_tabs:
            return {"success": False, "error": "No page target available"}
        
        ws_url = page_tabs[0]["webSocketDebuggerUrl"]
        
        # 4. Connect via WebSocket and navigate
        import websocket
        ws = websocket.create_connection(ws_url, timeout=60)
        msg_id = 1
        
        def send_cdp(method, params=None):
            nonlocal msg_id
            msg = {"id": msg_id, "method": method, "params": params or {}}
            ws.send(json.dumps(msg))
            msg_id += 1
            while True:
                resp = json.loads(ws.recv())
                if resp.get("id") == msg_id - 1:
                    return resp
        
        # Set viewport
        send_cdp("Emulation.setDeviceMetricsOverride", {
            "width": width, "height": height,
            "deviceScaleFactor": 1, "mobile": False,
        })
        
        send_cdp("Page.enable")
        send_cdp("Page.navigate", {"url": target_url})
        
        # Wait for page to load
        time.sleep(wait)
        
        # 5. Take screenshot
        if full_page:
            # Get full page dimensions
            metrics = send_cdp("Page.getLayoutMetrics")
            content_size = metrics.get("result", {}).get("contentSize", {})
            page_width = content_size.get("width", width)
            page_height = content_size.get("height", height)
            
            # Set viewport to full page
            send_cdp("Emulation.setDeviceMetricsOverride", {
                "width": int(page_width), "height": int(page_height),
                "deviceScaleFactor": 1, "mobile": False,
            })
            time.sleep(1)
        
        screenshot_result = send_cdp("Page.captureScreenshot", {
            "format": "png",
            "quality": 90,
        })
        
        screenshot_data = screenshot_result.get("result", {}).get("data", "")
        
        # Get page info + text content
        info_result = send_cdp("Runtime.evaluate", {
            "expression": """JSON.stringify({
                title: document.title,
                url: window.location.href,
                text_content: document.body.innerText,
                meta_description: (document.querySelector('meta[name="description"]') || {}).content || '',
                meta_keywords: (document.querySelector('meta[name="keywords"]') || {}).content || '',
                og_title: (document.querySelector('meta[property="og:title"]') || {}).content || '',
                og_description: (document.querySelector('meta[property="og:description"]') || {}).content || '',
                og_image: (document.querySelector('meta[property="og:image"]') || {}).content || '',
                h1: Array.from(document.querySelectorAll('h1')).map(e => e.innerText).join(' | '),
                h2s: Array.from(document.querySelectorAll('h2')).map(e => e.innerText),
                links_count: document.querySelectorAll('a[href]').length,
                images_count: document.querySelectorAll('img').length,
                word_count: document.body.innerText.split(/\\s+/).filter(w => w.length > 0).length
            })""",
            "returnByValue": True,
        })
        page_info_raw = info_result.get("result", {}).get("result", {}).get("value", "{}")
        
        ws.close()
        
        try:
            page_info = json.loads(page_info_raw)
        except Exception:
            page_info = {}
        
        return {
            "success": True,
            "data": {
                "screenshot_base64": screenshot_data,
                "page_title": page_info.get("title", ""),
                "page_url": page_info.get("url", target_url),
                "text_content": page_info.get("text_content", ""),
                "meta": {
                    "description": page_info.get("meta_description", ""),
                    "keywords": page_info.get("meta_keywords", ""),
                    "og_title": page_info.get("og_title", ""),
                    "og_description": page_info.get("og_description", ""),
                    "og_image": page_info.get("og_image", ""),
                },
                "structure": {
                    "h1": page_info.get("h1", ""),
                    "h2s": page_info.get("h2s", []),
                    "links_count": page_info.get("links_count", 0),
                    "images_count": page_info.get("images_count", 0),
                    "word_count": page_info.get("word_count", 0),
                },
                "width": width,
                "height": page_height if full_page else height,
                "full_page": full_page,
            }
        }
    
    except Exception as e:
        return {"success": False, "error": str(e)}
    
    finally:
        if chrome_proc and chrome_proc.poll() is None:
            try:
                chrome_proc.terminate()
                chrome_proc.wait(timeout=5)
            except Exception:
                chrome_proc.kill()
        shutil.rmtree(profile_dir, ignore_errors=True)


# --------------------------------------------------------------------------- #
# Pull-based worker: poll the management queue, scrape, post the result back
# --------------------------------------------------------------------------- #

def _extract_target_url(record: Dict[str, Any]) -> Optional[str]:
    """Derive the URL to scrape from an execution record.

    Prefers an explicit target_url; falls back to source_url/url, or builds
    https://<domain_name> from the domain.
    """
    for key in ("target_url", "source_url", "url"):
        val = record.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    domain = record.get("domain_name")
    if isinstance(domain, str) and domain.strip():
        d = domain.strip()
        return d if d.startswith(("http://", "https://")) else f"https://{d}"
    return None


def _result_payload(record: Dict[str, Any], scrape_result: Dict[str, Any],
                    scraped_at: str = None) -> Dict[str, Any]:
    """Map an internal scrape_url() result to the management POST contract.

    Success -> {status: completed, success: True, content, extracted}.
    Failure -> {status: error, success: False, error}.

    scraped_at (UTC ISO-8601) is when the extraction actually ran. The server
    needs it to turn a RELATIVE countdown ("11h 24m") into an absolute auction
    end time; without it, it has to fall back to POST arrival time.
    """
    domain = record.get("domain_name", "")
    if not scrape_result.get("success"):
        return {
            "domain_name": domain,
            "status": "error",
            "success": False,
            "error": scrape_result.get("error", "scrape failed"),
            "scraped_at": scraped_at,
        }

    data = scrape_result.get("data", {}) or {}
    variables = data.get("variables", {}) or {}
    # Prefer the main article/body content; fall back to full page text/html.
    content = (
        variables.get("source_content")
        or variables.get("page_html")
        or ""
    )
    return {
        "domain_name": domain,
        "status": "completed",
        "success": True,
        "content": content,
        "extracted": variables,
        "scraped_at": scraped_at,
    }


def _process_one(session: http_requests.Session, api_base: str) -> bool:
    """Pop one job, scrape it, POST the result. Returns True if a job was
    processed, False if the queue was empty (204)."""
    get_url = f"{api_base}{SCRAPER_ENDPOINT}"
    try:
        resp = session.get(get_url, timeout=30)
    except Exception as e:  # noqa: BLE001
        print(f"  ⚠️ GET failed: {e}")
        time.sleep(IDLE_POLL_INTERVAL)
        return False

    if resp.status_code == 204:
        return False
    if resp.status_code != 200:
        print(f"  ⚠️ GET HTTP {resp.status_code}: {resp.text[:200]}")
        time.sleep(IDLE_POLL_INTERVAL)
        return False

    try:
        body = resp.json()
        record = body.get("data") or {}
    except Exception as e:  # noqa: BLE001
        print(f"  ⚠️ GET bad JSON: {e}")
        return False

    if not record:
        return False

    target_url = _extract_target_url(record)
    with _stats_lock:
        _stats["active"] += 1
    try:
        if not target_url:
            scrape_result = {"success": False, "error": "no target_url/domain_name in record"}
        else:
            print(f"  → scraping {target_url}")
            scrape_result = scrape_url(
                target_url,
                record.get("selectors") or [],
                wait_for=record.get("wait_for_selector") or None,
            )
    except Exception as e:  # noqa: BLE001
        scrape_result = {"success": False, "error": f"scrape exception: {e}"}
    finally:
        # Stamped as close to the extraction as possible: a relative countdown
        # is only meaningful against the moment it was read.
        scraped_at = datetime.now(timezone.utc).isoformat()
        with _stats_lock:
            _stats["active"] -= 1

    payload = {
        "execution_record": record,
        "result": _result_payload(record, scrape_result, scraped_at),
    }
    try:
        post_resp = session.post(get_url, json=payload, timeout=30)
        ok = post_resp.status_code == 200
    except Exception as e:  # noqa: BLE001
        print(f"  ⚠️ POST failed: {e}")
        ok = False

    with _stats_lock:
        if scrape_result.get("success") and ok:
            _stats["processed"] += 1
        else:
            _stats["errors"] += 1
    status = "ok" if scrape_result.get("success") else "FAILED"
    print(f"  ✓ {record.get('domain_name','?')} scrape={status} post={'ok' if ok else 'fail'}")
    return True


def _worker_loop(worker_id: int, api_base: str) -> None:
    """One worker thread: continuously pop + scrape + submit."""
    session = http_requests.Session()
    print(f"[worker {worker_id}] polling {api_base}{SCRAPER_ENDPOINT}")
    while True:
        try:
            processed = _process_one(session, api_base)
            if not processed:
                time.sleep(IDLE_POLL_INTERVAL)
        except KeyboardInterrupt:
            break
        except Exception as e:  # noqa: BLE001
            print(f"[worker {worker_id}] loop error: {e}")
            time.sleep(IDLE_POLL_INTERVAL)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="URL Scraper pull-based worker")
    parser.add_argument(
        "--api-url", type=str, default=DEFAULT_API_URL,
        help="Management API base (…/api/v1). Worker polls {base}/url-scraper/.",
    )
    parser.add_argument("--workers", type=int, default=1, help="Parallel scraper workers")
    parser.add_argument("--timeout", type=int, default=SCRAPE_TIMEOUT)
    parser.add_argument("--chrome", type=str, default=CHROME_BIN)
    parser.add_argument("--extension", type=str, default=CF_AUTOCLICK_DIR)
    args = parser.parse_args()

    CHROME_BIN = args.chrome
    CF_AUTOCLICK_DIR = args.extension
    SCRAPE_TIMEOUT = args.timeout
    api_base = args.api_url.rstrip("/")

    print("🚀 URL Scraper (pull-based) starting")
    print(f"   API:       {api_base}{SCRAPER_ENDPOINT}")
    print(f"   Workers:   {args.workers}")
    print(f"   Chrome:    {CHROME_BIN}")
    print(f"   Extension: {CF_AUTOCLICK_DIR}")
    print(f"   Timeout:   {SCRAPE_TIMEOUT}s")

    if args.workers <= 1:
        _worker_loop(0, api_base)
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            for wid in range(args.workers):
                pool.submit(_worker_loop, wid, api_base)
            # Block forever (workers loop internally).
            while True:
                time.sleep(3600)
