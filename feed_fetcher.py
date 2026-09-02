#!/usr/bin/env python3
"""Download a Cloudflare-protected auction feed and push it to domain-metrics.

Why this exists
---------------
Provider feeds (NameJet first) sit behind a Cloudflare managed challenge. The
in-cluster browser-service could not clear it: the cluster egresses from one
fixed AWS address that Cloudflare consistently challenges, and SeleniumBase's
virtual-mouse click never earned a ``cf_clearance`` cookie. Every campaign
creation that touched NameJet failed with a 502 while a user waited.

This runs instead on a GitHub Actions runner — a fresh IP every run, no proxy
needed — using the same ungoogled-chromium + cf-autoclick stack the url-scraper
worker uses, which does clear the challenge.

The download itself is deliberately NOT a browser navigation: pointing Chrome
at a .csv makes it download the file rather than render it, so there would be
nothing in the DOM to read. Instead we clear the challenge, harvest the
session cookies over CDP (``Network.getAllCookies`` also returns HttpOnly
cookies, which ``document.cookie`` hides — and ``cf_clearance`` is HttpOnly),
then fetch the feed with plain HTTP carrying those cookies.

Deliberately standalone: it shares no code with scraper.py so the two can
evolve independently.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from typing import Any, Dict, List, Optional

import requests
import websocket

# Markers that mean Cloudflare served an interstitial instead of the real page.
CHALLENGE_MARKERS = ("just a moment", "challenge-platform", "cf_chl_opt", "cf-mitigated")

PROVIDERS: Dict[str, Dict[str, str]] = {
    "namejet": {
        "warmup_url": "https://www.namejet.com/",
        "feed_url": "https://www.namejet.com/file_dl.sn?file=alist.csv",
    },
}


def log(msg: str) -> None:
    print(msg, flush=True)


def _free_port() -> int:
    s = socket.socket()
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Chrome:
    """A throwaway headed chromium with cf-autoclick loaded, driven over CDP."""

    def __init__(self, chrome_bin: str, extension_dir: Optional[str]):
        self.chrome_bin = chrome_bin
        self.extension_dir = extension_dir
        self.proc: Optional[subprocess.Popen] = None
        self.profile = tempfile.mkdtemp(prefix="feedfetch_")
        self.ws: Optional[websocket.WebSocket] = None
        self._id = 0

    def __enter__(self) -> "Chrome":
        port = _free_port()
        args = [
            self.chrome_bin,
            f"--user-data-dir={self.profile}",
            f"--remote-debugging-port={port}",
            "--remote-allow-origins=*",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-background-networking",
            "--disable-sync",
            "--disable-translate",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--window-size=1280,1024",
        ]
        # cf-autoclick is what actually clears the Turnstile checkbox. Chrome
        # refuses to load extensions in headless mode, so this run is headed —
        # the workflow supplies an Xvfb display.
        if self.extension_dir and os.path.isdir(self.extension_dir):
            args.append(f"--load-extension={self.extension_dir}")
            log(f"  cf-autoclick loaded from {self.extension_dir}")
        else:
            log("  WARNING: cf-autoclick not found; the challenge will likely stand")

        self.proc = subprocess.Popen(
            args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )

        base = f"http://127.0.0.1:{port}"
        ws_url = None
        for _ in range(40):
            time.sleep(0.5)
            try:
                for tab in requests.get(f"{base}/json", timeout=2).json():
                    if tab.get("type") == "page":
                        ws_url = tab["webSocketDebuggerUrl"]
                        break
                if ws_url:
                    break
            except Exception:
                continue
        if not ws_url:
            raise RuntimeError("chromium did not expose a CDP page target")

        self.ws = websocket.create_connection(ws_url, timeout=60)
        self.call("Page.enable")
        self.call("Runtime.enable")
        self.call("Network.enable")
        return self

    def __exit__(self, *exc: Any) -> None:
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass
        if self.proc:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except Exception:
                self.proc.kill()
        shutil.rmtree(self.profile, ignore_errors=True)

    def call(self, method: str, params: Optional[Dict[str, Any]] = None,
             timeout: int = 45) -> Dict[str, Any]:
        assert self.ws is not None
        self._id += 1
        msg_id = self._id
        self.ws.send(json.dumps({"id": msg_id, "method": method, "params": params or {}}))
        deadline = time.time() + timeout
        while time.time() < deadline:
            self.ws.settimeout(3)
            try:
                got = json.loads(self.ws.recv())
            except Exception:
                continue
            if got.get("id") == msg_id:
                return got
        raise RuntimeError(f"CDP timeout waiting for {method}")

    def eval_js(self, expression: str) -> Any:
        got = self.call(
            "Runtime.evaluate", {"expression": expression, "returnByValue": True}
        )
        return got.get("result", {}).get("result", {}).get("value")

    def eval_js_slow(self, expression: str, timeout: int = 240) -> Any:
        """eval_js with a long timeout — the feed body is multi-megabyte."""
        got = self.call(
            "Runtime.evaluate",
            {"expression": expression, "returnByValue": True},
            timeout=timeout,
        )
        return got.get("result", {}).get("result", {}).get("value")


def clear_challenge(chrome: Chrome, warmup_url: str, timeout: int) -> None:
    """Navigate the origin and wait until Cloudflare stops interstitialling."""
    log(f"  warming up {warmup_url}")
    chrome.call("Page.navigate", {"url": warmup_url})

    deadline = time.time() + timeout
    last_title = ""
    while time.time() < deadline:
        time.sleep(2)
        title = (chrome.eval_js("document.title") or "").strip()
        if title != last_title:
            log(f"  title: {title!r}")
            last_title = title
        if title and not any(m in title.lower() for m in CHALLENGE_MARKERS):
            log("  challenge cleared")
            # Let cf_clearance settle before harvesting.
            time.sleep(3)
            return
    raise RuntimeError(
        f"Cloudflare challenge not cleared within {timeout}s "
        f"(last title: {last_title!r})"
    )


def harvest_cookies(chrome: Chrome) -> List[Dict[str, Any]]:
    """All cookies including HttpOnly ones — cf_clearance is HttpOnly."""
    got = chrome.call("Network.getAllCookies")
    cookies = got.get("result", {}).get("cookies", []) or []
    names = sorted({c.get("name", "") for c in cookies})
    log(f"  harvested {len(cookies)} cookies: {names}")
    if not any(c.get("name") == "cf_clearance" for c in cookies):
        log("  WARNING: no cf_clearance cookie — the download will probably 403")
    return cookies


def download_feed_in_browser(chrome: Chrome, feed_url: str) -> str:
    """Fetch the feed from inside the cleared page.

    A plain ``requests`` GET carrying the harvested cookies is NOT enough:
    Cloudflare also fingerprints the TLS handshake, so a Python client presents
    as a bot and still gets 403 even holding a valid cf_clearance. Issuing the
    request from the browser reuses Chrome's own connection, fingerprint,
    cookie jar and headers, so it is indistinguishable from the page's own
    traffic.

    Synchronous XHR on purpose: ``Runtime.evaluate`` is called without
    ``awaitPromise``, so a fetch() promise would come back unresolved. The feed
    is same-origin with the warmup page, so no CORS is involved.
    """
    log(f"  downloading {feed_url} from inside the browser")
    js = (
        "(() => { try {"
        " var x = new XMLHttpRequest();"
        f" x.open('GET', {json.dumps(feed_url)}, false);"
        " x.send(null);"
        " return JSON.stringify({status: x.status, body: x.responseText});"
        " } catch (e) { return JSON.stringify({status: -1, error: String(e)}); } })()"
    )
    raw = chrome.eval_js_slow(js)
    if not raw:
        raise RuntimeError("in-browser download returned nothing")
    payload = json.loads(raw)
    status = payload.get("status")
    if status != 200:
        raise RuntimeError(
            f"in-browser download failed: HTTP {status} "
            f"{payload.get('error', '')}".strip()
        )
    body = payload.get("body") or ""
    log(f"  HTTP {status}, {len(body.encode('utf-8'))} bytes")
    if any(m in body[:4096].lower() for m in CHALLENGE_MARKERS):
        raise RuntimeError("feed body is a Cloudflare challenge page, not the file")
    return body


def push_feed(api_url: str, token: str, provider: str, body: str,
              source_url: str) -> Dict[str, Any]:
    url = f"{api_url.rstrip('/')}/provider-feed/{provider}/"
    log(f"  pushing {len(body.encode('utf-8'))} bytes to {url}")
    resp = requests.post(
        url,
        params={"source_url": source_url},
        data=body.encode("utf-8"),
        headers={"X-Feed-Token": token, "Content-Type": "text/csv; charset=utf-8"},
        timeout=180,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"ingest rejected the feed: HTTP {resp.status_code} {resp.text[:400]}")
    return resp.json()


def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch a provider auction feed and push it")
    ap.add_argument("--provider", default="namejet", choices=sorted(PROVIDERS))
    ap.add_argument(
        "--api-url",
        default=os.getenv(
            "FEED_API_URL",
            "https://b-domain.articleinnovator.com/domain-metrics-management-service/api/v1",
        ),
    )
    ap.add_argument("--token", default=os.getenv("FEED_TOKEN", ""))
    ap.add_argument("--chrome", default=os.getenv("CHROME_BIN", "vendor/ungoogled-chromium/chrome"))
    ap.add_argument("--extension", default=os.getenv("CF_AUTOCLICK_DIR", "vendor/cf-autoclick"))
    ap.add_argument("--challenge-timeout", type=int, default=120)
    ap.add_argument("--attempts", type=int, default=3)
    args = ap.parse_args()

    if not args.token:
        log("FATAL: no feed token (pass --token or set FEED_TOKEN)")
        return 2

    spec = PROVIDERS[args.provider]
    last_error: Optional[Exception] = None

    for attempt in range(1, args.attempts + 1):
        log(f"=== {args.provider}: attempt {attempt}/{args.attempts}")
        try:
            with Chrome(args.chrome, args.extension) as chrome:
                clear_challenge(chrome, spec["warmup_url"], args.challenge_timeout)
                # Logged for diagnosis: cf_clearance present means the
                # challenge really cleared, which is the hard part.
                harvest_cookies(chrome)
                body = download_feed_in_browser(chrome, spec["feed_url"])
            out = push_feed(args.api_url, args.token, args.provider, body, spec["feed_url"])
            log(f"✅ stored: {json.dumps(out.get('data', out))}")
            return 0
        except Exception as e:  # noqa: BLE001
            last_error = e
            log(f"  attempt {attempt} failed: {type(e).__name__}: {e}")
            if attempt < args.attempts:
                backoff = 15 * attempt
                log(f"  retrying in {backoff}s")
                time.sleep(backoff)

    log(f"❌ {args.provider}: all {args.attempts} attempts failed: {last_error}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
