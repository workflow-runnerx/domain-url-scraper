"""Per-provider feed downloads, normalised to one row shape.

One place: every auction provider is fetched here, on a GitHub runner, and
posted to domain-metrics as parsed rows. Previously NameJet came from a runner
(Cloudflare) while the rest downloaded inside the cluster — two mechanisms, and
the in-cluster ones put a 190 MB CSV through the pod that serves the API.

Row shape matches what the platform's own parsers produced, so a feed fetched
here is indistinguishable from one fetched the old way:

    {add_date, domain_name, domain_type, price, end_date, provider, status}

Only NameJet needs a browser; the rest are plain HTTP. That is a property of
how each site has bot protection configured today, not of the code — if another
provider starts challenging, it gets the same browser treatment.
"""

from __future__ import annotations

import csv
import io
import zipfile
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional

import requests

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/15.3 Safari/605.1.15"
)


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _row(domain: str, price: Any, end_date: str, provider: str,
         domain_type: str = "AUCTION") -> Dict[str, Any]:
    return {
        "add_date": _today(),
        "domain_name": (domain or "").strip(),
        "domain_type": domain_type,
        "price": price,
        "end_date": end_date or _today(),
        "provider": provider,
        "status": "INITIATE",
    }


def _stream_csv(url: str, encoding: str = "utf-8", skip: int = 0,
                delimiter: str = ",", headers: Optional[Dict] = None,
                log: Callable[[str], None] = print) -> Iterator[Dict[str, str]]:
    """Stream a remote CSV row by row.

    Deliberately streaming: the Namecheap file is ~190 MB and reading it whole
    (or via pandas) is what used to OOM-kill the API pod.
    """
    h = {"User-Agent": UA}
    if headers:
        h.update(headers)
    with requests.get(url, stream=True, headers=h, timeout=(20, 300)) as r:
        r.raise_for_status()
        r.encoding = encoding
        lines = r.iter_lines(decode_unicode=True)
        for _ in range(skip):
            next(lines, None)
        reader = csv.DictReader(
            (ln for ln in lines if ln is not None), delimiter=delimiter
        )
        for row in reader:
            yield row


# --------------------------------------------------------------------------- #
# Per-provider parsers
# --------------------------------------------------------------------------- #

def fetch_namecheap(log=print) -> Iterator[Dict[str, Any]]:
    """~190 MB / ~1.2M rows. Streamed, never held whole."""
    url = "https://d3ry1h4w5036x1.cloudfront.net/reports/Namecheap_Market_Sales.csv"
    log(f"  namecheap: streaming {url}")
    for row in _stream_csv(url, encoding="utf-8", log=log):
        raw_end = (row.get("endDate") or "").strip()
        end = raw_end.split("T")[0] if raw_end else _today()
        yield _row(row.get("name"), row.get("price"), end, "Namecheap")


def fetch_sedo(log=print) -> Iterator[Dict[str, Any]]:
    """UTF-16LE with a `sep=;` preamble line — hence skip=1 and the delimiter."""
    url = "https://sedo.com/fileadmin/documents/resources/expiring_domain_auctions.csv"
    log(f"  sedo: streaming {url}")
    for row in _stream_csv(url, encoding="utf-16-le", skip=1, delimiter=";", log=log):
        clean = {(k or "").strip().strip('"'): v for k, v in row.items()}
        end_raw = (clean.get("End Time") or "").strip().strip('"')
        end = end_raw.split()[0] if end_raw else _today()
        price = (clean.get("Reserve Price") or "").strip().strip('"') or None
        yield _row(clean.get("Domain Name", "").strip('"'), price, end, "Sedo")


def fetch_godaddy(log=print) -> Iterator[Dict[str, Any]]:
    """A ~30 MB zip holding one CSV."""
    url = "https://inventory.auctions.godaddy.com/tdnam_all_no_adult_listings.csv.zip"
    log(f"  godaddy: downloading {url}")
    r = requests.get(url, headers={"User-Agent": UA,
                                   "Referer": "https://www.godaddy.com/"},
                     timeout=(20, 300))
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        name = z.namelist()[0]
        log(f"  godaddy: parsing {name}")
        with z.open(name) as fh:
            text = io.TextIOWrapper(fh, encoding="utf-8", errors="replace")
            for row in csv.DictReader(text):
                end = (row.get("auctionEndTime") or row.get("Auction End Time") or "").strip()
                end = end.split("T")[0].split()[0] if end else _today()
                yield _row(
                    row.get("domainName") or row.get("Domain Name"),
                    row.get("price") or row.get("Price"),
                    end, "Godaddy",
                )


def _dropcatch(kind: str, log=print) -> Iterator[Dict[str, Any]]:
    """DropCatch hands out a signed file URL, then serves a zipped CSV."""
    params = (
        {"FileType": "csv", "RequestType": "Dropping", "BackorderDay": "AllDays"}
        if kind == "pending_delete"
        else {"FileType": "csv", "RequestType": "Auction", "BackorderDay": "AllAuctions"}
    )
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.dropcatch.com",
        "Referer": "https://www.dropcatch.com/",
        "User-Agent": UA,
    }
    log(f"  dropcatch({kind}): resolving file url")
    r = requests.get("https://client.dropcatch.com/GetFileUrl",
                     params=params, headers=headers, timeout=(20, 120))
    r.raise_for_status()
    payload = r.json()
    file_url = payload.get("result") or payload.get("Result") or payload.get("url")
    if not file_url:
        raise RuntimeError(f"dropcatch: no file url in response: {str(payload)[:200]}")

    log(f"  dropcatch({kind}): downloading {file_url[:80]}")
    fr = requests.get(file_url, headers={"User-Agent": UA}, timeout=(20, 300))
    fr.raise_for_status()

    domain_type = "PENDING_DELETE" if kind == "pending_delete" else "AUCTION"
    provider = ("Dropcatch Pending Delete" if kind == "pending_delete"
                else "Dropcatch Auctions")
    skip = 0 if kind == "pending_delete" else 1

    with zipfile.ZipFile(io.BytesIO(fr.content)) as z:
        name = z.namelist()[0]
        with z.open(name) as fh:
            text = io.TextIOWrapper(fh, encoding="unicode_escape", errors="replace")
            for _ in range(skip):
                text.readline()
            for row in csv.DictReader(text):
                end = (row.get("Drop Date") if kind == "pending_delete"
                       else row.get("Auction End")) or _today()
                yield _row(row.get("Domain Name") or row.get("Domain"),
                           row.get("Price") or row.get("Current Bid"),
                           str(end).split()[0], provider, domain_type)


def fetch_dropcatch_pending_delete(log=print):
    return _dropcatch("pending_delete", log)


def fetch_dropcatch_auctions(log=print):
    return _dropcatch("auctions", log)


# NameJet is handled separately in feed_fetcher.py: it is the one provider
# Cloudflare currently challenges, so it needs the browser.
PARSERS: Dict[str, Callable[..., Iterator[Dict[str, Any]]]] = {
    "namecheap": fetch_namecheap,
    "sedo": fetch_sedo,
    "godaddy": fetch_godaddy,
    "dropcatch_pending_delete": fetch_dropcatch_pending_delete,
    "dropcatch_auctions": fetch_dropcatch_auctions,
}
