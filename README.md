# domain-url-scraper

Lives under `workflow-runnerx` with the other scraper workers
(`url-scraper-local`, `ahref-local`, `moz-local`, `map-local`, `bing-local`);
`domain-metrcis` holds the platform services.

Browser automation workers for the **domain-metrics** platform. Two independent
jobs live here; they share a repo (and the chromium + cf-autoclick setup) but no
code, so either can change without touching the other.

## 1. URL scraper worker — `scraper.py`

Pull-based. Drains `url-scraper.pool-queue` through the management service:

```
GET  {api}/url-scraper/    -> one execution record (204 when the queue is empty)
POST {api}/url-scraper/    -> {execution_record, result}
```

It navigates `target_url`, evaluates the job's `selectors` in the page, and
posts the extracted values back. Selector specs are defined **server-side**, so
a new scrape shape needs no worker deploy. Two job kinds use it today: ordinary
page scrapes, and the auction end-time refresh (`job_type: auction_end`).

Run by `.github/workflows/url-scraper-vnc.yaml` (manual dispatch), which brings
up XFCE + TurboVNC so you can watch the browser while it works.

## 2. Provider feed fetcher — `feed_fetcher.py`

Scheduled. Downloads Cloudflare-protected auction feeds (NameJet today) and
POSTs them to `/provider-feed/{provider}/`, where they are cached so campaign
creation reads them instantly instead of waiting on a live scrape.

Run by `.github/workflows/provider-feeds.yml` (cron + manual dispatch).

### Why it runs on a GitHub runner

The in-cluster `browser-service` could not clear NameJet's Cloudflare challenge:
the cluster egresses from one fixed AWS address Cloudflare consistently
challenges. A runner gets a fresh IP every run, so **no residential proxy is
needed**.

### Two things that are easy to get wrong

1. **`cf_clearance` alone is not enough.** Cloudflare also fingerprints the TLS
   handshake, so a Python `requests` GET carrying valid cookies still gets 403.
   The download is issued from *inside* the cleared page instead.
2. **Synchronous XHR, not `fetch()`.** CDP `Runtime.evaluate` is called without
   `awaitPromise`, so a promise would come back unresolved.

## Secrets

| Name | Used by |
|---|---|
| `DM_FEED_TOKEN` | `provider-feeds.yml` → `X-Feed-Token` on feed ingest |
