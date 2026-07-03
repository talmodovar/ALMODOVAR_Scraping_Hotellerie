import os
import sys
import json
import logging
import time
from pathlib import Path
import requests
from playwright.sync_api import sync_playwright

# Configure logging to write to stderr so stdout only contains JSON stream
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stderr
)
log = logging.getLogger("notaire")

BASE = "https://statuts.notaire.be/stapor_v1"
COOKIE_FILE = Path("notaire_cookies.json")
TMP_PDFS = Path("tmp/notaire")
TMP_PDFS.mkdir(parents=True, exist_ok=True)
PAGE_SIZE = 20

HEADERS_API = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "fr-BE,fr;q=0.9,en;q=0.8",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

SEED_BCE = "0836157420"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27019/")

PROXY_POOL = [
    "socks5://localhost:9050",
    "socks5://localhost:9052",
    "socks5://localhost:9054",
    "socks5://localhost:9056",
    "socks5://localhost:9058",
    "socks5://localhost:9060"
]
if "mongo" in MONGO_URI:
    PROXY_POOL = [
        "socks5://tor1:9050",
        "socks5://tor2:9050",
        "socks5://tor3:9050",
        "socks5://tor4:9050",
        "socks5://tor5:9050",
        "socks5://tor6:9050"
    ]

current_proxy_idx = 0

def get_current_proxy():
    return PROXY_POOL[current_proxy_idx]

def rotate_proxy():
    global current_proxy_idx
    old_proxy = PROXY_POOL[current_proxy_idx]
    current_proxy_idx = (current_proxy_idx + 1) % len(PROXY_POOL)
    log.info(f"Rotating proxy: {old_proxy} -> {PROXY_POOL[current_proxy_idx]}")

def _fetch_cookies_via_playwright() -> list[dict]:
    seed_url = (
        f"{BASE}/enterprise/{SEED_BCE}/statutes"
        f"?enterpriseNumber={SEED_BCE}&statuteStart=0&statuteCount=5"
    )
    log.info("Opening Playwright Chrome to renew F5 cookies...")
    
    proxy_opt = None
    proxy_server = get_current_proxy()
    if proxy_server:
        proxy_opt = {"server": proxy_server}

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(channel="chrome", headless=True, proxy=proxy_opt)
        except Exception:
            log.warning("System Chrome not found — falling back to Chromium")
            browser = p.chromium.launch(headless=True, proxy=proxy_opt)

        ctx = browser.new_context(
            locale="fr-BE",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        )
        page = ctx.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        page.goto("https://statuts.notaire.be/", wait_until="load", timeout=20000)
        page.wait_for_timeout(2000)
        page.goto(seed_url, wait_until="load", timeout=30000)

        for i in range(40):
            names = {c["name"] for c in ctx.cookies()}
            if "OClmoOot" in names and "Lyp1CWKh" in names:
                log.info(f"  Cookies OK ({i * 500}ms)")
                break
            page.wait_for_timeout(500)
        else:
            log.warning(f"  Timeout — cookies present: {[c['name'] for c in ctx.cookies()]}")

        cookies = ctx.cookies()
        browser.close()

    return cookies

def _build_session(cookies: list[dict]) -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS_API)
    proxy_server = get_current_proxy().replace("socks5://", "socks5h://")
    session.proxies = {"http": proxy_server, "https": proxy_server}
    
    for c in cookies:
        session.cookies.set(c["name"], c["value"], domain=c["domain"])
    return session

def _session_valid(session: requests.Session) -> bool:
    try:
        r = session.get(
            f"{BASE}/api/enterprises/{SEED_BCE}/statutes",
            params={"offset": 0, "limit": 1},
            timeout=10,
        )
        return "application/json" in r.headers.get("content-type", "")
    except Exception:
        return False

def get_session() -> requests.Session:
    if COOKIE_FILE.exists():
        try:
            cookies = json.loads(COOKIE_FILE.read_text())
            session = _build_session(cookies)
            if _session_valid(session):
                log.info("Session OK (cookies cached)")
                return session
            log.info("Cookies expired — renewing...")
        except Exception:
            pass

    cookies = _fetch_cookies_via_playwright()
    COOKIE_FILE.write_text(json.dumps(cookies, indent=2))
    log.info(f"Cookies saved → {COOKIE_FILE}")
    return _build_session(cookies)

def get_statutes(session: requests.Session, enterprise_number: str) -> list[dict]:
    url = f"{BASE}/api/enterprises/{enterprise_number}/statutes"
    session.headers["Referer"] = (
        f"{BASE}/enterprise/{enterprise_number}/statutes"
        f"?enterpriseNumber={enterprise_number}&statuteStart=0&statuteCount=5"
    )
    all_statutes, offset = [], 0

    while True:
        r = session.get(url, params={"deedDate": "", "offset": offset, "limit": PAGE_SIZE}, timeout=15)
        r.raise_for_status()

        if "application/json" not in r.headers.get("content-type", ""):
            log.error(f"[{enterprise_number}] Non-JSON response — session expired mid-run")
            break

        data = r.json()
        batch = data.get("statutes", [])
        total = data.get("totalItems", 0)
        all_statutes.extend(batch)
        log.info(f"  [{enterprise_number}] offset={offset} — {len(batch)} statutes (total: {total})")

        if not batch or len(all_statutes) >= total:
            break
        offset += PAGE_SIZE
        time.sleep(0.3)

    return [s for s in all_statutes if s.get("documentStatus") == "DONE"]

def download_statute_pdf(session: requests.Session, enterprise_number: str, statute: dict, dest_dir: Path) -> Path | None:
    doc_id = statute["documentId"]
    deed_date = statute.get("deedDate", "unknown").replace("-", "")
    dest = dest_dir / f"{enterprise_number}_{deed_date}_{doc_id}.pdf"

    if dest.exists():
        log.info(f"    Already downloaded: {dest.name}")
        return dest

    r = session.get(
        f"{BASE}/api/enterprises/{enterprise_number}/statutes/non-certified/{doc_id}",
        timeout=30,
    )
    if r.status_code == 404:
        return None
    r.raise_for_status()
    if "pdf" not in r.headers.get("content-type", "") and len(r.content) < 1000:
        return None

    dest.write_bytes(r.content)
    log.info(f"    Saved: {dest.name} ({len(r.content) // 1024} KB)")
    return dest

def main():
    if len(sys.argv) < 2:
        print(json.dumps({"event": "error", "message": "Enterprise number argument missing."}))
        sys.exit(1)

    enterprise_number = sys.argv[1]
    cleaned_bce = str(enterprise_number).replace(".", "").replace(" ", "").strip()
    
    print(json.dumps({"event": "start", "enterprise": enterprise_number, "cleaned": cleaned_bce}), flush=True)
    
    session = None
    try:
        print(json.dumps({"event": "session_validating"}), flush=True)
        session = get_session()
        
        print(json.dumps({"event": "statutes_fetching"}), flush=True)
        statutes = get_statutes(session, cleaned_bce)
        
        print(json.dumps({"event": "statutes_count", "count": len(statutes)}), flush=True)
        
        success_count = 0
        for s in statutes:
            doc_id = s.get("documentId")
            deed_date = s.get("deedDate", "unknown")
            description = s.get("deedNatureDecoded", "")
            
            print(json.dumps({
                "event": "downloading",
                "documentId": doc_id,
                "deedDate": deed_date,
                "description": description
            }), flush=True)
            
            pdf_path = download_statute_pdf(session, cleaned_bce, s, TMP_PDFS)
            if pdf_path:
                success_count += 1
                print(json.dumps({
                    "event": "downloaded",
                    "documentId": doc_id,
                    "deedDate": deed_date,
                    "filename": pdf_path.name,
                    "sizeBytes": pdf_path.stat().st_size
                }), flush=True)
            else:
                print(json.dumps({
                    "event": "download_failed",
                    "documentId": doc_id,
                    "deedDate": deed_date
                }), flush=True)
            
            time.sleep(0.5)
            
        print(json.dumps({"event": "done", "total": len(statutes), "downloaded": success_count}), flush=True)
        
    except Exception as e:
        log.exception("Fatal error during scraping")
        print(json.dumps({"event": "error", "message": str(e)}), flush=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
