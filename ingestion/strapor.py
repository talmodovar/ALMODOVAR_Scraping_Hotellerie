import os
import sys
import json
import logging
import time
from pathlib import Path
import requests
from playwright.sync_api import sync_playwright
from pymongo import MongoClient

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("strapor")

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

NO_NOTAIRE_FORMS = {"009", "017", "018", "025", "026", "027", "051", "052"}
SEED_BCE = "0836157420"

# MongoDB configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27019/")
MONGO_DB = os.getenv("MONGO_DB", "bce_db")
MONGO_STATE_URI = os.getenv("MONGO_STATE_URI", "mongodb://localhost:27018/")
MONGO_STATE_DB = os.getenv("MONGO_STATE_DB", "bce_state_db")

# Proxy Pool SOCKS5 internally inside Docker Network (with fallback to localhost/no proxy if running locally outside Docker)
PROXY_POOL = []
if "mongo:" in MONGO_URI:
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
    if not PROXY_POOL:
        return None
    return PROXY_POOL[current_proxy_idx]

def rotate_proxy():
    global current_proxy_idx
    if not PROXY_POOL:
        return
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
            # Try launching installed Chrome visible or headless depending on environment
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
    proxy_server = get_current_proxy()
    if proxy_server:
        proxy_server = proxy_server.replace("socks5://", "socks5h://") # Use socks5h for requests
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

def run_notaire_scraper():
    log.info("Connecting to MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    
    state_client = MongoClient(MONGO_STATE_URI)
    state_db = state_client[MONGO_STATE_DB]
    state_col = state_db["notaire_state"] # Keep notary status separate
    
    # 1. Target identification
    nace_hotels = ["55100", "55201", "55202", "55203", "55204", "55209", "55300", "55400", "55900"]
    nace_hotels_int = [int(x) for x in nace_hotels]
    
    excluded_jur_forms = [
        "110", "114", "116", "117", 110, 114, 116, 117,
        "301", "302", "303", 301, 302, 303,
        "310", "320", "330", "340", "350", 310, 320, 330, 340, 350,
        "400", "411", "412", "413", "414", "415", "416", "417", "418", "419", "420",
        400, 411, 412, 413, 414, 415, 416, 417, 418, 419, 420
    ]
    
    query = {
        "Status": "AC",
        "TypeOfEnterprise": {"$in": ["2", 2]},
        "JuridicalForm": {"$nin": excluded_jur_forms},
        "activities": {
            "$elemMatch": {
                "Classification": "MAIN",
                "NaceCode": {"$in": nace_hotels + nace_hotels_int}
            }
        }
    }
    
    log.info("Filtering hotellerie targets in enterprise_finale...")
    cursor = db["enterprise_finale"].find(query, {"EnterpriseNumber": 1, "JuridicalForm": 1})
    targets = []
    for doc in cursor:
        ent_num = doc.get("EnterpriseNumber")
        jur_form = str(doc.get("JuridicalForm", "")).strip()
        # Check needs_notaire_check condition: Active and not in NO_NOTAIRE_FORMS
        if ent_num and jur_form not in NO_NOTAIRE_FORMS:
            targets.append(ent_num)
            
    log.info(f"Found {len(targets)} targets requiring notary acts check.")
    
    # Initialize StateDB for Notaire
    inserted = 0
    for ent_num in targets:
        if not state_col.find_one({"_id": ent_num}):
            state_col.insert_one({
                "_id": ent_num,
                "status": "pending",
                "acts_count": 0,
                "acts_done": []
            })
            inserted += 1
    if inserted > 0:
        log.info(f"Populated notaire_state with {inserted} new targets.")
        
    session = get_session()
    
    log.info("Starting Notaire scraper loop...")
    while True:
        target = state_col.find_one_and_update(
            {"status": "pending"},
            {"$set": {"status": "in_progress"}},
            new=True
        )
        if not target:
            log.info("No more pending targets in StateDB.")
            break
            
        enterprise_number = target["_id"]
        cleaned_bce = str(enterprise_number).replace(".", "").replace(" ", "").strip()
        log.info(f"\nScraping notary acts for: {enterprise_number} (cleaned: {cleaned_bce})")
        
        try:
            # Check session validity
            if not _session_valid(session):
                log.info("Session expired. Renewing...")
                session = get_session()
                
            statutes = get_statutes(session, cleaned_bce)
            acts_done = target.get("acts_done", [])
            success_count = 0
            
            for s in statutes:
                doc_id = s["documentId"]
                if doc_id in acts_done:
                    continue
                    
                pdf_path = download_statute_pdf(session, cleaned_bce, s, TMP_PDFS)
                if pdf_path:
                    acts_done.append(doc_id)
                    success_count += 1
                time.sleep(0.5)
                
            state_col.update_one(
                {"_id": enterprise_number},
                {
                    "$set": {
                        "status": "done",
                        "acts_count": len(acts_done),
                        "acts_done": acts_done
                    }
                }
            )
            log.info(f"Finished {enterprise_number}. {success_count} acts downloaded.")
            
        except (requests.exceptions.RequestException, Exception) as e:
            log.error(f"Error scraping notary acts for {enterprise_number}: {e}")
            state_col.update_one({"_id": enterprise_number}, {"$set": {"status": "pending"}})
            rotate_proxy()
            session = get_session() # Re-build session with new proxy
            time.sleep(2)

if __name__ == "__main__":
    run_notaire_scraper()
