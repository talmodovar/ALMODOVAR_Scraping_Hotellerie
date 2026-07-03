import os
import sys
import time
import requests
import pandas as pd
from io import StringIO
from pymongo import MongoClient

# Base CBSO NBB API URL
BASE = "https://consult.cbso.nbb.be/api"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "fr-BE,fr;q=0.9,en;q=0.8",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

# MongoDB configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27019/")
MONGO_DB = os.getenv("MONGO_DB", "bce_db")
MONGO_STATE_URI = os.getenv("MONGO_STATE_URI", "mongodb://localhost:27018/")
MONGO_STATE_DB = os.getenv("MONGO_STATE_DB", "bce_state_db")

# HDFS configuration
HDFS_URL = os.getenv("HDFS_URL", "http://localhost:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")

# Proxy Pool SOCKS5 internally inside Docker Network (with fallback to localhost/no proxy if running locally outside Docker)
PROXY_POOL = [
    "socks5h://localhost:9050",
    "socks5h://localhost:9052",
    "socks5h://localhost:9054",
    "socks5h://localhost:9056",
    "socks5h://localhost:9058",
    "socks5h://localhost:9060"
]
if "mongo" in MONGO_URI:
    PROXY_POOL = [
        "socks5h://tor1:9050",
        "socks5h://tor2:9050",
        "socks5h://tor3:9050",
        "socks5h://tor4:9050",
        "socks5h://tor5:9050",
        "socks5h://tor6:9050"
    ]

current_proxy_idx = 0

def get_proxied_session():
    global current_proxy_idx
    session = requests.Session()
    session.headers.update(HEADERS)
    proxy = PROXY_POOL[current_proxy_idx]
    session.proxies = {"http": proxy, "https": proxy}
    return session

def rotate_proxy():
    global current_proxy_idx
    old_proxy = PROXY_POOL[current_proxy_idx]
    current_proxy_idx = (current_proxy_idx + 1) % len(PROXY_POOL)
    print(f"Rotating proxy: {old_proxy} -> {PROXY_POOL[current_proxy_idx]}")

def upload_to_hdfs(file_path, content_bytes):
    url = f"{HDFS_URL}/webhdfs/v1{file_path}?op=CREATE&user.name={HDFS_USER}&overwrite=true"
    try:
        r = requests.put(url, allow_redirects=False, timeout=10)
        if r.status_code == 307:
            redirect_url = r.headers["Location"]
            r2 = requests.put(redirect_url, data=content_bytes, timeout=30)
            r2.raise_for_status()
            print(f"    Uploaded to HDFS: {file_path}")
            return True
        else:
            print(f"    HDFS CREATE failed: {r.status_code} - {r.text}")
    except Exception as e:
        print(f"    HDFS upload exception: {e}")
    return False

def run_nbb_scraper():
    print("Connecting to MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    
    state_client = MongoClient(MONGO_STATE_URI)
    state_db = state_client[MONGO_STATE_DB]
    state_col = state_db["download_state"]
    
    # Target selection criteria
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
    
    print("Filtering hotellerie targets in enterprise_finale...")
    cursor = db["enterprise_finale"].find(query, {"EnterpriseNumber": 1})
    targets = [doc["EnterpriseNumber"] for doc in cursor if "EnterpriseNumber" in doc]
    print(f"Found {len(targets)} targets.")
    
    # Initialize StateDB
    inserted = 0
    for ent_num in targets:
        if not state_col.find_one({"_id": ent_num}):
            state_col.insert_one({
                "_id": ent_num,
                "status": "pending",
                "filings_count": 0,
                "filings_done": [],
                "filings_migrated": []
            })
            inserted += 1
    if inserted > 0:
        print(f"Populated StateDB with {inserted} new targets.")
        
    print("Starting NBB scraper loop...")
    while True:
        target = state_col.find_one_and_update(
            {"status": "pending"},
            {"$set": {"status": "in_progress"}},
            new=True
        )
        if not target:
            print("No more pending targets in StateDB.")
            break
            
        enterprise_number = target["_id"]
        cleaned_bce = str(enterprise_number).replace(".", "").replace(" ", "").strip()
        print(f"\nScraping enterprise: {enterprise_number} (cleaned: {cleaned_bce})")
        
        session = get_proxied_session()
        
        try:
            # Establish session page
            page_url = f"https://consult.cbso.nbb.be/consult-enterprise/{cleaned_bce}"
            session.headers.update({"Referer": page_url})
            session.get(page_url, timeout=10)
            
            # Fetch deposits
            api_url = (
                f"{BASE}/rs-consult/published-deposits"
                f"?page=0&size=20&enterpriseNumber={cleaned_bce}"
                f"&sort=periodEndDate,desc&sort=depositDate,desc"
            )
            r = session.get(api_url, timeout=10)
            
            if r.status_code == 429:
                print("Received HTTP 429. Rotating proxy and resetting status.")
                state_col.update_one({"_id": enterprise_number}, {"$set": {"status": "pending"}})
                rotate_proxy()
                time.sleep(2)
                continue
                
            r.raise_for_status()
            deposits = r.json().get("content", [])
            
            filings_done = target.get("filings_done", [])
            filings_migrated = target.get("filings_migrated", [])
            success_count = 0
            
            for filing in deposits:
                ref = filing.get("reference")
                year = filing.get("periodEndDateYear")
                deposit_id = filing.get("id")
                migrated = filing.get("migration", False)
                end_date = filing.get("periodEndDate", "")
                
                if not end_date or end_date < "2021-01-01":
                    continue
                if ref in filings_done or ref in filings_migrated:
                    continue
                if migrated:
                    print(f"  Filing {ref} ({year}) is migrated (legacy). Skipping CSV.")
                    filings_migrated.append(ref)
                    continue
                    
                # Download CSV
                csv_url = f"{BASE}/external/broker/public/deposits/consult/csv/{deposit_id}"
                csv_res = session.get(csv_url, timeout=15)
                
                if csv_res.status_code == 429:
                    raise requests.exceptions.RequestException("HTTP 429 inside loop")
                csv_res.raise_for_status()
                
                # Upload to HDFS
                hdfs_path = f"/{cleaned_bce}/hbb/{ref}.csv"
                if upload_to_hdfs(hdfs_path, csv_res.content):
                    filings_done.append(ref)
                    success_count += 1
                time.sleep(0.5)
                
            # --- POST-VALIDATION STEP ---
            # Verify that every reference in filings_done actually exists in HDFS
            hdfs_path_check = f"/{cleaned_bce}/hbb"
            hdfs_res = requests.get(f"{HDFS_URL}/webhdfs/v1{hdfs_path_check}?op=LISTSTATUS", timeout=10)
            hdfs_files = []
            if hdfs_res.status_code == 200:
                files_list = hdfs_res.json().get("FileStatuses", {}).get("FileStatus", [])
                hdfs_files = [f["pathSuffix"] for f in files_list]
            elif hdfs_res.status_code == 404 and len(filings_done) == 0:
                # 404 is fine if we actually expect 0 files
                pass
            else:
                raise Exception(f"Failed to verify HDFS files for {enterprise_number} (status: {hdfs_res.status_code})")
                
            # All non-migrated filings must have their CSV in HDFS
            missing_files = []
            for ref in filings_done:
                expected_filename = f"{ref}.csv"
                if expected_filename not in hdfs_files:
                    missing_files.append(expected_filename)
            
            if missing_files:
                raise Exception(f"Validation failed: missing CSV files in HDFS for {enterprise_number}: {missing_files}")

            state_col.update_one(
                {"_id": enterprise_number},
                {
                    "$set": {
                        "status": "done",
                        "filings_count": len(filings_done),
                        "filings_done": filings_done,
                        "filings_migrated": filings_migrated
                    }
                }
            )
            print(f"Finished {enterprise_number}. {success_count} filings uploaded.")
            
        except (requests.exceptions.RequestException, Exception) as e:
            print(f"Error scraping {enterprise_number}: {e}")
            state_col.update_one({"_id": enterprise_number}, {"$set": {"status": "pending"}})
            rotate_proxy()
            time.sleep(2)

if __name__ == "__main__":
    run_nbb_scraper()
