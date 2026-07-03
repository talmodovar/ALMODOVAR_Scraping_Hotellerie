import os
import sys
import json
import logging
import math
import asyncio
import subprocess
from datetime import datetime
from typing import List, Optional
import requests
import urllib3
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("backend")

app = FastAPI(
    title="FastAPI BCE Backend",
    description="Exposes Gold and Silver data and orchestrates real-time notary statutes streaming.",
    version="1.0.0"
)

# Enable CORS for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount notary PDFs directory
Path("tmp/notaire").mkdir(parents=True, exist_ok=True)
app.mount("/tmp/notaire", StaticFiles(directory="tmp/notaire"), name="notaire")

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27019/")
MONGO_DB = os.getenv("MONGO_DB", "bce_db")
MONGO_STATE_URI = os.getenv("MONGO_STATE_URI", "mongodb://localhost:27018/")
MONGO_STATE_DB = os.getenv("MONGO_STATE_DB", "bce_state_db")

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = client[MONGO_DB]

try:
    state_client = MongoClient(MONGO_STATE_URI, serverSelectionTimeoutMS=3000)
    state_db = state_client[MONGO_STATE_DB]
    # Force ping to detect connection failure early
    state_client.admin.command('ping')
    logger.info("Connected to state MongoDB.")
except Exception as e:
    logger.warning(f"Could not connect to state MongoDB ({MONGO_STATE_URI}): {e}. Statutes streaming may be unavailable.")
    state_client = None
    state_db = None

# Proxy Pool SOCKS5 internally inside Docker Network vs Localhost
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

# Helper to normalize BCE number format (e.g. 0836157420 -> 0836.157.420)
def normalize_bce(q: str) -> str:
    digits = "".join(c for c in q if c.isdigit())
    if len(digits) == 9:
        digits = "0" + digits
    if len(digits) == 10:
        return f"{digits[0:4]}.{digits[4:7]}.{digits[7:10]}"
    return q

# Helper to recursively clean up non-JSON-compliant NaN/Inf float values in MongoDB documents
def clean_nan_values(obj):
    if isinstance(obj, dict):
        return {k: clean_nan_values(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan_values(x) for x in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    return obj

# Helper to crawl directors from kbopub
def scrape_directors_from_kbopub(cleaned_bce: str) -> list:
    url = f"https://kbopub.economie.fgov.be/kbopub/zoeknummerform.html?nummer={cleaned_bce}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    # Bypass requests CA cert bundles if overridden on dev machine
    os.environ.pop("REQUESTS_CA_BUNDLE", None)
    os.environ.pop("CURL_CA_BUNDLE", None)
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    directors = []
    
    # Try different proxies from the pool in sequence
    for proxy in PROXY_POOL:
        logger.info(f"Attempting to crawl kbopub for {cleaned_bce} using proxy {proxy}...")
        try:
            proxies = {"http": proxy, "https": proxy}
            r = requests.get(url, headers=headers, proxies=proxies, timeout=12, verify=False)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, "html.parser")
                table = soup.find("table")
                if not table:
                    logger.warning(f"No table found on kbopub page for {cleaned_bce}")
                    return []
                
                in_functies = False
                for row in table.find_all("tr"):
                    h2 = row.find("h2")
                    if h2:
                        section = h2.get_text(strip=True)
                        if "Functies" in section:
                            in_functies = True
                            continue
                        elif in_functies:
                            break
                            
                    if in_functies:
                        tds = row.find_all("td")
                        if len(tds) >= 2:
                            classes = [c for td in tds for c in (td.get("class", []))]
                            if any(cls in ["QL", "RL"] for cls in classes):
                                role = tds[0].get_text(strip=True)
                                name = tds[1].get_text(strip=True)
                                name = " ".join(name.split())
                                
                                start_date = ""
                                if len(tds) >= 3:
                                    start_date = tds[2].get_text(strip=True)
                                    start_date = " ".join(start_date.split())
                                    
                                directors.append({
                                    "role": role,
                                    "name": name,
                                    "start_date": start_date
                                })
                logger.info(f"Successfully scraped {len(directors)} directors from kbopub.")
                return directors
            elif r.status_code == 404:
                logger.warning(f"Enterprise {cleaned_bce} not found on kbopub (404)")
                return []
            else:
                logger.warning(f"Failed to crawl kbopub (status: {r.status_code}) using proxy {proxy}")
        except Exception as e:
            logger.error(f"Error crawling kbopub with proxy {proxy}: {e}")
            
    logger.error("All proxies failed to crawl kbopub. Returning empty list.")
    return []



@app.on_event("startup")
def create_indexes():
    """Ensure required MongoDB indexes exist for fast search."""
    try:
        col = db["enterprise_silver"]
        existing = col.index_information()
        
        # Text index for name search (denominations.Denomination)
        if not any("text" in str(v.get("key")) for v in existing.values()):
            col.create_index([("denominations.Denomination", "text")], name="denomination_text")
            logger.info("Created text index on denomination.")
        
        # Index on NaceCode for activity filter
        if "activities.NaceCode_1" not in existing:
            col.create_index([("activities.NaceCode", 1)], name="activities.NaceCode_1")
            logger.info("Created index on activities.NaceCode.")

        # Index on EnterpriseNumber for fast lookup
        if "EnterpriseNumber_1" not in existing:
            col.create_index([("EnterpriseNumber", 1)], name="EnterpriseNumber_1")
            logger.info("Created index on EnterpriseNumber.")
            
        logger.info("MongoDB indexes verified.")
    except Exception as e:
        logger.warning(f"Could not create indexes: {e}")


@app.get("/")
def read_root():
    return {"status": "ok", "service": "BCE FastAPI Backend"}


@app.get("/api/enterprises/search")
def search_enterprises(q: str = Query(..., min_length=2, description="Name or BCE number of the enterprise")):
    q = q.strip()
                
    # 1. Check if the query looks like a BCE number
    nace_hotels = ["55100", "55201", "55202", "55203", "55204", "55209", "55300", "55400", "55900"]
    nace_hotels_int = [int(x) for x in nace_hotels]
    
    digits = "".join(c for c in q if c.isdigit())
    if len(digits) >= 5:
        # Normalize and construct matching filter for BCE number
        normalized = normalize_bce(digits)
        query_filter = {
            "$and": [
                {"EnterpriseNumber": {"$regex": f".*{normalized}.*"}},
                {"activities.NaceCode": {"$in": nace_hotels + nace_hotels_int}}
            ]
        }
    else:
        # 2. Text name search
        query_filter = {
            "$and": [
                {"$text": {"$search": q}},
                {"activities.NaceCode": {"$in": nace_hotels + nace_hotels_int}}
            ]
        }

    projection = {
        "_id": 0,
        "EnterpriseNumber": 1,
        "StatusLabel": 1,
        "JuridicalFormLabel": 1,
        "denominations.Denomination": 1,
        "addresses.Zipcode": 1,
        "addresses.MunicipalityFR": 1,
        "activities.NaceCode": 1,
        "activities.NaceLabel": 1
    }

    try:
        results = list(db["enterprise_silver"].find(query_filter, projection).limit(50))
        
        # If text search yielded nothing and query wasn't a number, fallback to regex substring match
        if not results and len(digits) < 5:
            regex_filter = {
                "$and": [
                    {"denominations.Denomination": {"$regex": q, "$options": "i"}},
                    {"activities.NaceCode": {"$in": nace_hotels + nace_hotels_int}}
                ]
            }
            results = list(db["enterprise_silver"].find(regex_filter, projection).limit(50))
            
        return [clean_nan_values(r) for r in results]
    except Exception as e:
        logger.error(f"Search failed: {e}")
        # Try fallback immediately if text search fails due to missing index
        if len(digits) < 5:
            regex_filter = {
                "$and": [
                    {"denominations.Denomination": {"$regex": q, "$options": "i"}},
                    {"activities.NaceCode": {"$in": nace_hotels + nace_hotels_int}}
                ]
            }
            results = list(db["enterprise_silver"].find(regex_filter, projection).limit(50))
            return [clean_nan_values(r) for r in results]
            
        raise HTTPException(status_code=500, detail="Database search operation failed")


@app.get("/api/enterprises/{enterprise_number}")
def get_enterprise_details(enterprise_number: str):
    formatted_bce = normalize_bce(enterprise_number)
    
    silver_doc = db["enterprise_silver"].find_one({"EnterpriseNumber": formatted_bce})
    if not silver_doc:
        # Fallback to checking without dots
        cleaned = formatted_bce.replace(".", "")
        # Try finding using exact cleaned format or dots format
        silver_doc = db["enterprise_silver"].find_one({"EnterpriseNumber": formatted_bce})
        if not silver_doc:
            raise HTTPException(status_code=404, detail=f"Enterprise {formatted_bce} not found in Silver database")

    silver_doc["_id"] = str(silver_doc["_id"])
    
    # Retrieve Gold ratios consolidated by Spark
    gold_doc = db["hotel_gold"].find_one({"enterprise_number": formatted_bce})
    if not gold_doc:
        # Try checking with cleaned bce format in case spark wrote without dots
        gold_doc = db["hotel_gold"].find_one({"enterprise_number": formatted_bce.replace(".", "")})
        
    if gold_doc:
        silver_doc["gold_data"] = {
            "last_updated": gold_doc.get("last_updated"),
            "years": gold_doc.get("years", [])
        }
    else:
        silver_doc["gold_data"] = None

    return clean_nan_values(silver_doc)


@app.get("/api/enterprises/{enterprise_number}/directors")
def get_enterprise_directors(enterprise_number: str):
    formatted_bce = normalize_bce(enterprise_number)
    cleaned_bce = formatted_bce.replace(".", "").replace(" ", "").strip()
    
    # 1. Try local MongoDB cache
    cached = db["directors"].find_one({"_id": formatted_bce})
    if cached:
        logger.info(f"Retrieving directors for {formatted_bce} from cache.")
        return cached.get("directors", [])
        
    # 2. Scrape on-demand
    logger.info(f"Directors for {formatted_bce} not in cache. Fetching from kbopub...")
    directors = scrape_directors_from_kbopub(cleaned_bce)
    
    # 3. Save to database even if empty to cache the result
    db["directors"].update_one(
        {"_id": formatted_bce},
        {
            "$set": {
                "directors": directors,
                "scraped_at": datetime.utcnow()
            }
        },
        upsert=True
    )
    
    return directors


@app.get("/api/enterprises/{enterprise_number}/statutes/stream")
def stream_enterprise_statutes(enterprise_number: str):
    from pathlib import Path
    formatted_bce = normalize_bce(enterprise_number)
    cleaned_bce = formatted_bce.replace(".", "").replace(" ", "").strip()

    async def event_generator():
        # Check cache in state database
        state_col = state_db["notaire_state"]
        cached = state_col.find_one({"_id": formatted_bce, "status": "done"})
        
        if cached:
            logger.info(f"Statutes for {formatted_bce} are cached as 'done'. Serving local files...")
            yield f"data: {json.dumps({'event': 'start', 'enterprise': enterprise_number, 'cleaned': cleaned_bce})}\n\n"
            yield f"data: {json.dumps({'event': 'session_validating'})}\n\n"
            yield f"data: {json.dumps({'event': 'statutes_fetching'})}\n\n"
            
            # List local PDF files in tmp/notaire
            pdf_dir = Path("tmp/notaire")
            files = list(pdf_dir.glob(f"{cleaned_bce}_*.pdf"))
            yield f"data: {json.dumps({'event': 'statutes_count', 'count': len(files)})}\n\n"
            
            for f in files:
                # Filename pattern: {cleaned_bce}_{deed_date}_{doc_id}.pdf
                parts = f.name.replace(".pdf", "").split("_")
                deed_date = parts[1] if len(parts) >= 2 else "unknown"
                doc_id = parts[2] if len(parts) >= 3 else "unknown"
                
                # yield downloading and downloaded events
                yield f"data: {json.dumps({'event': 'downloading', 'documentId': doc_id, 'deedDate': deed_date})}\n\n"
                await asyncio.sleep(0.1) # brief delay for visual effect
                yield f"data: {json.dumps({'event': 'downloaded', 'documentId': doc_id, 'deedDate': deed_date, 'filename': f.name, 'sizeBytes': f.stat().st_size})}\n\n"
                
            yield f"data: {json.dumps({'event': 'done', 'total': len(files), 'downloaded': len(files)})}\n\n"
            return

        # Otherwise, spawn process
        cmd = ["python", "ingestion/notaire.py", cleaned_bce]
        logger.info(f"Spawning subprocess: {' '.join(cmd)}")
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Read stdout line by line and yield SSE events
        while True:
            line = await process.stdout.readline()
            if not line:
                break
            
            decoded_line = line.decode("utf-8").strip()
            if decoded_line:
                yield f"data: {decoded_line}\n\n"
                
        # Read remaining exit codes/stderr
        stderr_data = await process.stderr.read()
        exit_code = await process.wait()
        
        if exit_code != 0:
            logger.error(f"notaire.py failed with code {exit_code}. Stderr: {stderr_data.decode('utf-8')}")
            # Yield error event
            yield f"data: {json.dumps({'event': 'error', 'message': f'Scraper exited with code {exit_code}'})}\n\n"
        else:
            logger.info("notaire.py finished successfully. Updating cache state...")
            state_col.update_one(
                {"_id": formatted_bce},
                {
                    "$set": {
                        "status": "done",
                        "scraped_at": datetime.utcnow()
                    }
                },
                upsert=True
            )

    return StreamingResponse(event_generator(), media_type="text/event-stream")
