import os
import sys
import time
from pymongo import MongoClient
import pandas as pd

# MongoDB configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
MONGO_DB = os.getenv("MONGO_DB", "bce_db")
DATA_DIR = os.getenv("KBO_DATA_DIR", "/opt/airflow/données")

print(f"Connecting to MongoDB at {MONGO_URI}...")
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

# Load code mappings from MongoDB 'code' collection
print("Loading code dictionary for FR translations...")
code_dict = {}
try:
    code_cursor = db["code"].find({"Language": "FR"})
    for doc in code_cursor:
        cat = doc.get("Category")
        code = str(doc.get("Code")).strip()
        desc = doc.get("Description")
        if cat and code:
            if cat not in code_dict:
                code_dict[cat] = {}
            code_dict[cat][code] = desc
    print("Code dictionary loaded successfully.")
except Exception as e:
    print(f"Warning: Could not load code translations from DB: {e}")

# Target collection setup
source_col = "enterprise_finale"
target_col = "enterprise_silver"
print(f"Dropping target collection '{target_col}' if exists...")
db[target_col].drop()
target_collection = db[target_col]

# Date conversion helper
def convert_date(date_str):
    if not date_str or not isinstance(date_str, str):
        return date_str
    try:
        parts = date_str.split("-")
        if len(parts) == 3:
            # DD-MM-YYYY -> YYYY-MM-DD
            return f"{parts[2]}-{parts[1]}-{parts[0]}"
    except Exception:
        pass
    return date_str

# Process batch-by-batch
batch_size = 5000
total_docs = db[source_col].count_documents({})
print(f"Transforming {total_docs} documents from '{source_col}' to '{target_col}'...")

cursor = db[source_col].find({}, batch_size=batch_size)
batch_docs = []
start_time = time.time()
processed = 0

for doc in cursor:
    # 1. Date normalization (Main StartDate, and nested branches/establishments)
    if "StartDate" in doc:
        doc["StartDate"] = convert_date(doc["StartDate"])
    
    if "branches" in doc:
        for branch in doc["branches"]:
            if "StartDate" in branch:
                branch["StartDate"] = convert_date(branch["StartDate"])
                
    if "establishments" in doc:
        for est in doc["establishments"]:
            if "StartDate" in est:
                est["StartDate"] = convert_date(est["StartDate"])

    # 2. Activity deduplication (NaceCode + Classification)
    if "activities" in doc and isinstance(doc["activities"], list):
        seen = set()
        dedup_activities = []
        for act in doc["activities"]:
            nace = str(act.get("NaceCode", "")).strip()
            cls_type = str(act.get("Classification", "")).strip()
            key = (nace, cls_type)
            if key not in seen:
                seen.add(key)
                
                # 5. Decode activities code -> label
                nace_version = str(act.get("NaceVersion", ""))
                cat_key = f"Nace{nace_version}" if nace_version in ["2003", "2008", "2025"] else "Nace2008"
                
                # Fetch Nace Label
                label = code_dict.get(cat_key, {}).get(nace)
                if not label:
                    # Fallback lookup in other NACE categories if version is not clear
                    for fallback_cat in ["Nace2008", "Nace2025", "Nace2003"]:
                        label = code_dict.get(fallback_cat, {}).get(nace)
                        if label:
                            break
                            
                act["NaceLabel"] = label or ""
                dedup_activities.append(act)
        doc["activities"] = dedup_activities

    # 3. Address Unique (Keep TypeOfAddress = REGO only)
    if "addresses" in doc and isinstance(doc["addresses"], list):
        rego_addresses = [addr for addr in doc["addresses"] if str(addr.get("TypeOfAddress")).strip() == "REGO"]
        doc["addresses"] = rego_addresses

    # 4. Denomination Principal (TypeOfDenomination = 1 nom officiel comes first)
    if "denominations" in doc and isinstance(doc["denominations"], list):
        # Sort denominations: TypeOfDenomination == 1 (or "1") first
        def get_denom_sort_key(d):
            val = str(d.get("TypeOfDenomination", "")).strip()
            return 0 if val == "1" else 1
            
        doc["denominations"] = sorted(doc["denominations"], key=get_denom_sort_key)

    # 5. Decode JuridicalForm and Status codes -> labels
    jur_code = str(doc.get("JuridicalForm", "")).strip()
    if jur_code:
        doc["JuridicalFormLabel"] = code_dict.get("JuridicalForm", {}).get(jur_code, "")
        
    status_code = str(doc.get("Status", "")).strip()
    if status_code:
        doc["StatusLabel"] = code_dict.get("Status", {}).get(status_code, "")

    batch_docs.append(doc)

    if len(batch_docs) >= batch_size:
        target_collection.insert_many(batch_docs, ordered=False)
        processed += len(batch_docs)
        elapsed = time.time() - start_time
        rate = processed / elapsed if elapsed > 0 else 0
        print(f"Processed {processed}/{total_docs} ({processed/total_docs*100:.2f}%). Rate: {rate:.1f} doc/s.")
        batch_docs = []

# Process remaining
if batch_docs:
    target_collection.insert_many(batch_docs, ordered=False)
    processed += len(batch_docs)

total_elapsed = time.time() - start_time
print(f"\nSilver transformation complete! Created '{target_col}' with {processed} documents in {total_elapsed:.2f} seconds.")

# Create index on EnterpriseNumber for fast retrieval in Silver layer
print(f"Creating index on 'EnterpriseNumber' in '{target_col}'...")
target_collection.create_index("EnterpriseNumber")
print("Index created successfully.")
