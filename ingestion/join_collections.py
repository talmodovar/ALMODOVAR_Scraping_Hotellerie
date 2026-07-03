import os
import sys
import time
from pymongo import MongoClient, ASCENDING

# MongoDB configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
MONGO_DB = os.getenv("MONGO_DB", "bce_db")

print(f"Connecting to MongoDB at {MONGO_URI}...")
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

# 1. Create Indexes on EntityNumber / EnterpriseNumber
print("\n--- 1. Creating Indexes ---")
index_definitions = {
    "enterprise": "EnterpriseNumber",
    "activity": "EntityNumber",
    "address": "EntityNumber",
    "branch": "EnterpriseNumber",
    "contact": "EntityNumber",
    "denomination": "EntityNumber",
    "establishment": "EnterpriseNumber"
}

for col_name, key in index_definitions.items():
    if col_name in db.list_collection_names():
        print(f"Creating index on '{key}' in collection '{col_name}'...")
        start = time.time()
        db[col_name].create_index([(key, ASCENDING)])
        print(f"  Index created in {time.time() - start:.2f} seconds.")
    else:
        print(f"Warning: Collection '{col_name}' not found, skipping index.")

# 2. Join Collections into enterprise_finale
print("\n--- 2. Joining Collections ---")
target_col_name = "enterprise_finale"
print(f"Dropping existing collection '{target_col_name}'...")
db[target_col_name].drop()
target_collection = db[target_col_name]

# Batch configuration
batch_size = 5000
total_enterprises = db["enterprise"].count_documents({})
print(f"Total enterprises to process: {total_enterprises}")

# Setup helper for bulk lookup
def fetch_related(col_name, key_field, ent_numbers):
    res = {}
    if col_name in db.list_collection_names():
        cursor = db[col_name].find({key_field: {"$in": ent_numbers}})
        for doc in cursor:
            # remove _id from subdocuments to avoid duplicates/errors
            doc.pop("_id", None)
            ent_num = doc.get(key_field)
            if ent_num not in res:
                res[ent_num] = []
            res[ent_num].append(doc)
    return res

start_join_time = time.time()
processed_count = 0

# Cursor to iterate over all enterprises
enterprise_cursor = db["enterprise"].find({}, batch_size=batch_size)
batch_docs = []

for ent in enterprise_cursor:
    batch_docs.append(ent)
    
    if len(batch_docs) >= batch_size:
        # Process batch
        ent_numbers = [doc["EnterpriseNumber"] for doc in batch_docs]
        
        # Fetch related records from other collections in bulk
        activities = fetch_related("activity", "EntityNumber", ent_numbers)
        addresses = fetch_related("address", "EntityNumber", ent_numbers)
        branches = fetch_related("branch", "EnterpriseNumber", ent_numbers)
        contacts = fetch_related("contact", "EntityNumber", ent_numbers)
        denominations = fetch_related("denomination", "EntityNumber", ent_numbers)
        establishments = fetch_related("establishment", "EnterpriseNumber", ent_numbers)
        
        # Merge in memory
        joined_batch = []
        for ent_doc in batch_docs:
            ent_num = ent_doc["EnterpriseNumber"]
            
            # Create a clean copy of the enterprise document
            joined_doc = dict(ent_doc)
            joined_doc.pop("_id", None)  # Remove original _id to let Mongo generate a new one
            
            # Attach related fields (empty list if none found)
            joined_doc["activities"] = activities.get(ent_num, [])
            joined_doc["addresses"] = addresses.get(ent_num, [])
            joined_doc["branches"] = branches.get(ent_num, [])
            joined_doc["contacts"] = contacts.get(ent_num, [])
            joined_doc["denominations"] = denominations.get(ent_num, [])
            joined_doc["establishments"] = establishments.get(ent_num, [])
            
            joined_batch.append(joined_doc)
            
        # Write to target collection
        if joined_batch:
            target_collection.insert_many(joined_batch, ordered=False)
            
        processed_count += len(batch_docs)
        elapsed = time.time() - start_join_time
        rate = processed_count / elapsed if elapsed > 0 else 0
        print(f"Processed {processed_count}/{total_enterprises} enterprises ({processed_count/total_enterprises*100:.2f}%). Rate: {rate:.1f} ent/s.")
        
        batch_docs = []

# Process any remaining documents in the final batch
if batch_docs:
    ent_numbers = [doc["EnterpriseNumber"] for doc in batch_docs]
    activities = fetch_related("activity", "EntityNumber", ent_numbers)
    addresses = fetch_related("address", "EntityNumber", ent_numbers)
    branches = fetch_related("branch", "EnterpriseNumber", ent_numbers)
    contacts = fetch_related("contact", "EntityNumber", ent_numbers)
    denominations = fetch_related("denomination", "EntityNumber", ent_numbers)
    establishments = fetch_related("establishment", "EnterpriseNumber", ent_numbers)
    
    joined_batch = []
    for ent_doc in batch_docs:
        ent_num = ent_doc["EnterpriseNumber"]
        joined_doc = dict(ent_doc)
        joined_doc.pop("_id", None)
        
        joined_doc["activities"] = activities.get(ent_num, [])
        joined_doc["addresses"] = addresses.get(ent_num, [])
        joined_doc["branches"] = branches.get(ent_num, [])
        joined_doc["contacts"] = contacts.get(ent_num, [])
        joined_doc["denominations"] = denominations.get(ent_num, [])
        joined_doc["establishments"] = establishments.get(ent_num, [])
        
        joined_batch.append(joined_doc)
        
    if joined_batch:
        target_collection.insert_many(joined_batch, ordered=False)
    processed_count += len(batch_docs)

total_elapsed = time.time() - start_join_time
print(f"\nJoin complete! Inserted {processed_count} documents into '{target_col_name}' in {total_elapsed:.2f} seconds.")
