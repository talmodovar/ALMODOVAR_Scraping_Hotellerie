import os
import sys
import time
from pathlib import Path
import pandas as pd
from pymongo import MongoClient

# MongoDB configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
MONGO_DB = os.getenv("MONGO_DB", "bce_db")
DATA_DIR = Path(os.getenv("KBO_DATA_DIR", "/opt/airflow/données"))

print(f"Connecting to MongoDB at {MONGO_URI}...")
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

print(f"Reading CSV files from {DATA_DIR}...")
if not DATA_DIR.exists():
    print(f"Error: Data directory {DATA_DIR} does not exist!")
    sys.exit(1)

# List of CSV files to import
csv_files = sorted(list(DATA_DIR.glob("*.csv")))

for csv_path in csv_files:
    collection_name = csv_path.stem
    print(f"\nProcessing file: {csv_path.name} -> Collection: {collection_name}")
    
    # Skip if collection already exists and has data to avoid re-importing huge files
    if collection_name in db.list_collection_names() and db[collection_name].count_documents({}) > 0:
        print(f"Collection '{collection_name}' already populated. Skipping.")
        continue

    # Drop existing collection for clean import
    print(f"Dropping collection '{collection_name}' if exists...")
    db[collection_name].drop()
    
    collection = db[collection_name]
    
    # Read in chunks of 50,000 rows
    chunk_size = 50000
    start_time = time.time()
    total_rows = 0
    
    try:
        # Use pandas chunking to prevent memory issues with large CSV files (e.g. activity.csv)
        for chunk in pd.read_csv(csv_path, chunksize=chunk_size, low_memory=False):
            # Replace NaN/None values with empty string or null values for MongoDB
            chunk = chunk.where(pd.notnull(chunk), None)
            
            # Convert to dictionary list
            records = chunk.to_dict(orient="records")
            
            # Insert into MongoDB
            if records:
                collection.insert_many(records, ordered=False)
                total_rows += len(records)
                print(f"  Inserted {total_rows} rows so far...")
                
        elapsed = time.time() - start_time
        print(f"Finished importing {collection_name}: {total_rows} rows in {elapsed:.2f} seconds.")
    except Exception as e:
        print(f"Error importing {csv_path.name}: {e}")

print("\nAll CSV files imported successfully!")
