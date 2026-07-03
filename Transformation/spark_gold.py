import os
import sys
import subprocess
import time
from datetime import datetime

# Inline installation of pymongo for the Spark container environment
try:
    import pymongo
except ImportError:
    print("Installing pymongo library inside Spark environment...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pymongo"])
    import pymongo

from pymongo import MongoClient

# Configurations
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
MONGO_STATE_URI = os.getenv("MONGO_STATE_URI", "mongodb://mongo_state:27017/")
MONGO_DB = os.getenv("MONGO_DB", "bce_db")
MONGO_STATE_DB = os.getenv("MONGO_STATE_DB", "bce_state_db")
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")

def main():
    print(f"Connecting to State DB at {MONGO_STATE_URI}...")
    state_client = MongoClient(MONGO_STATE_URI)
    state_db = state_client[MONGO_STATE_DB]
    download_state_col = state_db["download_state"]
    gold_state_col = state_db["gold_state"]

    # 1. Fetch download states where status is "done"
    print("Fetching target enterprises from State DB...")
    download_states = list(download_state_col.find({"status": "done"}))
    print(f"Found {len(download_states)} completed scraping targets.")

    # 2. Fetch already processed states
    gold_states = {doc["_id"]: doc for doc in gold_state_col.find({})}

    # 3. Determine which enterprises need processing
    targets_to_process = []
    for ds in download_states:
        ent_num = ds["_id"]
        filings_done = ds.get("filings_done", [])
        
        # Skip if there are no filings done
        if not filings_done:
            continue
            
        gs = gold_states.get(ent_num)
        if not gs:
            # Not processed yet
            targets_to_process.append(ds)
        else:
            # Processed, check if there are new filings
            processed_filings = set(gs.get("processed_filings", []))
            has_new = any(ref not in processed_filings for ref in filings_done)
            if has_new:
                targets_to_process.append(ds)

    if not targets_to_process:
        print("\n>>> No new filings to process in HDFS. Gold Layer is up to date. <<<")
        sys.exit(0)

    print(f"Found {len(targets_to_process)} enterprises that need processing/updating.")

    # 4. Build HDFS file paths for target enterprises
    # We load all files for these enterprises to rebuild their historical records
    hdfs_paths = []
    for ds in targets_to_process:
        ent_num = ds["_id"]
        cleaned_bce = str(ent_num).replace(".", "").replace(" ", "").strip()
        # We specify the wildcard to read all exercise CSVs for this company
        hdfs_paths.append(f"{HDFS_NAMENODE}/{cleaned_bce}/hbb/*.csv")

    print(f"Constructed HDFS paths for Spark:")
    for path in hdfs_paths[:5]:
        print(f"  {path}")
    if len(hdfs_paths) > 5:
        print(f"  ... and {len(hdfs_paths) - 5} more.")

    # 5. Initialize Spark Session
    print("\nInitializing Spark Session...")
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit, coalesce, when, year, to_date, input_file_name, regexp_extract, first

    spark = SparkSession.builder \
        .appName("GoldLayerProcessing") \
        .getOrCreate()

    print("Reading CSV files from HDFS...")
    # Read files. They are comma-separated with headers=false (the first rows are metadata).
    # Since NBB CSVs contain mixed types and headers, we load them as string.
    raw_df = spark.read.option("header", "false").option("delimiter", ",").csv(hdfs_paths)

    # 6. Extract enterprise number and filing reference from the input file name
    # file_path example: "hdfs://namenode:9000/0718935492/hbb/2022-20458383.csv"
    raw_df = raw_df.withColumn("file_path", input_file_name())
    raw_df = raw_df.withColumn("enterprise_number", regexp_extract(col("file_path"), r"/([^/]+)/hbb/", 1))
    raw_df = raw_df.withColumn("filename", regexp_extract(col("file_path"), r"([^/]+)$", 1))
    raw_df = raw_df.withColumn("filing_ref", regexp_extract(col("filename"), r"(.+)\.csv$", 1))

    # 7. Pivot PCMN codes and metadata
    # The codes we need to extract and pivot
    pivot_values = [
        "Accounting period end date",
        "Model code",
        "70", "70/76A", "70/74",
        "60", "60/61", "60/64", "60/66A",
        "71",
        "9901",
        "9904",
        "54", "55", "54/58",
        "17", "43", "17/49",
        "10/15",
        "100", "1011"
    ]

    print("Pivoting data on PCMN codes...")
    pivoted_df = raw_df.groupBy("enterprise_number", "filing_ref", "file_path") \
        .pivot("_c0", pivot_values) \
        .agg(first("_c1"))

    # 8. Clean, cast, and map variables
    print("Mapping and transforming business fields...")
    clean_df = pivoted_df.select(
        col("enterprise_number"),
        col("filing_ref"),
        col("Accounting period end date").alias("period_end_date"),
        col("Model code").alias("model_code"),
        
        # Cast values to double
        col("70").cast("double").alias("val_70"),
        col("70/76A").cast("double").alias("val_70_76A"),
        col("70/74").cast("double").alias("val_70_74"),
        col("60").cast("double").alias("val_60"),
        col("60/61").cast("double").alias("val_60_61"),
        col("60/64").cast("double").alias("val_60_64"),
        col("60/66A").cast("double").alias("val_60_66A"),
        col("71").cast("double").alias("val_71"),
        col("9901").cast("double").alias("val_9901"),
        col("9904").cast("double").alias("val_9904"),
        col("54").cast("double").alias("val_54"),
        col("55").cast("double").alias("val_55"),
        col("54/58").cast("double").alias("val_54_58"),
        col("17").cast("double").alias("val_17"),
        col("43").cast("double").alias("val_43"),
        col("17/49").cast("double").alias("val_17_49"),
        col("10/15").cast("double").alias("val_10_15"),
        col("100").cast("double").alias("val_100"),
        col("1011").cast("double").alias("val_1011")
    )

    # Filter invalid dates and extract year
    clean_df = clean_df.withColumn("year", year(to_date(col("period_end_date"), "yyyy-MM-dd")))
    clean_df = clean_df.filter(col("year").isNotNull())

    # Map model code to schema_type
    clean_df = clean_df.withColumn(
        "schema_type",
        when(col("model_code").rlike("(?i)f"), "full")
        .when(col("model_code").rlike("(?i)[av]"), "abrege")
        .when(col("model_code").rlike("(?i)m"), "micro")
        .otherwise("full")
    )

    # Compute operational fields
    clean_df = clean_df.withColumn("ca", coalesce(col("val_70"), col("val_70_76A"), col("val_70_74"), lit(0.0)))
    clean_df = clean_df.withColumn("achats", coalesce(col("val_60"), col("val_60_61"), col("val_60_64"), col("val_60_66A"), lit(0.0)))
    clean_df = clean_df.withColumn("variation_stocks", coalesce(col("val_71"), lit(0.0)))
    clean_df = clean_df.withColumn("ebit", coalesce(col("val_9901"), lit(0.0)))
    clean_df = clean_df.withColumn("resultat_net", coalesce(col("val_9904"), lit(0.0)))

    # Tresorerie = 54 + 55 (fallback to 54/58)
    clean_df = clean_df.withColumn(
        "tresorerie",
        coalesce(col("val_54") + col("val_55"), col("val_54"), col("val_55"), col("val_54_58"), lit(0.0))
    )

    # Dettes financieres = 17 + 43 (fallback to 17/49)
    clean_df = clean_df.withColumn(
        "dettes_financieres",
        coalesce(col("val_17") + col("val_43"), col("val_17"), col("val_43"), col("val_17_49"), lit(0.0))
    )

    # Fonds propres = 10/15
    clean_df = clean_df.withColumn("fonds_propres", coalesce(col("val_10_15"), lit(0.0)))

    # Capital souscrit = 100 (fallback to 1011)
    clean_df = clean_df.withColumn("capital_souscrit", coalesce(col("val_100"), col("val_1011"), lit(0.0)))

    # Compute Marge brute
    clean_df = clean_df.withColumn("marge_brute", col("ca") - col("achats") + col("variation_stocks"))

    # Compute Ratios
    # marge_nette = Resultat net / CA * 100
    clean_df = clean_df.withColumn(
        "marge_nette",
        when(col("ca") != 0.0, (col("resultat_net") / col("ca")) * 100.0).otherwise(lit(None))
    )
    # roe = Resultat net / Fonds propres * 100
    clean_df = clean_df.withColumn(
        "roe",
        when(col("fonds_propres") != 0.0, (col("resultat_net") / col("fonds_propres")) * 100.0).otherwise(lit(None))
    )
    # liquidite = Tresorerie / Dettes financieres
    clean_df = clean_df.withColumn(
        "liquidite",
        when(col("dettes_financieres") != 0.0, col("tresorerie") / col("dettes_financieres")).otherwise(lit(None))
    )
    # endettement = Dettes financieres / Fonds propres * 100
    clean_df = clean_df.withColumn(
        "endettement",
        when(col("fonds_propres") != 0.0, (col("dettes_financieres") / col("fonds_propres")) * 100.0).otherwise(lit(None))
    )

    print("Collecting results to driver...")
    rows = clean_df.collect()
    print(f"Collected {len(rows)} filing rows.")

    # 9. Group by enterprise and structure documents
    print("Consolidating financial years by enterprise...")
    enterprises = {}
    for r in rows:
        ent_num = r.enterprise_number
        
        # We find the matching formatted enterprise number from StateDB targets to keep clean database format
        original_ent_num = ent_num
        for ds in download_states:
            test_bce = str(ds["_id"]).replace(".", "").replace(" ", "").strip()
            if test_bce == ent_num:
                original_ent_num = ds["_id"]
                break

        if original_ent_num not in enterprises:
            enterprises[original_ent_num] = {
                "enterprise_number": original_ent_num,
                "years": [],
                "schema_type": r.schema_type,
                "last_updated": datetime.utcnow()
            }

        year_obj = {
            "year": int(r.year),
            "ca": r.ca,
            "marge_brute": r.marge_brute,
            "ebit": r.ebit,
            "resultat_net": r.resultat_net,
            "tresorerie": r.tresorerie,
            "dettes_financieres": r.dettes_financieres,
            "fonds_propres": r.fonds_propres,
            "capital_souscrit": r.capital_souscrit,
            "ratios": {
                "marge_brute": r.marge_brute,
                "marge_nette": r.marge_nette,
                "roe": r.roe,
                "liquidite": r.liquidite,
                "endettement": r.endettement
            }
        }
        enterprises[original_ent_num]["years"].append(year_obj)

    # Sort years and update schema_type based on the latest exercise
    for ent_num, ent_data in enterprises.items():
        ent_data["years"] = sorted(ent_data["years"], key=lambda x: x["year"])
        # Find latest schema type
        if ent_data["years"]:
            latest_year = ent_data["years"][-1]["year"]
            # Find the row for the latest year to get the schema_type
            for r in rows:
                test_bce = str(ent_num).replace(".", "").replace(" ", "").strip()
                if r.enterprise_number == test_bce and int(r.year) == latest_year:
                    ent_data["schema_type"] = r.schema_type
                    break

    # 10. Upsert into MongoDB
    print(f"Connecting to MongoDB Gold Layer at {MONGO_URI}...")
    mongo_client = MongoClient(MONGO_URI)
    gold_db = mongo_client[MONGO_DB]
    gold_col = gold_db["hotel_gold"]

    print("Upserting consolidated documents into MongoDB...")
    for ent_num, ent_data in enterprises.items():
        gold_col.update_one(
            {"enterprise_number": ent_num},
            {"$set": ent_data},
            upsert=True
        )
        print(f"  Upserted {ent_num} with {len(ent_data['years'])} years.")

    # 11. Update StateDB gold_state
    print("Updating State DB (gold_state) to track processed filings...")
    for ds in targets_to_process:
        ent_num = ds["_id"]
        filings_done = ds.get("filings_done", [])
        gold_state_col.update_one(
            {"_id": ent_num},
            {
                "$set": {
                    "processed_filings": filings_done,
                    "last_processed": datetime.utcnow()
                }
            },
            upsert=True
        )
        print(f"  State updated for {ent_num}.")

    print("\nGold layer population complete!")

if __name__ == "__main__":
    main()
