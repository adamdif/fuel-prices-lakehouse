import argparse
import pandas as pd
import os
import json
import hashlib
import re
from dotenv import load_dotenv
from io import BytesIO
from datetime import date, datetime, timezone
import boto3

# ===============================================

def to_snake_case(name: str) -> str:
    """
    Transform raw columns names into clean standardized ones
    Example :
    "Prix TTC (€)" -> "prix_ttc"
    "Code postal" -> "code_postal"
    """
    name = name.strip()
    name = name.replace("’", "'")
    name = re.sub(r"[^\w]+", "_", name, flags=re.UNICODE)
    name = re.sub(r"_+", "_", name)
    return name.strip("_").lower()

def compute_record_hash(df: pd.DataFrame, exclude_cols: set[str]) -> pd.Series:
    """
    Create unique id for each row based on its content
    """
    business_cols = [c for c in df.columns if c not in exclude_cols]
    # stable order
    business_cols = sorted(business_cols)

    def row_hash(row) -> str:
        parts = []
        for c in business_cols:
            v = row[c]
            if pd.isna(v):
                parts.append("")
            else:
                parts.append(str(v).strip())
        raw = "|".join(parts).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()

    return df.apply(row_hash, axis=1)

def s3_client():
    load_dotenv()
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")

    if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION]):
        raise ValueError("Missing AWS env vars: AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/AWS_DEFAULT_REGION")

    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION,
    )

def read_s3_bytes(s3, bucket: str, key: str) -> bytes:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read()


# =====================
# parameters
# =====================

# source_url = "https://www.data.gouv.fr/api/1/datasets/r/336c34b5-a527-4c35-b84d-18462daa7c51"
# today = date.today()
# today_str = str(today)
# ingestion_ts = datetime.now(timezone.utc).isoformat()

# s3_path = f"s3://{S3_BUCKET}/fuel-prices/bronze/ingestion_date={today_str}/data.csv.gz"

# df = pd.read_csv(
#     s3_path,
#     sep=';',
#     compression='gzip'
# )

# print(df.shape)
# print(df.head())

# df["ingestion_date"] = today
# df["ingestion_ts"] = ingestion_ts
# df["record_hash"] = source_url

# ===============================================

def main():

    parser = argparse.ArgumentParser(description="Build Silver layer for a given ingestion_date")
    parser.add_argument("--date", dest="ingestion_date", default=str(date.today()), help="YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    load_dotenv()
    bucket = os.getenv("S3_BUCKET")
    prefix = os.getenv("S3_PREFIX", "fuel-prices")

    if not bucket:
        raise ValueError("Missing S3_BUCKET in environment (.env)")

    ingestion_date = args.ingestion_date
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    bronze_prefix = f"{prefix}/bronze/ingestion_date={ingestion_date}/"
    bronze_data_key = f"{bronze_prefix}data.csv.gz"
    bronze_metadata_key = f"{bronze_prefix}metadata.json"

    silver_prefix = f"{prefix}/silver/ingestion_date={ingestion_date}/"
    silver_parquet_key = f"{silver_prefix}data.parquet"
    silver_metadata_key = f"{silver_prefix}metadata.json"

    s3 = s3_client()

    # Read Bronze metadata
    bronze_metadata = None
    try:
        bronze_metadata_bytes = read_s3_bytes(s3, bucket, bronze_metadata_key)
        bronze_metadata = json.loads(bronze_metadata_bytes.decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        bronze_metadata = None

    # Read Bronze data (csv.gz)
    csv_gz_bytes = read_s3_bytes(s3, bucket, bronze_data_key)

    df = pd.read_csv(
        BytesIO(csv_gz_bytes),
        sep=";",
        compression="gzip",
        encoding="utf-8",
        engine="python",
    )

    # Normalize column names
    df.columns = [to_snake_case(c) for c in df.columns]

    # Add technical columns
    df["ingestion_date"] = ingestion_date
    df["ingestion_ts"] = ingestion_ts

    exclude_for_hash = {"ingestion_date", "ingestion_ts", "record_hash"}
    df["record_hash"] = compute_record_hash(df, exclude_cols=exclude_for_hash)

    # Write parquet locally
    os.makedirs("data/silver", exist_ok=True)
    local_parquet = f"data/silver/silver-ingestion_date={ingestion_date}.parquet"

    df.to_parquet(
        local_parquet,
        engine="pyarrow",
        compression="snappy",
        index=False,
    )

    # Upload parquet to S3
    s3.upload_file(local_parquet, bucket, silver_parquet_key)

    # Silver metadata
    silver_metadata = {
        "layer": "silver",
        "ingestion_date": ingestion_date,
        "ingestion_ts": ingestion_ts,
        "source": {
            "bronze_data_key": bronze_data_key,
            "bronze_metadata_key": bronze_metadata_key,
            "bronze_metadata": bronze_metadata,
        },
        "destination": {
            "s3_bucket": bucket,
            "s3_parquet_key": silver_parquet_key,
        },
        "rows": int(df.shape[0]),
        "columns": list(df.columns),
    }

    os.makedirs("data/silver", exist_ok=True)
    local_metadata = f"data/silver/silver-ingestion_date={ingestion_date}.metadata.json"
    with open(local_metadata, "w", encoding="utf-8") as f:
        json.dump(silver_metadata, f, ensure_ascii=False, indent=2)

    s3.upload_file(local_metadata, bucket, silver_metadata_key)

    print("Silver written")
    print("S3 parquet:", f"s3://{bucket}/{silver_parquet_key}")
    print("S3 metadata:", f"s3://{bucket}/{silver_metadata_key}")
    print("Rows:", df.shape[0], "Cols:", df.shape[1])


if __name__ == "__main__":
    main()