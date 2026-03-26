import os
import boto3
import requests
from dotenv import load_dotenv
import hashlib
import gzip
import json

from datetime import date, datetime, timezone

# =========================
# Local directories
# =========================

os.makedirs("data/csv", exist_ok=True)
os.makedirs("data/parquet", exist_ok=True)

# =========================
# Dates and paths
# =========================

today = date.today()
today_str = str(today)
ingestion_ts = datetime.now(timezone.utc).isoformat()

print(today_str)

source_url = "https://www.data.gouv.fr/api/1/datasets/r/336c34b5-a527-4c35-b84d-18462daa7c51"
local_csv_path = f"data/csv/data-as-of-{today_str}.csv"
local_gz_path = f"data/csv/data-as-of-{today_str}.csv.gz"
local_metadata_path = f"data/csv/data-as-of-{today_str}.json"

s3_prefix = f"fuel-prices/bronze/ingestion_date={today_str}/"
s3_data_key = f"{s3_prefix}data.csv.gz"
s3_metadata_key = f"{s3_prefix}metadata.json"

# =========================
# Env vars
# =========================

load_dotenv()
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")

if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, S3_BUCKET]):
    raise ValueError("Missing one or more AWS environment variables.")

# =========================
# S3 client
# =========================

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_DEFAULT_REGION
)

# =========================
# Base functions
# =========================

def download_file(url, output_file):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(output_file, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    print(f"File downloaded at {output_file}")

def s3_ls(s3_bucket: str) -> None:
    """\"ls\" function to check what's in the bucket (main purpose is for debug)"""
    response = s3.list_objects_v2(Bucket=s3_bucket)
    if "Contents" in response:
        for obj in response["Contents"]:
            print(obj["Key"])
    else:
        print("Bucket is empty or permissions issue")

def gzip_file(input_path: str, output_path: str) -> None:
    """Compress file at binary level to preserve raw fidelity"""
    with open(input_path, "rb") as f_in, gzip.open(output_path, "wb") as f_out:
        while True:
            chunk = f_in.read(8192)
            if not chunk:
                break
            f_out.write(chunk)

# for metadata
def compute_sha256(file_path: str) -> str:
    """Compute SHA256"""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()

# =========================
# Download raw csv source
# =========================

download_file(source_url, local_csv_path)
print(f"Raw file downloaded: {local_csv_path}")

# =========================
# Compress raw file as csv.gz
# =========================

gzip_file(local_csv_path, local_gz_path)
print(f"Gzipped raw file created: {local_gz_path}")

# =========================
# Build metadata
# =========================

file_size_bytes = os.path.getsize(local_gz_path)
file_sha256 = compute_sha256(local_gz_path)

metadata = {
    "source_url": source_url,
    "ingestion_date": today_str,
    "ingestion_ts": ingestion_ts,
    "s3_bucket": S3_BUCKET,
    "s3_key": s3_data_key,
    "file_name": os.path.basename(local_gz_path),
    "bytes": file_size_bytes,
    "sha256": file_sha256,
    "format": "csv.gz",
    "layer": "bronze",
}

with open(local_metadata_path, "w", encoding="utf-8") as f:
    json.dump(metadata, f, ensure_ascii=False, indent=2)

print(f"Metadata file created: {local_metadata_path}")

# =========================
# Upload
# =========================

# arguments : 1) chemin local du fichier à envoyer, 2) nom du bucket, 3) chemin dans le bucket
s3.upload_file(
    local_gz_path,
    S3_BUCKET, 
    s3_data_key
)

s3.upload_file(
    local_metadata_path,
    S3_BUCKET, 
    s3_metadata_key
)

s3_ls(S3_BUCKET)
