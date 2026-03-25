import os
import boto3
import requests
from dotenv import load_dotenv

from datetime import date

import pandas as pd

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

print(today_str)

url = "https://www.data.gouv.fr/api/1/datasets/r/336c34b5-a527-4c35-b84d-18462daa7c51"
filename = f"data/csv/data-as-of-{today_str}.csv"

# =========================
# Env vars
# =========================

load_dotenv()
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")

with requests.get(url, stream=True) as r:
    r.raise_for_status()
    with open(filename, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)

print("File downloaded at \"data/csv\"")

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_DEFAULT_REGION
)

#"ls" function to check what's in the bucket:
def s3_ls(s3_bucket):
    response = s3.list_objects_v2(Bucket=s3_bucket)
    if "Contents" in response:
        for obj in response["Contents"]:
            print(obj["Key"])
    else:
        print("Bucket is empty or permissions issue")

df_today = pd.read_csv(f"data/csv/data-as-of-{today_str}.csv", sep=';', engine='python', encoding='utf-8')
df_today.to_csv(f"data/csv/data-as-of-{today_str}.csv.gz", index = False, compression="gzip")

# arguments : 1) chemin local du fichier à envoyer, 2) nom du bucket, 3) chemin dans le bucket
s3.upload_file(
    f"data/csv/data-as-of-{today_str}.csv.gz",
    S3_BUCKET, 
    f"fuel-prices/bronze/ingestion_date={today_str}/data.csv.gz"
)
s3_ls(S3_BUCKET)

# df_today.to_parquet(
#     f"data/parquet/data-as-of-{today_str}.parquet",
#     engine="pyarrow",
#     compression="snappy" #recommended for s3
# )

# table = pd.read_parquet(f"data/parquet/data-as-of-{today_str}.parquet")
# print(table.shape)
# print(table.head())

# arguments : 1) chemin local du fichier à envoyer, 2) nom du bucket, 3) chemin dans le bucket
# s3.upload_file(
#     f"data/parquet/data-as-of-{today_str}.parquet",
#     S3_BUCKET, 
#     f"fuel-prices/bronze/ingestion_date={today_str}/data.parquet"
# )
# s3_ls(S3_BUCKET)

# s3.delete_object(Bucket=S3_BUCKET, Key=f"fuel-prices/bronze/ingestion_date={today_str}/data.parquet")
# s3_ls(S3_BUCKET)