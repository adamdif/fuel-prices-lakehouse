import argparse
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")

if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, S3_BUCKET]):
    raise ValueError("Missing one or more AWS environment variables.")

bucket = "s3-fuel-lake-dev"
today_str = "2026-03-26"

s3_path = f"s3://{bucket}/fuel-prices/bronze/ingestion_date={today_str}/data.csv.gz"

df = pd.read_csv(
    s3_path,
    sep=';',
    compression='gzip'
)

print(df.shape)
print(df.head())