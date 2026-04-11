import os
import json
from datetime import date, datetime, timezone
from io import BytesIO
import argparse
import boto3
import pandas as pd
from dotenv import load_dotenv
from botocore.exceptions import ClientError


def s3_client():
    load_dotenv()
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_default_region = os.getenv("AWS_DEFAULT_REGION")

    if not all([aws_access_key_id, aws_secret_access_key, aws_default_region]):
        raise ValueError(
            "Missing AWS env vars: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION"
        )

    return boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_default_region,
    )


def read_s3_bytes(s3, bucket: str, key: str) -> bytes:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read()


def read_json(s3, bucket: str, key: str):
    try:
        raw = read_s3_bytes(s3, bucket, key)
        return json.loads(raw.decode("utf-8"))
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code in {"NoSuchKey", "404"}:
            return None
        raise


def detect_column(columns: list[str], searched_columns: list[str], label: str) -> str:
    for searched in searched_columns:
        if searched in columns:
            return searched

    raise ValueError(
        f"Could not find a '{label}' column. "
        f"Available columns: {columns}. "
        f"Tried: {searched_columns}"
    )


def main():

    parser = argparse.ArgumentParser(description="Build Gold layer for a given ingestion_date")
    parser.add_argument(
        "--date",
        dest="ingestion_date",
        default=str(datetime.now(timezone.utc).date()),
        help="YYYY-MM-DD (default: today UTC)",
    )
    args = parser.parse_args()

    load_dotenv()
    bucket = os.getenv("S3_BUCKET")
    prefix = os.getenv("S3_PREFIX")

    if not bucket:
        raise ValueError("Missing S3_BUCKET in environment (.env)")

    ingestion_date = args.ingestion_date
    processing_ts = datetime.now(timezone.utc).isoformat()

    silver_prefix = f"{prefix}/silver/ingestion_date={ingestion_date}/"
    silver_parquet_key = f"{silver_prefix}data.parquet"
    silver_metadata_key = f"{silver_prefix}metadata.json"

    gold_prefix = f"{prefix}/gold/ingestion_date={ingestion_date}/"
    gold_parquet_key = f"{gold_prefix}data.parquet"
    gold_metadata_key = f"{gold_prefix}metadata.json"

    s3 = s3_client()

    # Read Silver metadata (optional)
    silver_metadata = read_json(s3, bucket, silver_metadata_key)

    # Read Silver parquet from S3
    parquet_bytes = read_s3_bytes(s3, bucket, silver_parquet_key)
    df = pd.read_parquet(BytesIO(parquet_bytes), engine="pyarrow")

    if df.empty:
        raise ValueError(f"Silver dataset is empty for ingestion_date={ingestion_date}")

    # Transofrmation

    # Detect relevant business columns
    carburant_col = detect_column(
        list(df.columns),
        searched_columns=[
            "carburant",
            "nom_carburant",
            "type_carburant",
            "produit",
            "fuel_type",
            "gas_type",
        ],
        label="carburant",
    )

    prix_col = detect_column(
        list(df.columns),
        searched_columns=[
            "prix",
            "prix_valeur",
            "valeur",
            "price",
            "price_value",
        ],
        label="prix",
    )

    # Clean
    working_df = df[[carburant_col, prix_col]].copy()
    working_df[carburant_col] = working_df[carburant_col].astype(str).str.strip() # Standardize carb column
    working_df[prix_col] = pd.to_numeric(working_df[prix_col], errors="coerce") # Convert prices into num, else 'NaN'

    # Remove invalid lines
    working_df = working_df.dropna(subset=[carburant_col, prix_col]).copy()
    working_df = working_df[working_df[carburant_col] != ""].copy()

    if working_df.empty:
        raise ValueError(
            f"No valid rows left after cleaning. carburant_col={carburant_col}, prix_col={prix_col}"
        )

    # Aggregate Gold
    gold_df = (
        working_df.groupby(carburant_col, dropna=False)
        .agg(
            nb_rows=(prix_col, "count"),
            prix_moyen=(prix_col, "mean"),
            prix_min=(prix_col, "min"),
            prix_max=(prix_col, "max"),
        )
        .reset_index()
        .rename(columns={carburant_col: "carburant"})
    )

    gold_df["ingestion_date"] = ingestion_date
    gold_df["processing_ts"] = processing_ts

    gold_df = gold_df[
        [
            "ingestion_date",
            "processing_ts",
            "carburant",
            "nb_rows",
            "prix_moyen",
            "prix_min",
            "prix_max",
        ]
    ].copy()

    gold_df["prix_moyen"] = gold_df["prix_moyen"].round(3)
    gold_df["prix_min"] = gold_df["prix_min"].round(3)
    gold_df["prix_max"] = gold_df["prix_max"].round(3)

    gold_df = gold_df.sort_values(by=["carburant"]).reset_index(drop=True)

    # Write parquet locally
    os.makedirs("data/gold", exist_ok=True)
    local_parquet = f"data/gold/gold-ingestion_date={ingestion_date}.parquet"

    gold_df.to_parquet(
        local_parquet,
        engine="pyarrow",
        compression="snappy",
        index=False,
    )

    # Upload parquet to S3
    s3.upload_file(local_parquet, bucket, gold_parquet_key)

    # Gold metadata
    gold_metadata = {
        "layer": "gold",
        "ingestion_date": ingestion_date,
        "processing_ts": processing_ts,
        "source": {
            "silver_parquet_key": silver_parquet_key,
            "silver_metadata_key": silver_metadata_key,
            "silver_metadata": silver_metadata,
        },
        "destination": {
            "s3_bucket": bucket,
            "s3_parquet_key": gold_parquet_key,
        },
        "aggregation": {
            "group_by": ["carburant"],
            "metrics": ["nb_rows", "prix_moyen", "prix_min", "prix_max"],
        },
        "detected_columns": {
            "carburant_col": carburant_col,
            "prix_col": prix_col,
        },
        "rows": int(gold_df.shape[0]),
        "columns": list(gold_df.columns),
    }

    local_metadata = f"data/gold/gold-ingestion_date={ingestion_date}.metadata.json"
    with open(local_metadata, "w", encoding="utf-8") as f:
        json.dump(gold_metadata, f, ensure_ascii=False, indent=2)

    s3.upload_file(local_metadata, bucket, gold_metadata_key)

    print("Gold written")
    print("S3 parquet:", f"s3://{bucket}/{gold_parquet_key}")
    print("S3 metadata:", f"s3://{bucket}/{gold_metadata_key}")
    print("Detected carburant column:", carburant_col)
    print("Detected prix column:", prix_col)
    print("Rows:", gold_df.shape[0], "Cols:", gold_df.shape[1])
    print()
    print(gold_df.head(20).to_string(index=False))


if __name__ == "__main__":
    main()