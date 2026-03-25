# arguments : 1) chemin local du fichier à envoyer, 2) nom du bucket, 3) chemin dans le bucket
s3.upload_file(
    f"data/csv/data-as-of-{today_str}.csv.gz",
    S3_BUCKET, 
    f"fuel-prices/bronze/ingestion_date={today_str}/data.csv.gz"
)
s3_ls(S3_BUCKET)