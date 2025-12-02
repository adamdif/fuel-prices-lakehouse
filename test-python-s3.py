import os
import boto3
from dotenv import load_dotenv

import pandas as pd

##########
"""
The goal of this code is to :
1) load the csv
2) adds technical desired columns
3) calculates record_hash
4) normalizes types if necessary
5) writes local parquet
6) pushes it to s3 towards bronze/
"""
##########

csv_headerline = "id;latitude;longitude;Code postal;pop;Adresse;Ville;services;prix;rupture;horaires;geom;Prix Gazole mis à jour le;Prix Gazole;Prix SP95 mis à jour le;Prix SP95;Prix E85 mis à jour le;Prix E85;Prix GPLc mis à jour le;Prix GPLc;Prix E10 mis à jour le;Prix E10;Prix SP98 mis à jour le;Prix SP98;Début rupture e10 (si temporaire);Type rupture e10;Début rupture sp98 (si temporaire);Type rupture sp98;Début rupture sp95 (si temporaire);Type rupture sp95;Début rupture e85 (si temporaire);Type rupture e85;Début rupture GPLc (si temporaire);Type rupture GPLc;Début rupture gazole (si temporaire);Type rupture gazole;Carburants disponibles;Carburants indisponibles;Carburants en rupture temporaire;Carburants en rupture definitive;Automate 24-24 (oui/non);Services proposés;Département;code_departement;Région;code_region;horaires détaillés"


load_dotenv()
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_DEFAULT_REGION
)

# petit ls:
response = s3.list_objects_v2(Bucket=S3_BUCKET)
if "Contents" in response:
    for obj in response["Contents"]:
        print(obj["Key"])
else:
    print("Bucket is empty or permissions issue")

print(S3_BUCKET)