# config.py
import os
import logging

# Configuration for GCP
PROJECT_ID = "isi-group-m2-dsia"
BUCKET_NAME = "m2dsia-diallo-saikou-oumar-data"
DATASET_NAME = "data_diallo_saikou_oumar"
TABLE_NAME = "transactions"

# Folder paths
INPUT_FOLDER = "input/"
CLEAN_FOLDER = "clean/"
ERROR_FOLDER = "error/"
DONE_FOLDER = "done/"

LOCAL_TEMP_DIR = "temp/"

# Schema BigQuery
BIGQUERY_SCHEMA = [
    {"name": "transaction_id", "type": "INT64"},
    {"name": "product_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "category", "type": "STRING", "mode": "REQUIRED"},
    {"name": "price", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "quantity", "type": "INT64", "mode": "REQUIRED"},
    {"name": "date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "customer_name", "type": "STRING"},
    {"name": "customer_email", "type": "STRING"}
]

# Configuration Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
