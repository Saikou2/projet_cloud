import pandas as pd  # type: ignore
import os
import logging
from google.cloud import storage, bigquery
from config import *

# Format correct de BIGQUERY_TABLE
BIGQUERY_TABLE = f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}"

# Configuration du logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def download_files():
    """Télécharge les fichiers depuis le bucket GCS vers un dossier local."""
    try:
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blobs = bucket.list_blobs(prefix=INPUT_FOLDER)

        if not os.path.exists(LOCAL_TEMP_DIR):
            os.makedirs(LOCAL_TEMP_DIR)

        for blob in blobs:
            filename = os.path.basename(blob.name)  # Correction de l'extraction du nom du fichier
            local_path = os.path.join(LOCAL_TEMP_DIR, filename)

            if not os.path.exists(local_path):
                try:
                    blob.download_to_filename(local_path)
                    logging.info(f"Fichier téléchargé: {filename}")
                except Exception as e:
                    logging.error(f"Erreur lors du téléchargement de {filename}: {e}")
                    continue  # Passe au fichier suivant
            else:
                logging.info(f"Fichier déjà présent: {filename}")

            yield local_path
    except Exception as e:
        logging.error(f"Erreur lors de l'accès au bucket {BUCKET_NAME}: {e}")

def clean_data(file_path):
    """Nettoie les données en supprimant les valeurs manquantes et en convertissant la date."""
    try:
        df = pd.read_csv(file_path)
        df = df.dropna()

        # Vérification et conversion de la colonne 'date'
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors='coerce').dt.date
        else:
            logging.warning(f"Colonne 'date' absente dans {file_path}")

        logging.info(f"Nettoyage réussi: {file_path}")
        return df
    except Exception as e:
        logging.error(f"Erreur lors du nettoyage de {file_path}: {e}")
        return None

def upload_to_bigquery(df):
    """Charge un DataFrame dans une table BigQuery."""
    try:
        client = bigquery.Client()
        
        # Vérification que BIGQUERY_SCHEMA est bien défini
        if not isinstance(BIGQUERY_SCHEMA, list):
            raise ValueError("BIGQUERY_SCHEMA doit être une liste de dictionnaires {'name': 'colonne', 'type': 'STRING/INTEGER/etc.'}")

        job_config = bigquery.LoadJobConfig(schema=BIGQUERY_SCHEMA, write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(df, BIGQUERY_TABLE, job_config=job_config)
        job.result()  # Attente de la fin du job
        
        logging.info(f"{len(df)} lignes insérées dans {BIGQUERY_TABLE}.")
    except Exception as e:
        logging.error(f"Erreur lors du chargement dans BigQuery: {e}", exc_info=True)
