import os
import logging
from google.cloud import storage
from data_processing import download_files, clean_data, upload_to_bigquery
from config import BUCKET_NAME, CLEAN_FOLDER, ERROR_FOLDER, DONE_FOLDER

# Configuration des logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def move_file(bucket, source_path, destination_path):
    """Déplace un fichier d'un dossier à un autre dans le bucket GCS."""
    try:
        blob = bucket.blob(source_path)
        new_blob = bucket.copy_blob(blob, bucket, destination_path)
        blob.delete()
        logging.info(f"Fichier déplacé: {source_path} -> {destination_path}")
    except Exception as e:
        logging.error(f"Erreur lors du déplacement du fichier {source_path} -> {destination_path}: {e}")

def main():
    """Pipeline principal: téléchargement, nettoyage, upload et archivage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    for file_path in download_files():
        logging.info(f"Traitement du fichier: {file_path}")
        df = clean_data(file_path)
        filename = os.path.basename(file_path)

        if df is not None:
            upload_to_bigquery(df)
            move_file(bucket, f"input/{filename}", f"{CLEAN_FOLDER}/{filename}")
            move_file(bucket, f"{CLEAN_FOLDER}/{filename}", f"{DONE_FOLDER}/{filename}")
        else:
            move_file(bucket, f"input/{filename}", f"{ERROR_FOLDER}/{filename}")

if __name__ == "__main__":
    main()
