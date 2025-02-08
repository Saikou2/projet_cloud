import os
from google.cloud import storage
import logging
from config import BUCKET_NAME, INPUT_FOLDER

def upload_file_to_input(local_file_path, destination_filename):
    try:
        if not os.path.exists(local_file_path):
            logging.error(f"Le fichier {local_file_path} n'existe pas.")
            print(f"❌ Erreur : Le fichier {local_file_path} est introuvable.")
            return
        
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"{INPUT_FOLDER}{destination_filename}")
        blob.upload_from_filename(local_file_path)
        
        logging.info(f"✅ Fichier {destination_filename} envoyé dans {INPUT_FOLDER}")
        print(f"✅ Fichier {destination_filename} bien envoyé !")
    
    except Exception as e:
        logging.error(f"❌ Erreur lors de l'envoi du fichier: {e}")
        print(f"❌ Erreur : {e}")

if __name__ == "__main__":
    upload_file_to_input("transactions.csv", "transactions.csv")
