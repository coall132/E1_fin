import os
import json
import tempfile
import datetime as dt
import re
import unicodedata
from minio import Minio
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Charge les variables d'environnement depuis le fichier .env
load_dotenv()

# Imports depuis les modules locaux
from scrapper import scrapping_page_jaune
from google_api import make_api_request

# --- CONFIGURATION ---
client = Minio(
    "localhost:9000",
    access_key=os.getenv("AWS_ACCESS_KEY_ID", "admin"),
    secret_key=os.getenv("AWS_SECRET_KEY", "admin123"),
    secure=False
)
BUCKET_NAME = "datalake"

# --- FONCTIONS UTILITAIRES ---
def clean_filename(s):
    s = unicodedata.normalize('NFD', s).encode('ascii', 'ignore').decode('utf-8')
    return re.sub(r'[^a-zA-Z0-9_-]', '_', s)

def upload_data_to_minio(data: dict, object_name: str):
    json_data = json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(json_data)
        tmp_path = tmp.name
    try:
        client.fput_object(BUCKET_NAME, object_name, tmp_path, content_type="application/json")
        print(f"  -> Upload réussi : {object_name}")
    finally:
        os.remove(tmp_path)

def get_existing_files(prefix: str) -> set:
    """Récupère les noms de fichiers existants dans un préfixe sur MinIO."""
    try:
        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)
        return {os.path.basename(obj.object_name) for obj in client.list_objects(BUCKET_NAME, prefix=prefix, recursive=True)}
    except Exception as e:
        print(f"Impossible de lister les fichiers dans MinIO : {e}")
        return set()

# --- SCRIPT PRINCIPAL ---
def main():
    liste_departement = [37]
    liste_activite = ["Restaurants"] #muséee, hotel
    
    print("--- Démarrage de l'extraction de données (Local) ---")
    
    existing_pj_files = get_existing_files(f"raw/page_jaune/{dt.date.today()}/")
    existing_google_files = get_existing_files(f"raw/API_google/{dt.date.today()}/")
    print(f"{len(existing_pj_files)} fichiers PagesJaunes et {len(existing_google_files)} fichiers Google existent déjà pour aujourd'hui.")

    for dep in liste_departement:
        for activite in liste_activite:
            print(f"\n--- Traitement de : '{activite}' dans le département {dep} ---")
            scraped_results = scrapping_page_jaune(activite, dep)
            
            for item in scraped_results:
                nom = clean_filename(item.get('nom', 'sans_nom'))
                adresse = clean_filename(item.get('adresse', 'sans_adresse'))
                
                if nom == 'sans_nom' or adresse == 'sans_adresse':
                    continue

                file_identifier = f"{nom}_-_{adresse}.json"
                print(f"Traitement de l'établissement : {item.get('nom')}")

                # 1. Traitement Pages Jaunes
                if file_identifier not in existing_pj_files:
                    object_name_pj = f"raw/page_jaune/{dt.date.today()}/{file_identifier}"
                    if upload_data_to_minio(item, object_name_pj):
                        existing_pj_files.add(file_identifier)
                else:
                    print(f"  -> Fichier PagesJaunes déjà existant : {file_identifier}")

                # 2. Traitement API Google
                if file_identifier not in existing_google_files:
                    # On passe l'objet 'item' complet pour la validation
                    google_data = make_api_request(item)
                    if google_data:
                        object_name_google = f"raw/API_google/{dt.date.today()}/{file_identifier}"
                        if upload_data_to_minio(google_data, object_name_google):
                            existing_google_files.add(file_identifier)
                else:
                    print(f"  -> Fichier Google API déjà existant : {file_identifier}")

    print("\n--- Extraction de données terminée. ---")

if __name__ == "__main__":
    main()