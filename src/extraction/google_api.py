import os
import json
import time
import requests
import re

# Seuil maximal de requêtes par mois
MAX_REQUESTS = 3000
FILE_PATH = 'request_counter.json'

def load_request_count():
    """Charge le compteur de requêtes à partir du fichier."""
    try:
        with open(FILE_PATH, 'r') as file:
            # Si le fichier est vide, json.load() lèvera une erreur
            data = json.load(file)
            return data
    except (FileNotFoundError, json.JSONDecodeError):
        # Gère le cas où le fichier n'existe pas OU est vide/corrompu
        return {'requests_sent': 0, 'last_reset': time.time()}

def save_request_count(count_data):
    """Enregistre le compteur de requêtes dans le fichier."""
    with open(FILE_PATH, 'w') as file:
        json.dump(count_data, file)

def check_and_update_request_count():
    """Vérifie et met à jour le nombre de requêtes envoyées."""
    count_data = load_request_count()
    current_time = time.time()
    
    # Réinitialise si plus de 30 jours se sont écoulés
    if current_time - count_data.get('last_reset', 0) > 30 * 24 * 60 * 60:
        print("Réinitialisation du compteur de requêtes API.")
        count_data['requests_sent'] = 0
        count_data['last_reset'] = current_time

    if count_data['requests_sent'] >= MAX_REQUESTS:
        print(f"⚠️ Seuil de {MAX_REQUESTS} requêtes atteint. Réessayer plus tard.")
        return False
    
    count_data['requests_sent'] += 1
    save_request_count(count_data)
    print(f"Requête API n°{count_data['requests_sent']}/{MAX_REQUESTS} ce mois-ci.")
    return True

def normalize_text(text):
    """Met en minuscule, supprime les accents et la ponctuation simple."""
    if not text: return ""
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text) # Garde seulement les lettres, chiffres et espaces
    return text.strip()

def normalize_phone(phone_number):
    """Supprime tout sauf les chiffres."""
    if not phone_number: return ""
    return re.sub(r'\D', '', phone_number)

def validate_result(scraped_item, google_details):
    """
    Valide si le résultat de Google correspond à l'item scrappé.
    Retourne True si la correspondance est bonne, False sinon.
    """
    # Critère 1 : Numéro de téléphone (le plus fiable)
    scraped_phones = scraped_item.get("tel") or []
    google_phone = google_details.get("internationalPhoneNumber")
    
    if google_phone and scraped_phones:
        norm_google_phone = normalize_phone(google_phone)
        for phone in scraped_phones:
            if normalize_phone(phone) == norm_google_phone:
                print("  ✅ Validation par téléphone réussie.")
                return True

    # Critère 2 : Similarité du nom (si le téléphone échoue ou est absent)
    scraped_name = normalize_text(scraped_item.get("nom"))
    google_name = normalize_text(google_details.get("displayName", {}).get("text"))

    if scraped_name and google_name:
        # Vérifie si l'un est contenu dans l'autre (gère "Bistrot" vs "Le Bistrot")
        if scraped_name in google_name or google_name in scraped_name:
            print("  ✅ Validation par nom réussie.")
            return True

    print(f"  ❌ Échec de la validation. Scrappé: '{scraped_item.get('nom')}', Google a trouvé: '{google_details.get('displayName', {}).get('text')}'")
    return False


def make_api_request(scraped_item):
    """Fait une requête à l'API Google Places et valide le résultat."""
    API_KEY = os.getenv("GOOGLE_KEY")
    nom = scraped_item.get("nom")
    adresse = scraped_item.get("adresse")

    if not API_KEY:
        print("Erreur: La variable d'environnement GOOGLE_KEY n'est pas définie.")
        return None

    place_id = None
    
    # --- Première requête: Find Place ---
    if not check_and_update_request_count(): return None
        
    url_search = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    params = {"input": f"{nom} {adresse}", "inputtype": "textquery", "fields": "place_id", "key": API_KEY}
    
    try:
        response_search = requests.get(url_search, params=params)
        response_search.raise_for_status()
        data_search = response_search.json()
        # --- LIGNE DE DÉBOGAGE ---
        print(f"  -> Réponse de l'API Google : {data_search}")
        
        if data_search.get("candidates"):
            place_id = data_search["candidates"][0]["place_id"]
        else:
            print(f"  -> Aucun 'place_id' trouvé pour {nom}.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"  -> Erreur lors de la recherche du lieu : {e}")
        return None

    # --- Deuxième requête: Place Details ---
    if place_id:
        if not check_and_update_request_count(): return None
            
        url_detail = f"https://places.googleapis.com/v1/places/{place_id}"
        headers = {"X-Goog-Api-Key": API_KEY, "X-Goog-FieldMask": "*"}
        
        try:
            response_detail = requests.get(url_detail, headers=headers)
            response_detail.raise_for_status()
            google_details = response_detail.json()
            
            # --- ÉTAPE DE VALIDATION ---
            if validate_result(scraped_item, google_details):
                return google_details # On retourne les données seulement si la validation est bonne
            else:
                return None # Sinon, on considère que ce n'est pas le bon établissement

        except requests.exceptions.RequestException as e:
            print(f"  -> Erreur lors de la récupération des détails du lieu : {e}")
            return None
            
    return None