import requests
import json

# --- Configuration ---
# L'adresse de base de votre API qui tourne dans Docker
BASE_URL = "http://localhost:8000"
# Votre clé d'API secrète
API_KEY = "coall"

# Préparation de l'en-tête (header) qui contiendra la clé d'API
# C'est cet en-tête que votre API FastAPI va vérifier.
headers = {
    "X-API-KEY": API_KEY
}

def test_root():
    """
    Teste l'endpoint racine pour vérifier que l'API est bien en ligne.
    """
    print("--- Test 0: Vérification de la racine de l'API ---")
    url = f"{BASE_URL}/"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            print(f"✅ Succès ! Message reçu : {response.json().get('message')}")
        else:
            print(f"❌ Erreur ! Code de statut : {response.status_code}")
    except requests.exceptions.ConnectionError:
        print(f"❌ Erreur de connexion : Impossible de joindre l'API à l'adresse {url}.")
        print("   Vérifiez que vos conteneurs Docker sont bien en cours d'exécution.")
        return False
    return True

def test_get_all_etablissements():
    """
    Teste l'endpoint pour récupérer la liste de tous les établissements.
    """
    print("\n--- Test 1: Récupération de la liste des établissements ---")
    url = f"{BASE_URL}/etablissements/"
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            print("✅ Succès ! La requête a été autorisée.")
            etablissements = response.json()
            print(f"Nombre d'établissements reçus : {len(etablissements)}")
            
            # Affiche les 5 premiers pour ne pas surcharger la console
            for etab in etablissements[:5]:
                print(f"   - ID: {etab.get('id_etab')}, Nom: {etab.get('nom')}, Note: {etab.get('rating')}")

            if etablissements:
                return etablissements[0].get('id_etab')
        else:
            print(f"❌ Erreur ! Code de statut : {response.status_code}")
            print(f"   Réponse : {response.text}")
            
    except requests.exceptions.ConnectionError as e:
        print(f"❌ Erreur de connexion : Impossible de joindre l'API à l'adresse {url}.")
    
    return None

def test_get_one_etablissement(etab_id: int):
    """
    Teste l'endpoint pour récupérer les détails d'un établissement spécifique.
    """
    if not etab_id:
        print("\n--- Test 2: Ignoré (aucun ID d'établissement à tester) ---")
        return

    print(f"\n--- Test 2: Récupération des détails pour l'établissement ID: {etab_id} ---")
    url = f"{BASE_URL}/etablissement/{etab_id}"
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            print("✅ Succès ! Les détails complets ont été reçus.")
            etablissement = response.json()
            # Affiche les détails de manière lisible
            print(json.dumps(etablissement, indent=2, ensure_ascii=False))
        else:
            print(f"❌ Erreur ! Code de statut : {response.status_code}")
            print(f"   Réponse : {response.text}")

    except requests.exceptions.ConnectionError as e:
        print(f"❌ Erreur de connexion : Impossible de joindre l'API à l'adresse {url}.")

def test_get_horaire(etab_id: int):
    """
    Teste l'endpoint pour récupérer les horaires d'un établissement.
    """
    if not etab_id:
        print("\n--- Test 3: Ignoré (aucun ID d'établissement à tester) ---")
        return

    print(f"\n--- Test 3: Récupération des horaires pour l'établissement ID: {etab_id} ---")
    url = f"{BASE_URL}/etablissement/horaire/{etab_id}"
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            print("✅ Succès ! Les horaires ont été reçus.")
            horaires = response.json()
            print(f"Nombre de périodes d'ouverture trouvées : {len(horaires)}")
            print(json.dumps(horaires, indent=2))
        else:
            print(f"❌ Erreur ! Code de statut : {response.status_code}")
            print(f"   Réponse : {response.text}")

    except requests.exceptions.ConnectionError as e:
        print(f"❌ Erreur de connexion : Impossible de joindre l'API à l'adresse {url}.")

def test_get_reviews(etab_id: int):
    """
    Teste l'endpoint pour récupérer les avis d'un établissement.
    """
    if not etab_id:
        print("\n--- Test 4: Ignoré (aucun ID d'établissement à tester) ---")
        return

    print(f"\n--- Test 4: Récupération des avis pour l'établissement ID: {etab_id} ---")
    url = f"{BASE_URL}/etablissement/review/{etab_id}"
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            print("✅ Succès ! Les avis ont été reçus.")
            reviews = response.json()
            print(f"Nombre d'avis trouvés : {len(reviews)}")
            
            # Affiche le premier avis pour vérification
            if reviews:
                print("   Exemple d'avis :")
                print(json.dumps(reviews[0], indent=2, ensure_ascii=False))
        else:
            print(f"❌ Erreur ! Code de statut : {response.status_code}")
            print(f"   Réponse : {response.text}")

    except requests.exceptions.ConnectionError as e:
        print(f"❌ Erreur de connexion : Impossible de joindre l'API à l'adresse {url}.")


if __name__ == "__main__":
    # Étape 0: S'assurer que l'API est accessible
    if test_root():
        # Étape 1: Récupérer tous les établissements et obtenir l'ID du premier
        first_etab_id = test_get_all_etablissements()
        
        # Étapes suivantes : Utiliser cet ID pour les tests spécifiques
        if first_etab_id:
            test_get_one_etablissement(first_etab_id)
            test_get_horaire(first_etab_id)
            test_get_reviews(first_etab_id)