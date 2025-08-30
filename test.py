# test_api.py
import requests, json

BASE_URL = "http://localhost:8000"
API_KEY = "coall"  # doit matcher API_STATIC_KEY côté serveur

def get_token():
    url = f"{BASE_URL}/auth/token"
    r = requests.post(url, headers={"X-API-KEY": API_KEY})
    if r.status_code != 200:
        raise SystemExit(f"Auth échouée ({r.status_code}) -> {r.text}")
    data = r.json()
    return data["access_token"]

def test_root():
    print("--- Test 0: / ---")
    r = requests.get(f"{BASE_URL}/")
    print(r.status_code, r.json())

def auth_headers():
    return {"Authorization": f"Bearer {get_token()}"}

def test_get_all_etablissements():
    print("\n--- Test 1: /etablissements/ ---")
    r = requests.get(f"{BASE_URL}/etablissements/?skip=0&limit=100", headers=auth_headers())
    print(r.status_code)
    if r.status_code == 200:
        items = r.json()
        print(f"Nb: {len(items)}")
        for etab in items[:5]:
            print(f"- ID:{etab.get('id_etab')} Nom:{etab.get('nom')} Note:{etab.get('rating')}")
        return items[0].get("id_etab") if items else None
    else:
        print(r.text)

def test_get_one_etablissement(etab_id):
    if not etab_id: return
    print(f"\n--- Test 2: /etablissement/{etab_id} ---")
    r = requests.get(f"{BASE_URL}/etablissement/{etab_id}", headers=auth_headers())
    print(r.status_code)
    print(json.dumps(r.json(), indent=2, ensure_ascii=False) if r.status_code==200 else r.text)

def test_get_horaire(etab_id):
    if not etab_id: return
    print(f"\n--- Test 3: /etablissement/horaire/{etab_id} ---")
    r = requests.get(f"{BASE_URL}/etablissement/horaire/{etab_id}", headers=auth_headers())
    print(r.status_code)
    print(json.dumps(r.json(), indent=2) if r.status_code==200 else r.text)

def test_get_reviews(etab_id):
    if not etab_id: return
    print(f"\n--- Test 4: /etablissement/review/{etab_id} ---")
    r = requests.get(f"{BASE_URL}/etablissement/review/{etab_id}", headers=auth_headers())
    print(r.status_code)
    print(json.dumps(r.json()[0], indent=2, ensure_ascii=False) if r.status_code==200 and r.json() else r.text)

if __name__ == "__main__":
    test_root()
    first_id = test_get_all_etablissements()
    test_get_one_etablissement(first_id)
    test_get_horaire(first_id)
    test_get_reviews(first_id)
