from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from bs4 import BeautifulSoup
import time
import re
import random

def find_nb_page(soup):
    try:
        pagination_span = soup.find('span', id="SEL-compteur")
        if not pagination_span: return 1
        derniere_ligne = pagination_span.text.strip().split('\n')[-1]
        nb_pages = re.findall(r'\d+', derniere_ligne)
        return int(nb_pages[-1]) if nb_pages else 1
    except Exception:
        return 1

def find_num_addr(soup, liste_resultat):
    main_results = soup.find('section', id="listResults")
    if not main_results: return liste_resultat

    for res in main_results.find_all('li', class_='bi'):
        try:
            name_tag = res.select_one('h3')
            addr_tag = res.select_one('.bi-address')
            if not name_tag or not addr_tag: continue
            
            name = name_tag.text.strip()
            
            full_address_text = addr_tag.text.strip()
            addr = full_address_text.split("Voir le plan")[0].strip()

            num_fin = [num.text.strip() for num in res.select('.number-contact span') if num.text.strip()]
            description_tag = res.select_one('.bi-description')
            description = description_tag.text.strip() if description_tag else None

            liste_resultat.append({
                "nom": name, "adresse": addr,
                "tel": num_fin if num_fin else None, "description": description
            })
        except Exception:
            continue
    return liste_resultat

def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--headless=new")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/5.0 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")
    return webdriver.Chrome(options=options)

def scrapping_page_jaune(activite, dep):
    liste_resultat = []
    url_base = f"https://www.pagesjaunes.fr/annuaire/chercherlespros?quoiqui={activite}&ou={dep}"
    driver = init_driver()
    try:
        driver.get(url_base)
        time.sleep(random.uniform(3, 5))
        try:
            WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.ID, "didomi-notice-agree-button"))).click()
            print("Bandeau cookie accepté.")
        except Exception:
            print("Pas de bandeau cookie trouvé.")
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        nb_page = find_nb_page(soup)
        print(f"{nb_page} page(s) à scraper pour '{activite}'.")
        for page in range(1, min(nb_page + 1, nb_page)):
            if page > 1:
                driver.get(f"{url_base}&page={page}")
                time.sleep(random.uniform(2, 4))
            
            soup = BeautifulSoup(driver.page_source, "html.parser")
            find_num_addr(soup, liste_resultat)
            print(f"✅ Page {page} traitée. {len(liste_resultat)} résultats au total.")
    except Exception as e:
        print(f"Erreur pendant le scraping : {e}")
    finally:
        driver.quit()
    return liste_resultat