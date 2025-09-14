# E1_fin

Pour que tout fonctionne plusieurs étape sont à prévoir :
    1 - Pour commencer créer vous une clé api Google Map, pour cela  :
        A - Aller sur la console Google Cloud
        B - Créer un projet Google
        C - Activer l'API Google Places : APIs & Services → Dashboard → Enable APIs and Services
        D - Activer la facturation
        E - Créer la clé API : APIs & Services → Credentials → Create credentials → API key

    2 - Ensuite cloner le repo github en local ou sur un serveur

    3 - Aller dans le dossier cloner et créer un fichier .env contenant :
        GOOGLE_KEY= (votre clé API)
        AWS_ACCESS_KEY_ID= (votre user minIO)
        AWS_SECRET_ACCESS_KEY= (votre mdp MinIO)
        POSTGRES_USER=(votre user postgres)
        POSTGRES_PASSWORD=(votre mdp postgres)
        POSTGRES_DB= mydb
        DATABASE_URL='postgresql://(votre user postgres):(votre mdp postgres)@localhost:5433/mydb'
        SECRET=(clé pour le hashage)

    4 - Installer Docker Desktop

    5 - Ensuite pour profiter du service :
        A - lancer le docker avec : docker compose up --build -d
        B - lancer (à l'exterieur du docker) et dans le dossier extraction (du repo) la commande : python -m extract_data.py
        C - lancer la commande (à la racine du dossier) : docker compose run --rm spark-app

Voila profiter des données

Pour acceder à l'API aller sur l'endpoint 8000

