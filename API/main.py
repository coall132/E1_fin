from fastapi import FastAPI, Depends, HTTPException, Security
from fastapi.security import APIKeyHeader
from sqlalchemy.orm import Session
from typing import List, Optional

import CRUD
import models
import schema
from database import engine, get_db

api_key_header = APIKeyHeader(name="X-API-KEY")
SECRET_KEY = "coall"

async def get_api_key(api_key: str = Security(api_key_header)):
    if api_key == SECRET_KEY:
        return api_key
    else:
        raise HTTPException(
            status_code=403,
            detail="Clé API invalide ou manquante.",
        )

models.Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="API Restaurants",
    description="API pour récupérer les informations sur les établissements.",
    version="1.0.0"
)

@app.get("/")
def read_root():
    return {"message": "Bienvenue sur l'API des restaurants. Allez sur /docs pour voir les endpoints."}

@app.get("/etablissements/", response_model=List[schema.EtablissementBase], tags=["Établissements"], dependencies=[Depends(get_api_key)])
def read_etablissements(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    etablissements = CRUD.get_etablissements(db, skip=skip, limit=limit)
    return etablissements

@app.get("/etablissement/{etab_id}", response_model=schema.EtablissementWithOptions, tags=["Établissement"], dependencies=[Depends(get_api_key)])
def read_etablissement(etab_id: int, db: Session = Depends(get_db)):
    db_etablissement = CRUD.get_etablissement(db, etab_id=etab_id)
    return db_etablissement

@app.get("/etablissement/horaire/{etab_id}", response_model=List[schema.OpeningPeriod], tags=["Horaire"], dependencies=[Depends(get_api_key)])
def read_etablissement_horaire(etab_id: int, db: Session = Depends(get_db)):
    db_horaire = CRUD.get_horaire(db, etab_id=etab_id)
    return db_horaire

@app.get("/etablissement/review/{etab_id}", response_model=List[schema.Review], tags=["Review"], dependencies=[Depends(get_api_key)])
def read_etablissement_reviews(etab_id: int, db: Session = Depends(get_db)):
    db_reviews = CRUD.get_reviews(db, etab_id=etab_id)
    return db_reviews