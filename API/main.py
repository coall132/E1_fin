from fastapi import FastAPI, Depends, HTTPException, Security, status, Query
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List, Optional
import os

import CRUD
import models
import schema
from database import engine, get_db

app = FastAPI(
    title="API Restaurants",
    description="API pour récupérer les informations sur les établissements.",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ALLOW_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

API_STATIC_KEY = os.getenv("API_STATIC_KEY", "coall")
api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)

@app.post("/auth/token", response_model=schema.TokenOut, tags=["Auth"])
async def issue_token(api_key: Optional[str] = Security(api_key_header)):
    if api_key != API_STATIC_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Clé API invalide ou manquante.",
            headers={"WWW-Authenticate": "APIKey"},
        )
    token, exp_ts = CRUD.create_access_token(subject="api-client")
    return schema.TokenOut(access_token=token, expires_at=exp_ts)

@app.get("/")
def read_root():
    return {"message": "Bienvenue sur l'API des restaurants. Allez sur /docs pour voir les endpoints."}

# --- Endpoints protégés par Bearer JWT ---
@app.get("/etablissements/", response_model=List[schema.EtablissementBase], tags=["Établissements"])
def read_etablissements(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    _sub: str = Depends(CRUD.get_current_subject),  # <-- vérif Bearer JWT
):
    return CRUD.get_etablissements(db, skip=skip, limit=limit)

@app.get("/etablissement/{etab_id}", response_model=schema.EtablissementWithOptions, tags=["Établissement"])
def read_etablissement(
    etab_id: int,
    db: Session = Depends(get_db),
    _sub: str = Depends(CRUD.get_current_subject),
):
    etab = CRUD.get_etablissement(db, etab_id=etab_id)
    if not etab:
        raise HTTPException(status_code=404, detail="Établissement introuvable.")
    return etab

@app.get("/etablissement/horaire/{etab_id}", response_model=List[schema.OpeningPeriod], tags=["Horaire"])
def read_etablissement_horaire(
    etab_id: int,
    db: Session = Depends(get_db),
    _sub: str = Depends(CRUD.get_current_subject),
):
    return CRUD.get_horaire(db, etab_id=etab_id)

@app.get("/etablissement/review/{etab_id}", response_model=List[schema.Review], tags=["Review"])
def read_etablissement_reviews(
    etab_id: int,
    db: Session = Depends(get_db),
    _sub: str = Depends(CRUD.get_current_subject),
):
    return CRUD.get_reviews(db, etab_id=etab_id)