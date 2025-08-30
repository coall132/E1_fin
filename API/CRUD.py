from sqlalchemy.orm import Session, joinedload, selectinload
import models
import traceback
from jose import JWTError, jwt
from fastapi.security import OAuth2PasswordBearer, APIKeyHeader
from datetime import datetime, timedelta, timezone
from typing import List, Optional
import os
from fastapi import FastAPI, Depends, HTTPException, Security, status, Query


API_STATIC_KEY = os.getenv("API_STATIC_KEY", "coall")  # pour échanger contre un token
JWT_SECRET = os.getenv("JWT_SECRET", "coall")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "15"))

api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)  # pour /auth/token uniquement
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")

def get_etablissement(db: Session, etab_id: int):
    etablissement = db.query(models.Etablissement).filter(models.Etablissement.id_etab == etab_id).first()
    print(etablissement)
    if not etablissement:
        return None
    db.query(models.Options).filter(models.Options.id_etab == etab_id).first()
    return etablissement

def get_etablissements(db: Session, skip: int = 0, limit: int = 100):
    """
    Récupère une liste d'établissements avec pagination.
    Ne charge pas les relations pour que la liste soit plus légère.
    """
    return db.query(models.Etablissement).offset(skip).limit(limit).all()

def get_horaire(db: Session, etab_id: int):
    return db.query(models.OpeningPeriod).filter(models.OpeningPeriod.id_etab == etab_id).all()

def get_reviews(db: Session, etab_id: int):
    return db.query(models.Review).filter(models.Review.id_etab == etab_id).all()

def create_access_token(subject: str, expires_delta: Optional[timedelta] = None):
    expire = datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode = {"sub": subject, "exp": expire}
    encoded_jwt = jwt.encode(to_encode, JWT_SECRET, algorithm=ALGORITHM)
    return encoded_jwt, int(expire.timestamp())

async def get_current_subject(token: str = Depends(oauth2_scheme)) -> str:
    credentials_exc = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Token invalide ou expiré.",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[ALGORITHM])
        sub: Optional[str] = payload.get("sub")
        if sub is None:
            raise credentials_exc
        return sub
    except JWTError:
        raise credentials_exc