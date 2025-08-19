from sqlalchemy.orm import Session, joinedload, selectinload
import models
import traceback

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