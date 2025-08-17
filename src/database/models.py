import os
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, Text, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

Base = declarative_base()

class Etablissement(Base):
    __tablename__ = "etab"
    id_etab = Column(Integer, primary_key=True, autoincrement=True)
    nom = Column(String(255))
    internationalPhoneNumber = Column(String(30))
    adresse = Column(Text)
    description = Column(Text) # Vient des Pages Jaunes
    websiteUri = Column(Text)
    latitude = Column(Float)
    longitude = Column(Float)
    rating = Column(Float)
    priceLevel = Column(String(30))
    start_price = Column(Float)
    end_price = Column(Float)
    editorialSummary_text = Column(Text)
    google_place_id = Column(String(255), unique=True) # Identifiant unique de Google
    
    # Relations
    options = relationship("Options", back_populates="etab", uselist=False, cascade="all, delete-orphan")
    reviews = relationship("Review", back_populates="etab", cascade="all, delete-orphan")
    opening_periods = relationship("OpeningPeriod", back_populates="etab", cascade="all, delete-orphan")

class Options(Base):
    __tablename__ = "options"
    id_etab = Column(Integer, ForeignKey("etab.id_etab"), primary_key=True)
    allowsDogs = Column(Boolean)
    delivery = Column(Boolean)
    goodForChildren = Column(Boolean)
    goodForGroups = Column(Boolean)
    goodForWatchingSports = Column(Boolean)
    outdoorSeating = Column(Boolean)
    reservable = Column(Boolean)
    restroom = Column(Boolean)
    servesVegetarianFood = Column(Boolean)
    servesBrunch = Column(Boolean)
    servesBreakfast = Column(Boolean)
    servesDinner = Column(Boolean)
    servesLunch = Column(Boolean)
    # Relation inverse
    etab = relationship("Etablissement", back_populates="options")

class Review(Base):
    __tablename__ = "reviews"
    id_review = Column(Integer, primary_key=True, autoincrement=True)
    id_etab = Column(Integer, ForeignKey("etab.id_etab"))
    original_languageCode = Column(String(10))
    original_text = Column(Text)
    publishTime = Column(String(50))
    rating = Column(Float)
    relativePublishTimeDescription = Column(String(100))
    author = Column(String(255))
    # Relation inverse
    etab = relationship("Etablissement", back_populates="reviews")

class OpeningPeriod(Base):
    __tablename__ = "opening_period"
    id_period = Column(Integer, primary_key=True, autoincrement=True)
    id_etab = Column(Integer, ForeignKey("etab.id_etab"))
    open_day = Column(Integer)
    open_hour = Column(Integer)
    open_minute = Column(Integer)
    close_day = Column(Integer)
    close_hour = Column(Integer)
    close_minute = Column(Integer)
    # Relation inverse
    etab = relationship("Etablissement", back_populates="opening_periods")

def init_db():
    """Crée toutes les tables dans la base de données si elles n'existent pas."""
    # Utilise l'URL de la BDD pour la communication interne à Docker
    DATABASE_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@postgres:5432/{os.getenv('POSTGRES_DB')}"
    
    if not DATABASE_URL:
        raise ValueError("Les variables d'environnement de la base de données ne sont pas définies.")
        
    engine = create_engine(DATABASE_URL)
    
    print("Vérification et création des tables si nécessaire...")
    Base.metadata.create_all(bind=engine)
    print("Les tables sont prêtes.")

if __name__ == "__main__":
    init_db()