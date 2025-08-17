# src/database/models.py
import os
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, Text, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

Base = declarative_base()

class Etablissement(Base):
    __tablename__ = "etab"

    id_etab = Column(Integer, primary_key=True, autoincrement=True)

    nom = Column(String(40))
    internationalPhoneNumber = Column(String(10))
    adresse = Column(Text)
    description = Column(Text)
    websiteUri = Column(Text)
    latitude = Column(Float)
    longitude = Column(Float)
    rating = Column(Float)
    priceLevel = Column(String(30))
    start_price = Column(Float)
    end_price = Column(Float)
    editorialSummary_text = Column(Text)
    directionsUri = Column(Text)
    primaryTypeDisplayName_text = Column(Text)

    # Relations
    options = relationship("Options", back_populates="etab", uselist=False)
    reviews = relationship("Review", back_populates="etab", cascade="all, delete-orphan")
    opening_periods = relationship("OpeningPeriod", back_populates="etab", cascade="all, delete-orphan")

class Options(Base):
    __tablename__ = "options"

    id_etab = Column(Integer, ForeignKey("etab.id_etab"), primary_key=True)

    accessibilityOptions = Column(Text)
    allowsDogs = Column(Boolean)
    delivery = Column(Boolean)
    goodForChildren = Column(Boolean)
    goodForGroups = Column(Boolean)
    goodForWatchingSports = Column(Boolean)
    outdoorSeating = Column(Boolean)
    parkingOptions = Column(Text)
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
    translated_languageCode = Column(String(10))
    translated_text = Column(Text)
    flagContentUri = Column(Text)
    googleMapsUri = Column(Text)
    review_name = Column(Text)
    publishTime = Column(String(50))
    rating = Column(Float)
    relativePublishTimeDescription = Column(String(100))

    # Relation inverse
    etab = relationship("Etablissement", back_populates="reviews")

class OpeningPeriod(Base):
    __tablename__ = "opening_period"

    id_period = Column(Integer, primary_key=True, autoincrement=True)
    id_etab = Column(Integer, ForeignKey("etab.id_etab"))

    open_day = Column(Integer)
    open_hour = Column(Integer)
    open_minute = Column(Integer)
    open_truncated = Column(Boolean)

    close_day = Column(Integer)
    close_hour = Column(Integer)
    close_minute = Column(Integer)
    close_truncated = Column(Boolean)

    # Relation inverse
    etab = relationship("Etablissement", back_populates="opening_periods")

# Connection to the database using environment variable
DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    print("Initializing database schema...")
    Base.metadata.create_all(bind=engine)
    print("Database schema initialized.")

if __name__ == "__main__":
    init_db()