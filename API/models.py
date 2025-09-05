from sqlalchemy import Column, Integer, String, Float, Boolean, ForeignKey, func, DateTime
from sqlalchemy.orm import relationship
from database import Base

class Etablissement(Base):
    __tablename__ = "etab"
    id_etab = Column(Integer, primary_key=True, index=True)
    nom = Column(String, index=True)
    internationalPhoneNumber = Column(String, nullable=True)
    adresse = Column(String, nullable=True)
    description = Column(String, nullable=True)
    websiteUri = Column(String, nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    rating = Column(Float, nullable=True)
    priceLevel = Column(String, nullable=True)
    start_price = Column(Float, nullable=True)
    end_price = Column(Float, nullable=True)
    editorialSummary_text = Column(String, nullable=True)
    google_place_id = Column(String, unique=True, index=True)

    reviews = relationship("Review", back_populates="etablissement")
    options = relationship("Options", back_populates="etablissement", uselist=False)
    opening_periods = relationship("OpeningPeriod", back_populates="etablissement")

class Options(Base):
    __tablename__ = "options"
    id_etab = Column(Integer, ForeignKey("etab.id_etab"), primary_key=True)
    allowsDogs = Column(Boolean, nullable=True)
    delivery = Column(Boolean, nullable=True)
    goodForChildren = Column(Boolean, nullable=True)
    goodForGroups = Column(Boolean, nullable=True)
    goodForWatchingSports = Column(Boolean, nullable=True)
    outdoorSeating = Column(Boolean, nullable=True)
    reservable = Column(Boolean, nullable=True)
    restroom = Column(Boolean, nullable=True)
    servesVegetarianFood = Column(Boolean, nullable=True)
    servesBrunch = Column(Boolean, nullable=True)
    servesBreakfast = Column(Boolean, nullable=True)
    servesDinner = Column(Boolean, nullable=True)
    servesLunch = Column(Boolean, nullable=True)

    etablissement = relationship("Etablissement", back_populates="options")

class Review(Base):
    __tablename__ = "reviews"
    id_review = Column(Integer, primary_key=True, index=True)
    id_etab = Column(Integer, ForeignKey("etab.id_etab"))
    original_languageCode = Column(String, nullable=True)
    original_text = Column(String, nullable=True)
    publishTime = Column(String, nullable=True)
    rating = Column(Float, nullable=True)
    relativePublishTimeDescription = Column(String, nullable=True)
    author = Column(String, nullable=True)

    etablissement = relationship("Etablissement", back_populates="reviews")

class OpeningPeriod(Base):
    __tablename__ = "opening_period"
    id_period = Column(Integer, primary_key=True, index=True)
    id_etab = Column(Integer, ForeignKey("etab.id_etab"))
    open_day = Column(Integer, nullable=True)
    open_hour = Column(Integer, nullable=True)
    open_minute = Column(Integer, nullable=True)
    close_day = Column(Integer, nullable=True)
    close_hour = Column(Integer, nullable=True)
    close_minute = Column(Integer, nullable=True)
    
    etablissement = relationship("Etablissement", back_populates="opening_periods")

class Tombstone(Base):
    __tablename__ = "tombstone"
    key = Column(String, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())