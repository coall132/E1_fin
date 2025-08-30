from pydantic import BaseModel, Field
from typing import List, Optional

class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_at: int
    
class OpeningPeriod(BaseModel):
    open_day: Optional[int]
    open_hour: Optional[int]
    open_minute: Optional[int]
    close_day: Optional[int]
    close_hour: Optional[int]
    close_minute: Optional[int]

    class Config: orm_mode = True

class Review(BaseModel):
    original_languageCode: Optional[str]
    original_text: Optional[str]
    publishTime: Optional[str]
    rating: Optional[float]
    relativePublishTimeDescription: Optional[str]
    author: Optional[str]

    class Config: orm_mode = True

class Options(BaseModel):
    allowsDogs: Optional[bool]
    delivery: Optional[bool]
    goodForChildren: Optional[bool]
    goodForGroups: Optional[bool]
    goodForWatchingSports: Optional[bool]
    outdoorSeating: Optional[bool]
    reservable: Optional[bool]
    restroom: Optional[bool]
    servesVegetarianFood: Optional[bool]
    servesBrunch: Optional[bool]
    servesBreakfast: Optional[bool]
    servesDinner: Optional[bool]
    servesLunch: Optional[bool]
    
    class Config: orm_mode = True

class EtablissementBase(BaseModel):
    id_etab: int
    nom: Optional[str]
    adresse: Optional[str]
    rating: Optional[float]

    class Config: orm_mode = True

class Etablissement(EtablissementBase):
    internationalPhoneNumber: Optional[str]
    description: Optional[str]
    websiteUri: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    priceLevel: Optional[str]
    start_price: Optional[float]
    end_price: Optional[float]
    editorialSummary_text: Optional[str]
    google_place_id: Optional[str]
    reviews: List[Review] = Field(default_factory=list)
    options: Optional[Options] = None
    opening_periods: List[OpeningPeriod] = Field(default_factory=list)

    class Config: orm_mode = True

class EtablissementWithOptions(EtablissementBase):
    internationalPhoneNumber: Optional[str]
    description: Optional[str]
    websiteUri: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    priceLevel: Optional[str]
    start_price: Optional[float]
    end_price: Optional[float]
    editorialSummary_text: Optional[str]
    google_place_id: Optional[str]
    options: Optional[Options] = None  # On inclut seulement les options

    class Config:
        orm_mode = True