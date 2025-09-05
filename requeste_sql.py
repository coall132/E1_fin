import os
from sqlalchemy import create_engine, text

engine = create_engine(os.getenv("DATABASE_URL"))

sql1 = """
WITH avg_rating AS (
  SELECT AVG(rating) AS avg_rating
  FROM "etab"
  WHERE rating IS NOT NULL
    AND rating = rating  )
UPDATE "etab"
SET rating = avg_rating.avg_rating
FROM avg_rating
WHERE rating IS NULL
   OR rating <> rating;
"""

sql2 = """
UPDATE "options" SET
    "allowsDogs"            = COALESCE("allowsDogs", FALSE),
    "delivery"              = COALESCE("delivery", FALSE),
    "goodForChildren"       = COALESCE("goodForChildren", FALSE),
    "goodForGroups"         = COALESCE("goodForGroups", FALSE),
    "goodForWatchingSports" = COALESCE("goodForWatchingSports", FALSE),
    "outdoorSeating"        = COALESCE("outdoorSeating", FALSE),
    "reservable"            = COALESCE("reservable", FALSE),
    "restroom"              = COALESCE("restroom", FALSE),
    "servesVegetarianFood"  = COALESCE("servesVegetarianFood", FALSE),
    "servesBrunch"          = COALESCE("servesBrunch", FALSE),
    "servesBreakfast"       = COALESCE("servesBreakfast", FALSE),
    "servesDinner"          = COALESCE("servesDinner", FALSE),
    "servesLunch"           = COALESCE("servesLunch", FALSE);
"""

with engine.begin() as conn:  # transaction + commit auto si OK
    conn.execute(text(sql1))
    conn.execute(text(sql2))

print("Mises à jour effectuées.")