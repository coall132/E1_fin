import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, input_file_name, regexp_replace, split, element_at
from minio import Minio
from src.database.models import init_db

def ensure_bucket_exists(client, bucket_name):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' created.")
    else:
        print(f"Bucket '{bucket_name}' already exists.")

def main():
    # --- 0. Initialisation de la base de données ---
    # S'assure que toutes les tables sont créées avant de continuer.
    init_db()

    print("--- Starting Spark Job ---")
    
    minio_client = Minio("minio:9000", access_key=os.getenv("AWS_ACCESS_KEY_ID"), secret_key=os.getenv("AWS_SECRET_KEY"), secure=False)
    ensure_bucket_exists(minio_client, "datalake")

    spark = SparkSession.builder \
        .appName("MinIO to Postgres") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_KEY")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    db_url = f"jdbc:postgresql://postgres:5432/{os.getenv('POSTGRES_DB')}"
    db_properties = {"user": os.getenv("POSTGRES_USER"), "password": os.getenv("POSTGRES_PASSWORD"), "driver": "org.postgresql.Driver"}

    # --- 1. Read Raw Data ---
    try:
        # CORRECTION : Lecture plus flexible des fichiers
        google_df = spark.read.option("multiLine", "true").json("s3a://datalake/raw/API_google/")
        
        if google_df.rdd.isEmpty():
            print("No data found in MinIO. Exiting.")
            spark.stop()
            return
        print(f"Found {google_df.count()} records in MinIO.")

        pj_df = spark.read.option("multiLine", "true").json(f"s3a://datalake/raw/page_jaune/") \
            .withColumn("filename_full", element_at(split(input_file_name(), "/"), -1)) \
            .withColumn("filename", regexp_replace(col("filename_full"), ".json", ""))
        
        # Join to get description
        google_df = google_df \
            .withColumn("filename_full", element_at(split(input_file_name(), "/"), -1)) \
            .withColumn("filename", regexp_replace(col("filename_full"), ".json", "")) \
            .join(pj_df.select("filename", "description"), "filename", "left")

    except Exception as e:
        print(f"Error reading from MinIO: {e}. Exiting.")
        spark.stop()
        return
    spark.stop()
    print("--- Spark Job Finished ---")
    # --- 2. Transform and Insert Data for Each Table ---
"""
    # === 2.1 Etablissement ===
    etablissement_df = google_df.select(
        col("displayName.text").alias("nom"),
        col("internationalPhoneNumber"),
        col("formattedAddress").alias("adresse"),
        col("description"),
        col("websiteUri"),
        col("location.latitude").alias("latitude"),
        col("location.longitude").alias("longitude"),
        col("rating"),
        col("priceLevel"),
        col("priceRange.startPrice.units").alias("start_price"),
        col("priceRange.endPrice.units").alias("end_price"),
        col("editorialSummary.text").alias("editorialSummary_text"),
        col("name").alias("google_place_id")
    ).distinct()

    # CORRECTION : Utiliser le mode "ignore" pour éviter les erreurs de duplication
    etablissement_df.write.jdbc(url=db_url, table="etab", mode="ignore", properties=db_properties)
    print(f"Successfully wrote/ignored {etablissement_df.count()} rows to 'etab' table.")

    # === 2.2 Get Generated IDs ===
    etab_with_ids_df = spark.read.jdbc(url=db_url, table="etab", properties=db_properties).select("id_etab", "google_place_id")

    # === 2.3 Options ===
    if "goodForChildren" in google_df.columns:
        options_df = google_df.select(
            col("name").alias("google_place_id"),
            col("allowsDogs"), col("delivery"), col("goodForChildren"), col("goodForGroups"),
            col("goodForWatchingSports"), col("outdoorSeating"), col("reservable"), col("restroom"),
            col("servesVegetarianFood"), col("servesBrunch"), col("servesBreakfast"),
            col("servesDinner"), col("servesLunch")
        ).join(etab_with_ids_df, "google_place_id").drop("google_place_id")
        
        options_df.write.jdbc(url=db_url, table="options", mode="ignore", properties=db_properties)
        print(f"Successfully wrote/ignored {options_df.count()} rows to 'options' table.")

    # === 2.4 Reviews ===
    if "reviews" in google_df.columns:
        reviews_df = google_df.select(col("name").alias("google_place_id"), explode("reviews").alias("review")) \
            .select(
                "google_place_id",
                col("review.originalText.languageCode").alias("original_languageCode"),
                col("review.originalText.text").alias("original_text"),
                col("review.publishTime"),
                col("review.rating"),
                col("review.relativePublishTimeDescription"),
                col("review.authorAttribution.displayName").alias("author")
            ).join(etab_with_ids_df, "google_place_id").drop("google_place_id")
        
        reviews_df.write.jdbc(url=db_url, table="reviews", mode="append", properties=db_properties)
        print(f"Successfully wrote {reviews_df.count()} new rows to 'reviews' table.")

    # === 2.5 Opening Periods ===
    if "regularOpeningHours.periods" in google_df.columns:
        opening_periods_df = google_df.select(col("name").alias("google_place_id"), explode("regularOpeningHours.periods").alias("period")) \
            .select(
                "google_place_id",
                col("period.open.day").alias("open_day"),
                col("period.open.hour").alias("open_hour"),
                col("period.open.minute").alias("open_minute"),
                col("period.close.day").alias("close_day"),
                col("period.close.hour").alias("close_hour"),
                col("period.close.minute").alias("close_minute")
            ).join(etab_with_ids_df, "google_place_id").drop("google_place_id")
            
        opening_periods_df.write.jdbc(url=db_url, table="opening_period", mode="append", properties=db_properties)
        print(f"Successfully wrote {opening_periods_df.count()} new rows to 'opening_period' table.")
"""

if __name__ == "__main__":
    main()