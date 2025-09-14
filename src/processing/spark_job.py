import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, input_file_name, regexp_replace, split, element_at
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, BooleanType, LongType, FloatType, IntegerType
from minio import Minio
from src.database.models import init_db
from functools import reduce
import API.utils

# === Schémas ===
google_api_schema = StructType([
    StructField("name", StringType(), True),
    StructField("internationalPhoneNumber", StringType(), True),
    StructField("formattedAddress", StringType(), True),
    StructField("websiteUri", StringType(), True),
    StructField("location", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True)
    ]), True),
    StructField("rating", DoubleType(), True),
    StructField("priceLevel", StringType(), True),
    StructField("priceRange", StructType([
        StructField("startPrice", StructType([StructField("units", StringType(), True)]), True),
        StructField("endPrice", StructType([StructField("units", StringType(), True)]), True)
    ]), True),
    StructField("editorialSummary", StructType([StructField("text", StringType(), True)]), True),
    StructField("displayName", StructType([StructField("text", StringType(), True)]), True),
    StructField("allowsDogs", BooleanType(), True),
    StructField("delivery", BooleanType(), True),
    StructField("goodForChildren", BooleanType(), True),
    StructField("goodForGroups", BooleanType(), True),
    StructField("goodForWatchingSports", BooleanType(), True),
    StructField("outdoorSeating", BooleanType(), True),
    StructField("reservable", BooleanType(), True),
    StructField("restroom", BooleanType(), True),
    StructField("servesVegetarianFood", BooleanType(), True),
    StructField("servesBrunch", BooleanType(), True),
    StructField("servesBreakfast", BooleanType(), True),
    StructField("servesDinner", BooleanType(), True),
    StructField("servesLunch", BooleanType(), True),
    StructField("reviews", ArrayType(StructType([
        StructField("originalText", StructType([
            StructField("languageCode", StringType(), True),
            StructField("text", StringType(), True)
        ]), True),
        StructField("publishTime", StringType(), True),
        StructField("rating", DoubleType(), True),
        StructField("relativePublishTimeDescription", StringType(), True),
        StructField("authorAttribution", StructType([StructField("displayName", StringType(), True)]), True)
    ])), True),
    StructField("regularOpeningHours", StructType([
        StructField("periods", ArrayType(StructType([
            StructField("open", StructType([
                StructField("day", LongType(), True),
                StructField("hour", LongType(), True),
                StructField("minute", LongType(), True)
            ]), True),
            StructField("close", StructType([
                StructField("day", LongType(), True),
                StructField("hour", LongType(), True),
                StructField("minute", LongType(), True)
            ]), True)
        ])), True)
    ]), True)
])

pj_schema = StructType([
    StructField("nom", StringType(), True),
    StructField("adresse", StringType(), True),
    StructField("tel", ArrayType(StringType()), True),
    StructField("description", StringType(), True)
])


def ensure_bucket_exists(client, bucket_name):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' created.")
    else:
        print(f"Bucket '{bucket_name}' already exists.")


def main():
    init_db()
    print("--- Starting Spark Job ---")
    minio_client = Minio(
        "minio:9000",
        access_key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        secure=False,
    )
    ensure_bucket_exists(minio_client, "datalake")
    # --- ÉTAPE DE DIAGNOSTIC ---
    print("--- Listing objects in MinIO directly ---")
    try:
        objects = minio_client.list_objects(
            "datalake", prefix="raw/API_google/", recursive=True
        )
        object_list = [obj.object_name for obj in objects]
        if not object_list:
            print("MinIO client found NO objects in raw/API_google/")
        else:
            print(f"MinIO client found {len(object_list)} objects:")
    except Exception as e:
        print(f"Error listing objects with MinIO client: {e}")
    print("-----------------------------------------")
    # --- FIN DE L'ÉTAPE DE DIAGNOSTIC ---

    spark = (
        SparkSession.builder.appName("MinIO to Postgres")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    db_url = f"jdbc:postgresql://postgres:5432/{os.getenv('POSTGRES_DB')}"
    db_properties = {
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver",
        "tcpKeepAlive": "true",
    }

    # --- 1. Read Raw Data ---
    try:
        # Lecture des données Google
        google_df = (
            spark.read.option("multiLine", "true")
            .option("recursiveFileLookup", "true")
            .schema(google_api_schema)
            .json("s3a://datalake/raw/API_google/")
        )

        if google_df.rdd.isEmpty():
            print("No data found in MinIO. Exiting.")
            spark.stop()
            return
        print(f"Found {google_df.count()} records in MinIO.")

        # Lecture Page Jaune et jointure pour récupérer la description
        pj_df = (
            spark.read.option("multiLine", "true")
            .option("recursiveFileLookup", "true")
            .schema(pj_schema)
            .json("s3a://datalake/raw/page_jaune/")
            .withColumn("filename_full", element_at(split(input_file_name(), "/"), -1))
            .withColumn("filename", regexp_replace(col("filename_full"), ".json", ""))
        )

        google_df = (
            google_df.withColumn(
                "filename_full", element_at(split(input_file_name(), "/"), -1)
            )
            .withColumn("filename", regexp_replace(col("filename_full"), ".json", ""))
            .join(pj_df.select("filename", "description"), "filename", "left")
        )
    except Exception as e:
        print(f"Error reading from MinIO: {e}. Exiting.")
        spark.stop()
        return

    # --- 2. Transform and Insert Data for Each Table ---
    # === 2.1 Etablissement ===
    etablissement_df = (
        google_df.select(
            col("displayName.text").alias("nom"),
            col("internationalPhoneNumber"),
            col("formattedAddress").alias("adresse"),
            col("description"),
            col("websiteUri"),
            col("location.latitude").cast(FloatType()).alias("latitude"),
            col("location.longitude").cast(FloatType()).alias("longitude"),
            col("rating").cast(FloatType()),
            col("priceLevel"),
            col("priceRange.startPrice.units").cast(FloatType()).alias("start_price"),
            col("priceRange.endPrice.units").cast(FloatType()).alias("end_price"),
            col("editorialSummary.text").alias("editorialSummary_text"),
            col("name").alias("google_place_id"),
        )
        .dropDuplicates(["google_place_id"])
    )

    

    # --- Logique anti-doublons ---
    print("Lecture des IDs existants dans la table 'etab'...")
    try:
        existing_ids_df = spark.read.jdbc(
            url=db_url,
            table="(select google_place_id from etab) as et",
            properties=db_properties,
        )
    except Exception as e:
        print("La table 'etab' est probablement vide. On continue...")
        existing_ids_df = spark.createDataFrame([], StructType([StructField("google_place_id", StringType(), False)]))

    to_insert_df = etablissement_df.join(
        existing_ids_df, on="google_place_id", how="left_anti"
    )

    print("Tentative d'insertion des nouvelles lignes dans 'etab'...")
    to_insert_df.write.jdbc(
        url=db_url, 
        table="etab", 
        mode="append", 
        properties=db_properties
    )
    
    inserted_count = to_insert_df.count()
    print(f"✅ {inserted_count} nouvelle(s) ligne(s) insérée(s) dans 'etab'.")

    # --- FIN DE LA LOGIQUE ---
    print("--- Vérification des données dans la table 'etab' depuis Spark ---")
    etab_from_db_df = spark.read.jdbc(url=db_url, table="etab", properties=db_properties)
    print(f"Il y a maintenant {etab_from_db_df.count()} lignes au total dans la table 'etab'.")

    # === 2.2 Get Generated IDs ===
    etab_with_ids_df = etab_from_db_df.select("id_etab", "google_place_id")

    # === 2.3 Options ===
    print("Traitement de la table 'options'...")
    option_cols = [
        "allowsDogs", "delivery", "goodForChildren", "goodForGroups",
        "goodForWatchingSports", "outdoorSeating", "reservable", "restroom",
        "servesVegetarianFood", "servesBrunch", "servesBreakfast",
        "servesDinner", "servesLunch",
    ]
    existing_option_cols = [c for c in option_cols if c in google_df.columns]

    if existing_option_cols:
        options_df = google_df.select(
            col("name").alias("google_place_id"), *[col(c) for c in existing_option_cols]
        )
        filter_condition = reduce(
            lambda a, b: a | b, [col(c).isNotNull() for c in existing_option_cols]
        )
        options_df = options_df.filter(filter_condition)

        if not options_df.rdd.isEmpty():
            options_to_insert_base = options_df.join(
                etab_with_ids_df, "google_place_id"
            ).drop("google_place_id")

            # --- Logique anti-doublons pour 'options' ---
            try:
                existing_options_df = spark.read.jdbc(
                    url=db_url, table="(select id_etab from options) as opt", properties=db_properties
                )
            except Exception:
                print("La table 'options' est probablement vide. On continue...")
                existing_options_df = spark.createDataFrame([], StructType([StructField("id_etab", IntegerType(), False)]))
            
            final_options_to_insert = options_to_insert_base.join(
                existing_options_df, on="id_etab", how="left_anti"
            )
            # --- Fin de la logique anti-doublons ---

            inserted_count = final_options_to_insert.count()
            if inserted_count > 0:
                final_options_to_insert.write.jdbc(
                    url=db_url, table="options", mode="append", properties=db_properties
                )
                print(f"✅ {inserted_count} nouvelle(s) ligne(s) insérée(s) dans 'options'.")
            else:
                print("Aucune nouvelle option à insérer.")
        else:
            print("Aucune ligne avec des données d'options trouvée.")
    else:
        print("Aucune colonne relative aux options trouvée dans les données source.")

    # === 2.4 Reviews ===
    try:
        tombstone_df = (
            spark.read.jdbc(url=db_url, table="tombstone", properties=db_properties)
            .select("key").distinct()
        )
        print(f"Tombstones chargées: {tombstone_df.count()} clés.")
    except Exception:
        print("Table 'tombstone' absente (pas de blocage RGPD).")
        tombstone_df = spark.createDataFrame(
            [], StructType([StructField("key", StringType(), True)])
        )

    print("Traitement de la table 'reviews'...")
    if "reviews" in google_df.columns:
        # 1) Aplatir + calculer la clé hash (author+text)
        reviews_raw_df = (
            google_df
            .select(col("name").alias("google_place_id"), explode("reviews").alias("review"))
            .select(
                "google_place_id",
                col("review.originalText.languageCode").alias("original_languageCode"),
                col("review.originalText.text").alias("original_text"),
                col("review.publishTime"),
                col("review.rating").cast(FloatType()).alias("rating"),
                col("review.relativePublishTimeDescription"),
                col("review.authorAttribution.displayName").alias("author"),
            )
            # UDF déjà dispo chez toi: API.utils.make_review_key(author, original_text)
            .withColumn("key", API.utils.make_review_key(col("author"), col("original_text")))
        )

        # 2) Exclure les avis tombstonés (left_anti sur la clé)
        reviews_raw_df = reviews_raw_df.join(tombstone_df, on="key", how="left_anti")

        # 3) Ajouter la FK puis DROP l'auteur clair et RENOMMER key -> author
        reviews_df = (
            reviews_raw_df
            .join(etab_with_ids_df, "google_place_id")
            .drop("google_place_id")
            .drop("author")                      # on supprime l'auteur en clair
            .withColumnRenamed("key", "author")  # on écrit le hash dans la colonne author
        )

        # 4) Anti-doublons sur (id_etab, author(hash), publishTime)
        unique_keys = ["id_etab", "author", "publishTime"]
        try:
            existing_reviews_df = spark.read.jdbc(
                url=db_url,
                table=f"(select {', '.join(unique_keys)} from reviews) as rev",
                properties=db_properties,
            )
        except Exception:
            existing_reviews_df = spark.createDataFrame([], reviews_df.select(*unique_keys).schema)

        final_reviews_to_insert = reviews_df.join(existing_reviews_df, on=unique_keys, how="left_anti")
        final_reviews_to_insert = final_reviews_to_insert.dropDuplicates(unique_keys)
        
        # 5) Insertion
        inserted_count = final_reviews_to_insert.count()
        if inserted_count > 0:
            final_reviews_to_insert.write.jdbc(
                url=db_url, table="reviews", mode="append", properties=db_properties
            )
            print(f"✅ {inserted_count} nouvelle(s) ligne(s) insérée(s) dans 'reviews'.")
        else:
            print("Aucun nouvel avis à insérer (après tombstone & anti-doublon).")

    # === 2.5 Opening Periods ===
    if "regularOpeningHours" in google_df.columns:
        print("Traitement des périodes d'ouverture...")
        hours_to_process = google_df.select(
            col("name").alias("google_place_id"),
            col("regularOpeningHours")
        ).filter(col("regularOpeningHours.periods").isNotNull())

        hours_with_fk = hours_to_process.join(
            etab_with_ids_df,
            "google_place_id",
            "inner"
        )

        opening_periods_df = (
            hours_with_fk.select(
                "id_etab",
                explode("regularOpeningHours.periods").alias("period")
            )
            .select(
                col("id_etab"),
                col("period.open.day").cast(IntegerType()).alias("open_day"),
                col("period.open.hour").cast(IntegerType()).alias("open_hour"),
                col("period.open.minute").cast(IntegerType()).alias("open_minute"),
                col("period.close.day").cast(IntegerType()).alias("close_day"),
                col("period.close.hour").cast(IntegerType()).alias("close_hour"),
                col("period.close.minute").cast(IntegerType()).alias("close_minute"),
            )
        )
        
        # --- Logique anti-doublons pour 'opening_period' ---
        period_keys = ["id_etab", "open_day", "open_hour", "open_minute", "close_day", "close_hour", "close_minute"]
        try:
            existing_periods_df = spark.read.jdbc(
                url=db_url, table=f"(select {', '.join(period_keys)} from opening_period) as op", properties=db_properties
            )
        except Exception:
            print("La table 'opening_period' est probablement vide. On continue...")
            existing_periods_df = spark.createDataFrame([], opening_periods_df.schema)
            
        final_periods_to_insert = opening_periods_df.join(
            existing_periods_df, on=period_keys, how="left_anti"
        )
        # --- Fin de la logique anti-doublons ---

        inserted_count = final_periods_to_insert.count()
        if inserted_count > 0:
            final_periods_to_insert.write.jdbc(
                url=db_url,
                table="opening_period",
                mode="append",
                properties=db_properties,
            )
            print(f"✅ {inserted_count} nouvelle(s) ligne(s) insérée(s) dans 'opening_period'.")
        else:
            print("Aucune nouvelle période d'ouverture à insérer.")
    else:
        print("Aucune colonne 'regularOpeningHours' trouvée dans les données source.")

    spark.stop()
    print("--- Spark Job Finished ---")


if __name__ == "__main__":
    main()