# src/processing/spark_job.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    print("--- Starting Spark Job ---")
    # --- 1. Create Spark Session ---
    spark = SparkSession.builder \
        .appName("MinIO to Postgres") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_KEY")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    print("Spark session created successfully.")

    # --- 2. Read Data from MinIO ---
    try:
        df = spark.read.option("multiLine", "true").json("s3a://datalake/raw/API_google/*/*.json")
        print(f"Found {df.count()} records in MinIO.")
        df.printSchema()
        df.show(5, truncate=False)
    except Exception as e:
        print(f"Error reading from MinIO: {e}")
        print("This might be because no data has been extracted yet. Exiting.")
        spark.stop()
        return

    # --- 3. Transform Data ---
    # Adaptez cette section à la structure de vos JSON et de votre BDD
    etablissement_df = df.select(
        col("name").alias("nom"),
        col("internationalPhoneNumber"),
        col("formattedAddress").alias("adresse"),
        col("rating"),
        col("websiteUri")
        # ... ajoutez les autres colonnes que vous voulez insérer
    ).na.drop(subset=["nom"])

    print("Transformed DataFrame ready for insertion:")
    etablissement_df.show(5, truncate=False)

    # --- 4. Write Data to PostgreSQL ---
    db_url = f"jdbc:postgresql://postgres:5432/{os.getenv('POSTGRES_DB')}"
    db_properties = {
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }

    try:
        etablissement_df.write.jdbc(
            url=db_url,
            table="etab",
            mode="append",
            properties=db_properties
        )
        print("Data successfully written to PostgreSQL table 'etab'.")
    except Exception as e:
        print(f"Error writing to PostgreSQL: {e}")

    spark.stop()
    print("--- Spark Job Finished ---")

if __name__ == "__main__":
    main()