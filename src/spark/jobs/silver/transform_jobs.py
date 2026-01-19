# src/spark/jobs/silver/transform_jobs.py
"""
Silver Layer: Clean, normalize, and deduplicate job data.
"""
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, udf, current_timestamp, md5, concat_ws,
    to_timestamp, when, trim, lower
)
from pyspark.sql.types import StringType
from src.spark.lib.config import get_spark_session
from src.spark.lib.text_utils import normalize_title, normalize_company, extract_district
from src.config import settings
from src.utils.logger import get_logger

logger = get_logger("spark.silver.transform")

# Register UDFs
normalize_title_udf = udf(normalize_title, StringType())
normalize_company_udf = udf(normalize_company, StringType())
extract_district_udf = udf(extract_district, StringType())

def generate_job_id(df: DataFrame) -> DataFrame:
    """Generate unique job_id from source + source_job_id."""
    return df.withColumn(
        "job_id",
        md5(concat_ws("_", col("source"), col("source_job_id")))
    )

def clean_and_normalize(df: DataFrame) -> DataFrame:
    """Apply cleaning and normalization."""
    return (df
        # Trim whitespace
        .withColumn("title", trim(col("title")))
        .withColumn("company", trim(col("company")))
        .withColumn("location", trim(col("location")))

        # Normalize text
        .withColumn("title_normalized", normalize_title_udf(col("title")))
        .withColumn("company_normalized", normalize_company_udf(col("company")))

        # Extract district
        .withColumn("district", extract_district_udf(col("location")))

        # Parse timestamp
        .withColumn("scraped_at", to_timestamp(col("scraped_at")))

        # Add processing metadata
        .withColumn("processed_at", current_timestamp())
    )

def deduplicate(df: DataFrame) -> DataFrame:
    """Remove duplicate jobs, keeping most recent."""
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number

    window = Window.partitionBy("job_id").orderBy(col("scraped_at").desc())

    return (df
        .withColumn("_rank", row_number().over(window))
        .filter(col("_rank") == 1)
        .drop("_rank"))

def transform_silver(date: str = None):
    """Transform Bronze data to Silver layer."""
    spark = get_spark_session("JobsPulse_Silver")

    # Read from Bronze
    bronze_path = str(settings.PROCESSED_DIR / "bronze" / "jobs")

    if date:
        df = spark.read.parquet(bronze_path).filter(col("_ingestion_date") == date)
    else:
        df = spark.read.parquet(bronze_path)

    logger.info(f"Read {df.count()} records from Bronze layer")

    # Apply transformations
    df = generate_job_id(df)
    df = clean_and_normalize(df)
    df = deduplicate(df)

    # Select Silver columns
    silver_df = df.select(
        "job_id",
        "source",
        "source_job_id",
        "title",
        "title_normalized",
        "company",
        "company_normalized",
        "location",
        "district",
        "job_url",
        "category",
        "scraped_at",
        "processed_at"
    )

    # Write to Silver layer
    output_path = str(settings.PROCESSED_DIR / "silver" / "jobs")

    (silver_df.write
        .mode("overwrite")
        .partitionBy("district")
        .parquet(output_path))

    logger.info(f"Wrote {silver_df.count()} records to Silver layer: {output_path}")

    spark.stop()

if __name__ == "__main__":
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else None
    transform_silver(date)