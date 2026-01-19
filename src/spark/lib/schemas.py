from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    IntegerType, ArrayType, DoubleType
)


# Bronze layer schema - raw data as-is
BRONZE_JOB_SCHEMA = StructType([
    StructField("source", StringType(), False),
    StructField("source_job_id", StringType(), False),
    StructField("title", StringType(), True),
    StructField("company", StringType(), True),
    StructField("location", StringType(), True),
    StructField("job_url", StringType(), True),
    StructField("category", StringType(), True),
    StructField("functional_area_id", StringType(), True),
    StructField("scraped_at", StringType(), True),
])

# Silver layer schema - cleaned and normalized
SILVER_JOB_SCHEMA = StructType([
    StructField("job_id", StringType(), False),  # Generated unique ID
    StructField("source", StringType(), False),
    StructField("source_job_id", StringType(), False),
    StructField("title", StringType(), True),
    StructField("title_normalized", StringType(), True),
    StructField("company", StringType(), True),
    StructField("company_normalized", StringType(), True),
    StructField("location", StringType(), True),
    StructField("district", StringType(), True),
    StructField("job_url", StringType(), True),
    StructField("category", StringType(), True),
    StructField("scraped_at", TimestampType(), True),
    StructField("processed_at", TimestampType(), True),
])

