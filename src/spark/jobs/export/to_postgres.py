"""
Export Gold layer data to PostgreSQL for API serving.
"""
from pyspark.sql import SparkSession
from src.spark.lib.config import get_spark_session
from src.config import settings
from src.utils.logger import get_logger

logger = get_logger("spark.export.postgres")

JDBC_URL = f"jdbc:postgresql://localhost:5432/jobspulse"
JDBC_PROPERTIES = {
    "user": "etl",
    "password": "etl123",
    "driver": "org.postgresql.Driver"
}

def export_table(spark: SparkSession, parquet_path: str, table_name: str, mode: str = "overwrite"):
    """Export Parquet to PostgreSQL table."""
    df = spark.read.parquet(parquet_path)

    (df.write
        .mode(mode)
        .jdbc(JDBC_URL, table_name, properties=JDBC_PROPERTIES))

    logger.info(f"Exported {df.count()} rows to {table_name}")

def run_export():
    """Export all Gold tables to PostgreSQL."""
    spark = get_spark_session("JobsPulse_Export")

    gold_base = str(settings.CURATED_DIR / "gold")
    silver_path = str(settings.PROCESSED_DIR / "silver" / "jobs")

    # Export Silver jobs (main table)
    export_table(spark, silver_path, "jobs")

    # Export Gold aggregations
    export_table(spark, f"{gold_base}/dim_skills", "dim_skills")
    export_table(spark, f"{gold_base}/dim_companies", "dim_companies")
    export_table(spark, f"{gold_base}/fact_daily_jobs", "fact_daily_jobs")
    export_table(spark, f"{gold_base}/skill_trends", "skill_trends")

    spark.stop()
    logger.info("Export to PostgreSQL completed")

if __name__ == "__main__":
    run_export()