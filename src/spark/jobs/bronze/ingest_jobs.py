"""
Bronze Layer: Ingest raw scraped data into Parquet format.
No transformations - just structure the raw data.
"""

from datetime import datetime
from pathlib import Path
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from src.spark.lib.config import get_spark_session
from src.spark.lib.schemas import BRONZE_JOB_SCHEMA
from src.config import settings
from src.utils.logger import get_logger

logger = get_logger("spark.bronze.ingest")

def find_raw_files(date: str = None) -> list[Path]:
    """Find all raw JSON files for given date or today."""
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")

    raw_files = []
    for source_dir in settings.RAW_DIR.iterdir():
        if source_dir.is_dir():
            date_dir = source_dir / date
            if date_dir.exists():
                raw_files.extend(date_dir.glob("*.json"))

    return raw_files

def load_raw_json(file_path: Path) -> list[dict]:
    """Load records from raw JSON file."""
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("records", [])

def ingest_bronze(date: str = None):
    """Ingest raw data into Bronze layer."""
    spark = get_spark_session("JobsPulse_Bronze")

    # Find raw files
    raw_files = find_raw_files(date)
    if not raw_files:
        logger.warning(f"No raw files found for date: {date}")
        return

    logger.info(f"Found {len(raw_files)} raw files to process")

    # Load all records
    all_records = []
    for file_path in raw_files:
        records = load_raw_json(file_path)
        all_records.extend(records)
        logger.info(f"Loaded {len(records)} records from {file_path.name}")

    if not all_records:
        logger.warning("No records to process")
        return
    
    # Create DataFrame
    df = spark.createDataFrame(all_records, schema=BRONZE_JOB_SCHEMA)

    # Add metadata
    df = df.withColumn("_ingested_at", current_timestamp())
    df = df.withColumn("_ingestion_date", lit(date or datetime.now().strftime("%Y-%m-%d")))

    # Write to Bronze layer (partitioned by date and source)
    output_path = str(settings.PROCESSED_DIR / "bronze" / "jobs")

    (df.write
        .mode("append")
        .partitionBy("_ingestion_date", "source")
        .parquet(output_path))

    logger.info(f"Wrote {df.count()} records to Bronze layer: {output_path}")

    spark.stop()

if __name__ == "__main__":
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else None
    ingest_bronze(date)