import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

class Settings:
    # Paths
    BASE_DIR = Path(__file__).parent.parent
    DATA_DIR = BASE_DIR / "data"
    RAW_DIR = DATA_DIR / "raw"
    PROCESSED_DIR = DATA_DIR / "processed"
    CURATED_DIR = DATA_DIR / "curated"
    
    # Database
    DATABASE_URL = os.getenv(
        "DATABASE_URL",
        "postgresql://etl:etl123@localhost:5432/jobspulse"
    )
    
    # MinIO / S3
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    MINIO_BUCKET = os.getenv("MINIO_BUCKET", "jobspulse")
    
    # Scraping
    REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "2.0"))
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

    # API
    API_HOST = os.getenv("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("API_PORT", "8000"))
    
settings = Settings()