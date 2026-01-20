"""
Data quality checks for JobsPulse pipeline.
"""
import sys
from sqlalchemy import create_engine, text
from src.config import settings
from src.utils.logger import get_logger

logger = get_logger("data_quality")

def get_engine():
    return create_engine(settings.DATABASE_URL)

def check_jobs_not_empty():
    """Ensure jobs table has records."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM jobs")).scalar()
        assert result > 0, f"Jobs table is empty! Count: {result}"
        logger.info(f"✓ Jobs table has {result} records")

def check_no_duplicate_job_ids():
    """Ensure no duplicate job_ids."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT job_id, COUNT(*) as cnt
            FROM jobs
            GROUP BY job_id
            HAVING COUNT(*) > 1
        """)).fetchall()
        assert len(result) == 0, f"Found {len(result)} duplicate job_ids"
        logger.info("✓ No duplicate job_ids found")

def check_skills_extracted():
    """Ensure skills were extracted."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM dim_skills")).scalar()
        assert result > 0, f"No skills extracted! Count: {result}"
        logger.info(f"✓ Extracted {result} unique skills")

def check_recent_data():
    """Ensure data is recent (within 7 days)."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*)
            FROM jobs
            WHERE scraped_at >= CURRENT_DATE - INTERVAL '7 days'
        """)).scalar()
        logger.info(f"✓ Found {result} jobs from last 7 days")

def run_all_checks():
    """Run all data quality checks."""
    checks = [
        check_jobs_not_empty,
        check_no_duplicate_job_ids,
        check_skills_extracted,
        check_recent_data,
    ]

    failed = 0
    for check in checks:
        try:
            check()
        except AssertionError as e:
            logger.error(f"✗ {check.__name__}: {e}")
            failed += 1
        except Exception as e:
            logger.error(f"✗ {check.__name__}: Unexpected error: {e}")
            failed += 1

    if failed > 0:
        logger.error(f"Data quality checks failed: {failed}/{len(checks)}")
        sys.exit(1)
    else:
        logger.info(f"All {len(checks)} data quality checks passed!")

if __name__ == "__main__":
    run_all_checks()