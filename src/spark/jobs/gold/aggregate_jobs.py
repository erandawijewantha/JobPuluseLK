"""
Gold Layer: Create business-ready aggregations and metrics.
"""
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, countDistinct, avg, explode, array,
    current_date, datediff, when, lit, collect_list,
    struct, to_date, date_trunc
)
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql import functions as F
from src.spark.lib.config import get_spark_session
from src.spark.lib.skill_extractor import extract_skills_from_title
from src.config import settings
from src.utils.logger import get_logger

logger = get_logger("spark.gold.aggregate")

# Register UDF
from pyspark.sql.functions import udf
extract_skills_udf = udf(extract_skills_from_title, ArrayType(StringType()))

def build_dim_skills(silver_df: DataFrame, spark: SparkSession) -> DataFrame:
    """Build skills dimension from job titles."""
    # Extract skills from each job
    jobs_with_skills = silver_df.withColumn(
        "skills",
        extract_skills_udf(col("title"))
    )

    # Explode to get one row per skill
    skill_jobs = jobs_with_skills.select(
        col("job_id"),
        col("scraped_at"),
        explode(col("skills")).alias("skill")
    )

    # Aggregate skill metrics
    skill_metrics = (skill_jobs
        .groupBy("skill")
        .agg(
            count("*").alias("job_count"),
            countDistinct("job_id").alias("unique_jobs"),
            F.max("scraped_at").alias("last_seen")
        )
        .orderBy(col("job_count").desc())
    )

    return skill_metrics

def build_fact_daily_jobs(silver_df: DataFrame) -> DataFrame:
    """Build daily job posting fact table."""
    return (silver_df
        .withColumn("post_date", to_date(col("scraped_at")))
        .groupBy("post_date", "source", "district", "category")
        .agg(
            count("*").alias("job_count"),
            countDistinct("company_normalized").alias("unique_companies")
        )
        .orderBy("post_date", "source")
    )

def build_dim_companies(silver_df: DataFrame) -> DataFrame:
    """Build company dimension with job counts."""
    return (silver_df
        .filter(col("company_normalized").isNotNull())
        .groupBy("company_normalized")
        .agg(
            count("*").alias("total_jobs"),
            countDistinct("title_normalized").alias("unique_roles"),
            collect_list(struct("title", "district", "scraped_at")).alias("recent_jobs")
        )
        .orderBy(col("total_jobs").desc())
    )

def build_skill_trends(silver_df: DataFrame, spark: SparkSession) -> DataFrame:
    """Build weekly skill trends."""
    jobs_with_skills = silver_df.withColumn(
        "skills",
        extract_skills_udf(col("title"))
    ).withColumn(
        "week",
        date_trunc("week", col("scraped_at"))
    )

    skill_jobs = jobs_with_skills.select(
        col("week"),
        explode(col("skills")).alias("skill")
    )

    return (skill_jobs
        .groupBy("week", "skill")
        .agg(count("*").alias("job_count"))
        .orderBy("week", col("job_count").desc())
    )

def run_gold_aggregations():
    """Run all Gold layer aggregations."""
    spark = get_spark_session("JobsPulse_Gold")

    # Read Silver data
    silver_path = str(settings.PROCESSED_DIR / "silver" / "jobs")
    silver_df = spark.read.parquet(silver_path)

    logger.info(f"Read {silver_df.count()} records from Silver layer")

    gold_base = str(settings.CURATED_DIR / "gold")

    # Build and save dimension/fact tables
    # 1. Skill Dimension
    dim_skills = build_dim_skills(silver_df, spark)
    dim_skills.write.mode("overwrite").parquet(f"{gold_base}/dim_skills")
    logger.info(f"Built dim_skills: {dim_skills.count()} skills")

    # 2. Daily Jobs Fact
    fact_daily = build_fact_daily_jobs(silver_df)
    fact_daily.write.mode("overwrite").parquet(f"{gold_base}/fact_daily_jobs")
    logger.info(f"Built fact_daily_jobs: {fact_daily.count()} records")

    # 3. Company Dimension
    dim_companies = build_dim_companies(silver_df)
    dim_companies.write.mode("overwrite").parquet(f"{gold_base}/dim_companies")
    logger.info(f"Built dim_companies: {dim_companies.count()} companies")

    # 4. Skill Trends
    skill_trends = build_skill_trends(silver_df, spark)
    skill_trends.write.mode("overwrite").parquet(f"{gold_base}/skill_trends")
    logger.info(f"Built skill_trends: {skill_trends.count()} records")

    spark.stop()

if __name__ == "__main__":
    run_gold_aggregations()