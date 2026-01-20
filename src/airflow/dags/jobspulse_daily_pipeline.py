"""
JobsPulse Daily Pipeline
Runs nightly to scrape, process, and load job data.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.filesystem import FileSensor

default_args = {
    'owner': 'jobspulse',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='jobspulse_daily_pipeline',
    default_args=default_args,
    description='Daily job scraping and processing pipeline',
    schedule_interval='0 2 * * *',  # 2 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['jobspulse', 'scraping', 'etl'],
) as dag:

    # ============ SCRAPING TASK GROUP ============
    with TaskGroup(group_id='scrape') as scrape_group:
        scrape_topjobs = BashOperator(
            task_id='scrape_topjobs',
            bash_command='python -m src.scrapers.topjobs',
        )

        scrape_ikman = BashOperator(
            task_id='scrape_ikman',
            bash_command='python -m src.scrapers.ikman',
        )

        # Run scrapers in parallel
        [scrape_topjobs, scrape_ikman]

    # ============ BRONZE LAYER ============
    with TaskGroup(group_id='bronze') as bronze_group:
        ingest_bronze = BashOperator(
            task_id='ingest_jobs',
            bash_command='python -m src.spark.jobs.bronze.ingest_jobs {{ ds }}',
        )

    # ============ SILVER LAYER ============
    with TaskGroup(group_id='silver') as silver_group:
        transform_silver = BashOperator(
            task_id='transform_jobs',
            bash_command='python -m src.spark.jobs.silver.transform_jobs {{ ds }}',
        )

    # ============ GOLD LAYER ============
    with TaskGroup(group_id='gold') as gold_group:
        aggregate_gold = BashOperator(
            task_id='aggregate_jobs',
            bash_command='python -m src.spark.jobs.gold.aggregate_jobs',
        )

    # ============ EXPORT ============
    with TaskGroup(group_id='export') as export_group:
        export_postgres = BashOperator(
            task_id='to_postgres',
            bash_command='python -m src.spark.jobs.export.to_postgres',
        )

    # ============ DATA QUALITY ============
    with TaskGroup(group_id='quality') as quality_group:
        run_quality_checks = BashOperator(
            task_id='quality_checks',
            bash_command='python -m tests.data_quality.run_checks',
        )

    # ============ PIPELINE FLOW ============
    scrape_group >> bronze_group >> silver_group >> gold_group >> export_group >> quality_group