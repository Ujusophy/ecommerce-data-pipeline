from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging

# ---- DAG Default Arguments ----
# These apply to every task unless overridden
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,           # don't wait for previous run to succeed
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,                       # retry failed tasks twice
    "retry_delay": timedelta(minutes=5) # wait 5 min between retries
}

# ---- DAG Definition ----
with DAG(
    dag_id="ecommerce_pipeline",
    description="E-Commerce Silver → Gold pipeline",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 * * * *",      # run every hour (cron syntax)
    catchup=False,                      # don't backfill missed runs
    tags=["ecommerce", "silver", "gold"],
) as dag:

    # ---- Task 1: Health Check ----
    # Always start with a health check — fail fast before wasting compute
    def check_data_availability(**context):
        """
        Check Bronze layer has new data before running expensive Spark jobs.
        In production this would check row counts, file timestamps, etc.
        """
        import os
        bronze_path = "/opt/airflow/data/bronze/ecommerce_events"

        if not os.path.exists(bronze_path):
            raise FileNotFoundError(
                f"Bronze layer not found at {bronze_path}. "
                "Is the Spark streaming job running?"
            )

        files = [f for f in os.listdir(bronze_path) if f.endswith(".parquet")]
        if len(files) == 0:
            raise ValueError("Bronze layer exists but has no parquet files yet.")

        logging.info(f"✅ Bronze layer healthy — found {len(files)} parquet files")
        return len(files)

    health_check = PythonOperator(
        task_id="check_bronze_availability",
        python_callable=check_data_availability,
        provide_context=True,
    )

    # ---- Task 2: Run Silver Job ----
    # BashOperator runs shell commands — perfect for spark-submit
    run_silver = BashOperator(
        task_id="run_silver_job",
        bash_command="""
            /opt/spark/bin/spark-submit \
            --packages io.delta:delta-spark_2.12:3.0.0 \
            /opt/airflow/spark-jobs/silver_job.py
        """,
        execution_timeout=timedelta(minutes=30),   # kill if takes longer than 30 min
    )

    # ---- Task 3: Run dbt Models ----
    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command="""
            cd /opt/airflow/dbt/ecommerce_gold && \
            dbt run --profiles-dir /opt/airflow/dbt
        """,
        execution_timeout=timedelta(minutes=15),
    )

    # ---- Task 4: Run dbt Tests ----
    # Always test AFTER building — catch bad data before it hits dashboards
    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command="""
            cd /opt/airflow/dbt/ecommerce_gold && \
            dbt test --profiles-dir /opt/airflow/dbt
        """,
        execution_timeout=timedelta(minutes=10),
    )

    # ---- Task 5: Pipeline Summary ----
    def log_pipeline_summary(**context):
        """
        Log a summary of what this pipeline run produced.
        In production this would send a Slack message or email.
        """
        ti = context["task_instance"]
        run_id = context["run_id"]
        execution_date = context["execution_date"]

        logging.info("=" * 50)
        logging.info("✅ PIPELINE RUN COMPLETE")
        logging.info(f"   Run ID:         {run_id}")
        logging.info(f"   Execution date: {execution_date}")
        logging.info(f"   DAG: ecommerce_pipeline")
        logging.info("   Tasks completed: health_check → silver → dbt models → dbt tests")
        logging.info("=" * 50)

    notify_success = PythonOperator(
        task_id="log_pipeline_summary",
        python_callable=log_pipeline_summary,
        provide_context=True,
        trigger_rule="all_success",     # only runs if ALL upstream tasks succeeded
    )

    # ---- Task Dependencies ----
    # This defines the ORDER tasks run in — the DAG structure
    # >> means "then run"
    health_check >> run_silver >> run_dbt_models >> run_dbt_tests >> notify_success