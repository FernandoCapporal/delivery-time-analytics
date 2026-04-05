"""
DAG: weekly_delivery_etl
Description: Weekly ETL pipeline that creates and populates
             uber_prod.mx_delivery_trips_info from source tables.
Schedule: Every Monday at 00:00 UTC (covers previous week)
"""

from datetime import datetime, timedelta
from airflow.models import Variable
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
import os


process_name = "weekly_delivery_etl"
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
QUERIES_DIR = os.path.join(DAG_DIR, "queries")

# ─────────────────────────────────────────────────────────────
# Airflow Variables
# ─────────────────────────────────────────────────────────────
dag_variable = Variable.get(process_name, default_var={}, deserialize_json=True)
countries  = dag_variable.get("countries", {"mx": "Mexico"})
schema = dag_variable.get("schema", "uber_prod")
cron = dag_variable.get("cron", "0 0 * * 1")
# ─────────────────────────────────────────────────────────────
# Default args
# ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "luis.caporal",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ─────────────────────────────────────────────────────────────
# DAG definition
# ─────────────────────────────────────────────────────────────
with DAG(
    dag_id=process_name,
    description="Weekly ETL: extracts delivery trips and writes to uber_prod schema",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=cron,  # Every Monday at midnight UTC
    catchup=False,
    tags=["delivery", "etl", "weekly"],
    template_searchpath=[QUERIES_DIR],
) as dag:

    # ─────────────────────────────────────────────────────────
    # Task 1: Create schema if not exists
    # ─────────────────────────────────────────────────────────
    create_schema = PostgresOperator(
        task_id="create_schema",
        postgres_conn_id="postgres_local",  # Connection ID defined in Airflow UI
        sql="""
            CREATE SCHEMA IF NOT EXISTS uber_prod;
        """,
    )

    for country, country_name in countries.items():

        params = {
            "country": country,
            "country_name": country_name,
            "schema": schema,
        }

        # ─────────────────────────────────────────────────────────
        # Task 2: Insert weekly data
        #         Date range is dynamically computed from {{ ds }}
        # ─────────────────────────────────────────────────────────
        insert_weekly_data = PostgresOperator(
            task_id=f"{country}_weekly_delivery_data",
            postgres_conn_id="postgres_local",
            sql="weekly_delivery_query.sql",
            params=params,
        )

        # ─────────────────────────────────────────────────────────
        # Task 3: Save historic
        # ─────────────────────────────────────────────────────────
        validate_load = PostgresOperator(
            task_id=f"{country}_historic_delivery_data",
            postgres_conn_id="postgres_local",
            sql="historic_delivery_query.sql",
            params=params,
        )

        # ─────────────────────────────────────────────────────────
        # Task dependencies
        # ─────────────────────────────────────────────────────────
        (
            create_schema
            >> insert_weekly_data
            >> validate_load
        )