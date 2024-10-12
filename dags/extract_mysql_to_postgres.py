import yaml
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from datetime import datetime, timedelta


# Akses config untuk mendapatkan list table
with open("dags/resources/config/tabel_tokoreza.yaml", "r") as f:
    config = yaml.safe_load(f)


# Fungsi transfer dari mysql ke Postgres
@task
def extract_transfer_data(table_name):
    # Buat koneksi MySQL dan PostgreSQL menggunakan SQLAlchemy engine
    mysql_engine = MySqlHook(mysql_conn_id="mysql_tokoreza").get_sqlalchemy_engine()

    staging_filename = f"staging_{table_name}.csv"

    # Membuat query untuk membaca
    query = f"SELECT * FROM {table_name}"

    # Membangun koneksi dan membaca data
    with mysql_engine.connect() as mysql_conn:
        df = pd.read_sql(query, mysql_conn)

    # Tempat staging
    staging_path = f"data/{staging_filename}"
    df.to_csv(staging_path, index=False)

    return staging_path


@task
def load_data_from_staging_to_postgres(file_path, table_name):
    postgres_engine = PostgresHook(
        postgres_conn_id="postgres_tokoreza"
    ).get_sqlalchemy_engine()

    df = pd.read_csv(file_path)

    # Load data ke postgres
    with postgres_engine.connect() as pg_conn:
        df.to_sql(
            f"datatoko.{table_name}",
            pg_conn,
            schema="datatoko_reza",
            if_exists="replace",
            index=False,
        )


# Mendefinisikan DAG
@dag(
    schedule_interval="15 9-21/2 * * 5#1,5",
    start_date=datetime(2024, 10, 12),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Transfer data dari MySQL ke PostgreSQL menggunakan Pandas",
)
def extract_mysql_to_postgres():
    start_task = EmptyOperator(task_id="start_task")
    end_task = EmptyOperator(task_id="end_task")

    # Dynamic task untuk setiap tabel
    for item in config.get("ingestion", []):
        extract_task = extract_transfer_data.override(
            task_id=f"extract_to_staging.{item['table'].lower()}"
        )(item["table"])

        load_task = load_data_from_staging_to_postgres.override(
            task_id=f"load_to_postgres.{item['table'].lower()}"
        )(extract_task, item["table"])
        start_task >> extract_task >> load_task >> end_task


# Inisialisasi DAG
dag = extract_mysql_to_postgres()
