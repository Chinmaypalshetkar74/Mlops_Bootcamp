from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id='nasa_apod_postgres_etl',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
) as dag:

    # STEP 1: Create table
    @task
    def create_table():
        hook = PostgresHook(postgres_conn_id='postgres_default')
        hook.run("""
            CREATE TABLE IF NOT EXISTS apod_data (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                explanation TEXT,
                apod_date DATE,
                media_type VARCHAR(50),
                url TEXT
            );
        """)

    # STEP 2: Extract APOD using DEMO_KEY
    extract_apod = HttpOperator(
        task_id='extract_apod',
        http_conn_id=None,  # no Airflow connection needed
        endpoint='https://api.nasa.gov/planetary/apod',
        method='GET',
        params={"api_key": "DEMO_KEY"},
        response_filter=lambda r: r.json(),
        log_response=True,
    )

    # STEP 3: Transform
    @task
    def transform_apod_data(response):
        return {
            "title": response.get("title"),
            "explanation": response.get("explanation"),
            "apod_date": datetime.strptime(response.get("date"), "%Y-%m-%d").date(),
            "media_type": response.get("media_type"),
            "url": response.get("url"),
        }

    # STEP 4: Load to Postgres
    @task
    def load_to_postgres(data):
        hook = PostgresHook(postgres_conn_id='postgres_default')
        hook.run(
            """
            INSERT INTO apod_data (title, explanation, apod_date, media_type, url)
            VALUES (%s, %s, %s, %s, %s);
            """,
            parameters=(
                data["title"],
                data["explanation"],
                data["apod_date"],
                data["media_type"],
                data["url"],
            ),
        )

    # Dependencies
    create_table() >> extract_apod
    transformed = transform_apod_data(extract_apod.output)
    load_to_postgres(transformed)
