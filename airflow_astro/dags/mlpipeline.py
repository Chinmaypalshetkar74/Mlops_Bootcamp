from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# Task 1: Preprocess data
def preprocess_data():
    print("Pre-processing data...")


# Task 2: Train model
def train_model():
    print("Training model...")


# Task 3: Evaluate model
def evaluate_model():
    print("Evaluating model...")


# Define the DAG
with DAG(
    dag_id='ml_pipeline',
    start_date=datetime(2025, 4, 22),
    schedule='@weekly',
    catchup=False,
    tags=['ml', 'pipeline']
) as dag:

    preprocess_task = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess_data,
    )

    train_task = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
    )

    evaluate_task = PythonOperator(
        task_id='evaluate_model',
        python_callable=evaluate_model,
    )

    preprocess_task >> train_task >> evaluate_task
