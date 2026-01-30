

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# Task functions
def start_number(ti):
    ti.xcom_push(key='current_value', value=10)
    print("Starting with number 10")


def add_five(ti):
    current_value = ti.xcom_pull(
        key='current_value',
        task_ids='start_task'
    )
    new_value = current_value + 5
    ti.xcom_push(key='current_value', value=new_value)
    print(f"Adding 5: {current_value} + 5 = {new_value}")


def multiply_by_two(ti):
    current_value = ti.xcom_pull(
        key='current_value',
        task_ids='add_five_task'
    )
    new_value = current_value * 2
    ti.xcom_push(key='current_value', value=new_value)
    print(f"Multiplying by 2: {current_value} * 2 = {new_value}")


def subtract_three(ti):
    current_value = ti.xcom_pull(
        key='current_value',
        task_ids='multiply_by_two_task'
    )
    new_value = current_value - 3
    ti.xcom_push(key='current_value', value=new_value)
    print(f"Subtracting 3: {current_value} - 3 = {new_value}")


def compute_square(ti):
    current_value = ti.xcom_pull(
        key='current_value',
        task_ids='subtract_three_task'
    )
    final_value = current_value ** 2
    ti.xcom_push(key='final_value', value=final_value)
    print(f"Computing square: {current_value}^2 = {final_value}")


# DAG definition
with DAG(
    dag_id='math_sequence_dag',
    start_date=datetime(2025, 4, 22),
    schedule ='@once',
    catchup=False,
    tags=['example', 'math']
) as dag:

    start_task = PythonOperator(
        task_id='start_task',
        python_callable=start_number
    )

    add_five_task = PythonOperator(
        task_id='add_five_task',
        python_callable=add_five
    )

    multiply_by_two_task = PythonOperator(
        task_id='multiply_by_two_task',
        python_callable=multiply_by_two
    )

    subtract_three_task = PythonOperator(
        task_id='subtract_three_task',
        python_callable=subtract_three
    )

    compute_square_task = PythonOperator(
        task_id='compute_square_task',
        python_callable=compute_square
    )

    # Task dependencies
    start_task >> add_five_task >> multiply_by_two_task >> subtract_three_task >> compute_square_task
