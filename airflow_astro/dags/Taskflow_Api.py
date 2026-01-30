"""
Docstring for dags.Taskflow_Api


Apache Airflow introduces the TaskFlow API, which allows users to define tasks using Python functions and 
decorators like @task. this is a cleaner and more intutive way of writing 
task without needing to manually use operators like pythonoperator.
"""


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.decorators import task



#Define the DAG
with DAG(
    dag_id='math_sequence_dag_with_tasksflow',
    start_date=datetime(2025, 4, 22),
    schedule='@once',
    catchup=False,
) as dag:
    

# task 1 
    @task
    def start_number():
        initial_value = 10
        print(f"Starting with number {initial_value}")
        return initial_value
    
    #task 2
    @task
    def add_five(current_value):
        new_value = current_value + 5
        print(f"Adding 5: {current_value} + 5 = {new_value}")
        return new_value

    @task
    def multiply_by_two(current_value):
        new_value = current_value * 2
        print(f"Multiplying by 2: {current_value} * 2 = {new_value}")
        return new_value

    @task
    def subtract_three(current_value):
        new_value = current_value - 3
        print(f"Subtracting 3: {current_value} - 3 = {new_value}")
        return new_value

    @task
    def compute_square(current_value):
        final_value = current_value ** 2
        print(f"Computing square: {current_value}^2 = {final_value}")
        return final_value

    # Task dependencies
    initial_value = start_number()
    after_addition = add_five(initial_value)
    after_multiplication = multiply_by_two(after_addition)
    after_subtraction = subtract_three(after_multiplication)
    final_result = compute_square(after_subtraction)
