from airflow import (
    DAG,
)  # Import the DAG class to define the Directed Acyclic Graph (DAG) for Airflow
from airflow.operators.python import (
    PythonOperator,
)  # Import the PythonOperator to run Python functions as tasks
from datetime import (
    datetime,
)  # Import datetime to set the start date of the DAG

# Import the custom function that processes the example data
from scripts.data_processing import prepare_example_data


# Define the DAG, which orchestrates the tasks to run within a specific schedule and context.
with DAG(
    dag_id="example_dag",  # Unique identifier for the DAG
    start_date=datetime(
        2024, 1, 1
    ),  # The start date of the DAG; the first time it will run
    schedule_interval=None,  # Set to None, meaning the DAG will be triggered manually (not on a schedule)
) as example_dag:
    """
    This DAG is designed to run the `prepare_example_data` function as a Python task.
    It is triggered manually (as `schedule_interval` is set to `None`), and the task will
    process the data according to the defined steps in the `prepare_example_data` function.
    """

    # Define a task using the PythonOperator to execute the `prepare_example_data` function
    prepare_data = PythonOperator(
        task_id="prepare_data",  # Unique task identifier within the DAG
        python_callable=prepare_example_data,  # The Python function to execute when the task runs
        provide_context=True,  # Pass Airflow context (including `kwargs`) to the Python function
    )
    """
    The `prepare_data` task calls the `prepare_example_data` function from the `scripts.data_processing`
    module. The task will run the function with the Airflow context (e.g., `kwargs`, including `run_id`) 
    so that the data processing can be triggered accordingly based on the context provided by Airflow.
    """
