from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

# Define default args for the master DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 12, 12),
}

with DAG(
    dag_id="master_dag",
    default_args=default_args,
    schedule_interval="@daily",  # Schedule for the master DAG
) as master_dag:

    # Trigger the 'client_data_processing' DAG for Client 1
    trigger_client_1 = TriggerDagRunOperator(
        task_id="trigger_client_data_processing_for_client_1",
        trigger_dag_id="client_data_processing",  # Same DAG ID to trigger
        conf={
            "client_id": "client_1",
            "data_path": "/path/to/client_1/data",
        },  # Parameters for Client 1
        wait_for_completion=True,  # Wait for this instance to complete before moving on
    )

    # Define task dependencies if needed
    trigger_client_1
