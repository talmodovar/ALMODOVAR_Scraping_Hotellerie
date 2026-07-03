import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def run_spark_gold_container():
    import docker
    
    print("Connecting to local Docker daemon via socket...")
    client = docker.from_env()
    
    # Define host path for mounting workspace
    host_project_path = "C:\\Users\\Thomas\\Documents\\IPSSI\\Architecture data\\Travail à rendre\\ALMODOVAR_PROJET"
    
    print(f"Starting Apache Spark container with host volume: {host_project_path}")
    
    # Run spark-submit inside the Spark container
    # Since it is connected to the same network 'bce_network', it can reach namenode, mongo, and mongo_state.
    logs = client.containers.run(
        image="apache/spark:3.5.0",
        command="/opt/spark/bin/spark-submit /app/Transformation/spark_gold.py",
        network_mode="almodovar_projet_bce_network",
        user="root",
        volumes={
            host_project_path: {
                "bind": "/app",
                "mode": "rw"
            }
        },
        environment={
            "MONGO_URI": "mongodb://mongo:27017/",
            "MONGO_STATE_URI": "mongodb://mongo_state:27017/",
            "MONGO_DB": "bce_db",
            "MONGO_STATE_DB": "bce_state_db",
            "HDFS_NAMENODE": "hdfs://namenode:9000"
        },
        remove=True,
        stdout=True,
        stderr=True
    )
    
    print("Spark container execution complete. Logs:")
    print(logs.decode("utf-8"))

# Define DAG
with DAG(
    "gold_processing_dag",
    default_args=default_args,
    description="Run Spark processing to consolidate data into the hotel_gold collection in MongoDB",
    schedule_interval="@yearly", # recalculate yearly or triggered manually
    catchup=False,
) as dag:

    run_spark_task = PythonOperator(
        task_id="run_spark_gold_layer",
        python_callable=run_spark_gold_container,
    )

    run_spark_task
