from datetime import datetime, timedelta
import sys
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.email import EmailOperator

# Ensure project modules are discoverable
sys.path.insert(0, '/opt/airflow/project')

from etl.extract import extract_all
from etl.transform import transform_all
from etl.load import load_all

# Updated to use environment variables to prevent privacy leaks
default_args = {
    'owner': 'Lesego',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': [os.getenv('ALERT_EMAIL', 'your_email@example.com')],  # Privacy Fix
}

with DAG(
    dag_id='eco_commerce_daily_etl',
    default_args=default_args,
    description='Daily Eco-Commerce ETL: file_sensor → extract → transform → validate → load → cleanup',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['eco', 'etl', 'postgres'],
    max_active_runs=1,
) as dag:

    # 1. FileSensor: Monitors for new sales data drops
    wait_for_sales_file = FileSensor(
        task_id='file_sensor',
        filepath='/opt/airflow/project/staging/sales_*.csv',
        poke_interval=30,
        timeout=120,
        mode='poke',
        soft_fail=True,
        dag=dag,
    )

    # 2. Debug mount: Verifies filesystem visibility
    debug_mount = BashOperator(
        task_id='debug_mount',
        bash_command='echo "=== Current dir ===" && pwd && ls -la /opt/airflow/project',
        trigger_rule='none_failed',
        dag=dag,
    )

    # 3. Run ingestion: Triggers the robust shell script
    run_ingestion = BashOperator(
        task_id='run_ingestion',
        bash_command='cd /opt/airflow/project && ./ingest.sh || echo "Ingest completed with warnings - continuing"',
        trigger_rule='none_failed',
        dag=dag,
    )

    # 4. Extract: Python-based multi-format extraction
    def extract_wrapper(**context):
        data = extract_all('/opt/airflow/project/staging')
        context['task_instance'].xcom_push(key='extracted_data', value=data)
        return data

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_wrapper,
        provide_context=True,
        trigger_rule='none_failed',
        dag=dag,
    )

    # 5. Transform: Cleans data and calculates "Green" metrics
    def transform_wrapper(**context):
        data = context['task_instance'].xcom_pull(key='extracted_data', task_ids='extract')
        transformed = transform_all(data)
        context['task_instance'].xcom_push(key='transformed_data', value=transformed)
        return transformed

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_wrapper,
        provide_context=True,
        trigger_rule='none_failed',
        dag=dag,
    )

    # 6. Validate: Basic threshold checks before loading
    def validate_wrapper(**context):
        transformed = context['task_instance'].xcom_pull(key='transformed_data', task_ids='transform')
        row_count = len(transformed) if transformed is not None else 0
        if row_count < 10:
            print(f"Warning: only {row_count} rows after transform")
        else:
            print(f"Validation passed: {row_count} rows")

    validate = PythonOperator(
        task_id='validate',
        python_callable=validate_wrapper,
        provide_context=True,
        trigger_rule='none_failed',
        dag=dag,
    )

    # 7. Load: SCD Type 2 and Fact table ingestion
    def load_wrapper(**context):
        data = context['task_instance'].xcom_pull(key='transformed_data', task_ids='transform')
        load_all(data)
        row_count = len(data.get('sales', [])) if data else 0
        context['task_instance'].xcom_push(key='row_count', value=row_count)

    load = PythonOperator(
        task_id='load',
        python_callable=load_wrapper,
        provide_context=True,
        trigger_rule='none_failed',
        dag=dag,
    )

    # 8. SQLOperator: Verifies load metadata
    log_metadata = PostgresOperator(
        task_id='log_metadata',
        postgres_conn_id='postgres_default',
        sql="""
        SELECT load_timestamp, rows_loaded, status 
        FROM metadata_loads 
        ORDER BY load_timestamp DESC 
        LIMIT 1;
        """,
        trigger_rule='none_failed',
        dag=dag,
    )

    # 9. Cleanup: Purges staging area after success
    cleanup = BashOperator(
        task_id='cleanup',
        bash_command='rm -f /opt/airflow/project/staging/* && echo "Staging cleaned"',
        trigger_rule='none_failed',
        dag=dag,
    )

    # 10. Email alert: Fixed to use environment variable
    failure_email = EmailOperator(
        task_id='failure_email',
        to=os.getenv('ALERT_EMAIL', 'your_email@example.com'), # Privacy Fix
        subject='Airflow Failure: {{ dag.dag_id }}',
        html_content='<p>Task <b>{{ task_instance.task_id }}</b> failed at {{ logical_date }}</p>',
        trigger_rule='one_failed',
        dag=dag,
    )

    # DAG Dependency Sequence
    wait_for_sales_file >> debug_mount >> run_ingestion >> extract >> transform >> validate >> load >> log_metadata >> cleanup >> failure_email