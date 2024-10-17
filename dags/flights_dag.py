import sys
import os

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

from airflow.operators.python import PythonOperator
from airflow import DAG

from datetime import datetime

sys.path.insert(0, root_dir)

from pipelines.flights_pipeline import run_pipeline

default_args = {
    'owner': 'omar-sinno',
    'start_date': datetime(2024, 4, 4, 1, 41)
}

with DAG(dag_id='google_flights_pipeline', default_args=default_args, schedule_interval='@daily') as dag:

    op_kwargs = {
        "engine": "google_flights",
        "departure_id": "BEY",
        "arrival_id": "HND",
        "outbound_date": "2024-05-10",
        "return_date": "2024-05-15",
        "currency": "USD",
        "hl": "en"
	}
    
    extract_data = PythonOperator(
        task_id='google_flights_extraction',
        python_callable=run_pipeline,
        op_kwargs=op_kwargs,
        dag=dag
    )

    extract_data