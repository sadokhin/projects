# Import modules that are reqiured to create DAG (not for functions)
from datetime import datetime
import logging

from airflow import models
from airflow.operators.python_operator import PythonOperator

from custom_modules.internal_data_processing import task_fail_slack_alert

# Add DDL query if applicable
'''
CREATE TABLE IF NOT EXISTS  (

);
'''
# Fill DAG's parameters
default_args = {
    'owner': '@U06LQGHAKBN',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'on_failure_callback': task_fail_slack_alert
}

# Template for get connection details from Airflow
# from airflow.hooks.base_hook import BaseHook

# service_hook = BaseHook.get_connection('service')
# service_user = service_hook.login
# service_token = service_hook.password
# service_host = service_hook.host

# Template for get variables from airflow 
# models.Variable.get('slack_hook_roxana_reports')

# Fill literals for DAG's descriptions
START_DATE = datetime(2024, 3, 27, 0, 0, 0)
DAG_NAME = 'template'
DAG_NUMBER = '1'
DESCRIPTION = 'DAG template'

################################## Parametrs. Don't edit it! ##################################
with models.DAG(
        dag_id=f'{DAG_NAME}_dag_{DAG_NUMBER}',
        default_args=default_args,
        catchup=False,
        start_date=START_DATE,
        description=DESCRIPTION,
        schedule_interval='0 7 * * *',
        tags = ['template'],
) as dag:
    ####################################################################
    def your_function():
        # Import modules for function
        import requests
        import json
        import pandas

        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

        return

    # Define task
    your_task = PythonOperator(
        task_id='your_task',
        python_callable=your_function,
        dag=dag,
    )

    # Define task relation
    your_task