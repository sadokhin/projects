# Import modules that are reqiured to create DAG (not for functions)
from datetime import datetime
import logging

from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook

from custom_modules.internal_data_processing import task_fail_slack_alert

# Get connection details from Airflow

# Fill DAG's parameters
default_args = {
    'owner': '@U06LQGHAKBN',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'on_failure_callback': task_fail_slack_alert,
}

# Fill literals for DAG's descriptions
START_DATE = datetime(2024, 4, 1, 0, 0, 0)
DAG_NAME = 'mart_data_dbt'
DAG_NUMBER = '1'
DESCRIPTION = 'DAG to create dbt models using a BashOperator'
DBT_PROJECT_DIR = "/opt/airflow/dags/repo/dbt"

################################## Parametrs. Don't edit it! ##################################
with models.DAG(
        dag_id=f'{DAG_NAME}_dag_{DAG_NUMBER}',
        default_args=default_args,
        catchup=False,
        start_date=START_DATE,
        description=DESCRIPTION,
        schedule_interval='25 0-23/1 * * *',
        tags = ['dbt','bash','transform'],
) as dag:
    ####################################################################

    ch_analytics_hook = BaseHook.get_connection('clickhouse_analytics')
    host=ch_analytics_hook.host, 
    port=ch_analytics_hook.port, 
    username=ch_analytics_hook.login, 
    password=ch_analytics_hook.password

    env_vars = {
        "DBT_USER": username[0],
        "DBT_ENV_SECRET_PASSWORD": password,
        "DBT_HOST": host[0],
        "DBT_PORT": port[0]
    }

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"rm -rf /tmp/dbt*;\
                        cp -R /opt/airflow/dags/repo/dbt /tmp;\
                        export DBT_PORT={env_vars['DBT_PORT']};\
                        export DBT_HOST={env_vars['DBT_HOST']};\
                        export DBT_USER={env_vars['DBT_USER']};\
                        export DBT_ENV_SECRET_PASSWORD={env_vars['DBT_ENV_SECRET_PASSWORD']};\
                        cd /tmp/dbt;\
                        dbt deps;\
                        dbt run",

    ) #--exclude daily_roxana roxana_rivals roxana_rivals2
    dbt_run