import asyncio
import datetime
import json
import pandas
import pandas_gbq
import requests
import time

from airflow import models
from airflow.operators.python_operator import PythonOperator
from common.common_function import task_fail_telegram_alert

# Step 1. Parameters. Edit it
SOURCE_NAME = 'bitrix_crm_enum'
DAG_NUMBER = '1'
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
################################## Parameters. Don't edit it! ##################################

# Параметры подключения к BQ
LOCATION = 'europe-west3'
PROJECT = 'southern-idea-345503'
DATASET = 'technical_tables'

BX_WH_URL = models.Variable.get('BX_WH_URL_Contacts_1_secret')

default_args = {
    'owner': 'Composer Example',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'on_failure_callback': task_fail_telegram_alert
}

################################## Parametrs. Don't edit it! ##################################
with models.DAG(
    dag_id=f'{DATASET}_{SOURCE_NAME}_dag_{DAG_NUMBER}',
    default_args=default_args,
    catchup=False,
    start_date=YESTERDAY,
    schedule_interval='10 2 * * 6',
    tags = ['load','bitrix','api'],
) as dag:

    def _call_bx_method(method, data):
        def _call_bx(method, data):
            return requests.post(
                url='{}/{}'.format(BX_WH_URL, method),
                data=data
            )

        response = None
        call_delay = 1
        while call_delay < 10:
            print(call_delay)
            try:
                response = _call_bx(method, data)
                if response.ok and response.json():
                    return response
            except:
                pass
            time.sleep(call_delay)
            call_delay += call_delay
        return response


    async def crm_enum_activitytype():
        response = _call_bx_method('crm.enum.activitytype', '')
        response_data = response.json()
        return response_data.get('result', {})


    async def crm_enum_activitystatus():
        response = _call_bx_method('crm.enum.activitystatus', '')
        response_data = response.json()
        return response_data.get('result', {})


    async def crm_enum_ownertype():
        response = _call_bx_method('crm.enum.ownertype', '')
        response_data = response.json()
        return response_data.get('result', {})


    def load_data():
        items = asyncio.run(crm_enum_activitytype())
        load_enum_data('bitrix_activitytype', items)

        items = asyncio.run(crm_enum_activitystatus())
        load_enum_data('bitrix_activitystatus', items)

        items = asyncio.run(crm_enum_ownertype())
        load_enum_data('bitrix_ownertype', items)

        return True


    def load_enum_data(table_name, items):
        simple_types = (str, int, float,)

        for item in items:
            for field, value in item.items():
                if isinstance(value, simple_types):
                    item[field] = str(value)
                else:
                    item[field] = json.dumps(value)

        TABLE_NAME = f'{PROJECT}.{DATASET}.{table_name}'
        df = pandas.json_normalize(items)
        pandas_gbq.to_gbq(
            dataframe=df,
            destination_table=TABLE_NAME,
            project_id=PROJECT,
            if_exists='replace',
        )


    load_data_task = PythonOperator(
        task_id='load_data_task',
        python_callable=load_data,
    )

    def done_func():
        print('Crm enum loaded')
        return 'Crm enum loaded'

    done_task = PythonOperator(task_id='done_task', python_callable=done_func)

    load_data_task >> done_task
