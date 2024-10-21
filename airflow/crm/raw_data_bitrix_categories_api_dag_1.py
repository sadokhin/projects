import asyncio
import datetime
import pandas
import pandas_gbq
import requests
import time

from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from common.common_function import task_fail_telegram_alert

# Step 1. Parameters. Edit it
SOURCE_NAME = 'bitrix_categories'
DAG_NUMBER = '1'

################################## Parameters. Don't edit it! ##################################

# Параметры подключения к BQ
LOCATION = 'europe-west3'
PROJECT = 'southern-idea-345503'
DATASET = 'technical_tables'
TABLE = 'bitrix_pipelines'
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

# Таблицы для хранения данных
TABLE_NAME = f'{PROJECT}.{DATASET}.{TABLE}'

BX_WH_URL = models.Variable.get('BX_WH_URL_Contacts_1_secret')

default_args = {
    'owner': 'Composer Example',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': task_fail_telegram_alert
}

################################## Parametrs. Don't edit it! ##################################
with models.DAG(
    dag_id=f'{DATASET}_{SOURCE_NAME}_dag_{DAG_NUMBER}',
    default_args=default_args,
    catchup=False,
    start_date=YESTERDAY,
    schedule_interval='5 2 * * 6',
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

    async def categories_list():
        response = _call_bx_method('crm.category.list?entityTypeId=2', '')  # 2 - сделка
        response_data = response.json()
        return response_data.get('result', {}).get('categories', [])

    def load_categories():
        categories = asyncio.run(categories_list())
        print(categories)
        force_int_fields = ('id', 'sort', 'entityTypeId')

        for category in categories:
            for field, value in category.items():
                if field in force_int_fields:
                    try:
                        category[field] = int(value)
                    except (TypeError, ValueError):
                        category[field] = None

        print('categories')
        print(categories)

        table_schema = []
        for field in force_int_fields:
            table_schema.append({'name': field, 'type': 'INTEGER'})

        df = pandas.json_normalize(categories)
        pandas_gbq.to_gbq(df, TABLE_NAME, PROJECT, table_schema=table_schema, if_exists='replace')

        return True

    load_categories_task = PythonOperator(
        task_id='load_categories_task',
        python_callable=load_categories,
    )

    def done_func():
        print('Categories loaded')
        return 'Categories loaded'

    done_task = PythonOperator(task_id='done_task', python_callable=done_func)

    load_categories_task >> done_task