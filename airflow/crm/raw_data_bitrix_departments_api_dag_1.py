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
SOURCE_NAME = 'bitrix_departments'
DAG_NUMBER = '1'

################################## Parameters. Don't edit it! ##################################

# Параметры подключения к BQ
LOCATION = 'europe-west3'
PROJECT = 'southern-idea-345503'
DATASET = 'technical_tables'
TABLE = 'bitrix_departments'
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
    schedule_interval='0 22 * * *',
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

    async def departments_list():
        response = _call_bx_method('department.get', '')
        data = response.json()
        departments = data['result']
        while 'next' in data:
            method = 'department.get/?start='+str(data['next'])
            response = _call_bx_method(method, '')
            data = response.json()
            departments.extend(data['result'])
        return departments

    def load_departments():
        departments = asyncio.run(departments_list())
        print(departments)
        force_int_fields = ('id', 'sort', 'uf_head')

        for department in departments:
            for field, value in department.items():
                if field in force_int_fields:
                    try:
                        department[field] = int(value)
                    except (TypeError, ValueError):
                        department[field] = None

        print('departments')
        print(departments)

        table_schema = []
        for field in force_int_fields:
            table_schema.append({'name': field, 'type': 'INTEGER'})

        df = pandas.json_normalize(departments)
        pandas_gbq.to_gbq(df, TABLE_NAME, PROJECT, table_schema=table_schema, if_exists='replace')

        return True

    load_departments_task = PythonOperator(
        task_id='load_departments_task',
        python_callable=load_departments,
    )

    def done_func():
        print('departments loaded')
        return 'departments loaded'

    done_task = PythonOperator(task_id='done_task', python_callable=done_func)

    load_departments_task >> done_task
