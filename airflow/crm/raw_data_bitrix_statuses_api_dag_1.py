import asyncio
import datetime
import json
import pandas
import pandas_gbq
import requests
import time

from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from common.common_function import task_fail_telegram_alert
from dateutil import parser
from urllib.parse import quote_plus

# Step 1. Parameters. Edit it
SOURCE_NAME = 'bitrix_status'
DAG_NUMBER = '1'

################################## Parameters. Don't edit it! ##################################

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
TODAY = datetime.datetime.now()

# Параметры подключения к BQ
LOCATION = 'europe-west3'
PROJECT = 'southern-idea-345503'
DATASET = 'technical_tables'
STATUS_TABLE = 'bitrix_status'
DEAL_STATUS_TABLE = 'bitrix_deal_status'
LEAD_STATUS_TABLE = 'bitrix_lead_status'
SOURCE_TABLE = 'bitrix_source'
DEAL_TYPE_TABLE = 'bitrix_deal_type'

# Переменные
# temp_path = models.Variable.get('temp_path')
## Файл csv и таблица для хранения профилей
# CSV_NAME = f"{temp_path}/{DATASET}_{TABLE}_{TODAY.strftime('%Y-%m-%d')}.csv"

# Таблицы для хранения данных
STATUS_TABLE_NAME = f'{PROJECT}.{DATASET}.{STATUS_TABLE}'
DEAL_STATUS_TABLE_NAME = f'{PROJECT}.{DATASET}.{DEAL_STATUS_TABLE}'
LEAD_STATUS_TABLE_NAME = f'{PROJECT}.{DATASET}.{LEAD_STATUS_TABLE}'
SOURCE_TABLE_NAME = f'{PROJECT}.{DATASET}.{SOURCE_TABLE}'
DEAL_TYPE_TABLE_NAME = f'{PROJECT}.{DATASET}.{DEAL_TYPE_TABLE}'

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
    # schedule_interval=datetime.timedelta(days=1),
    schedule_interval='20 2 * * *',
    tags = ['load','bitrix','api'],
) as dag:

    def _call_bx_method(method, data):
        def _call_bx(method, data):
            return requests.post(
                url='{}/{}/'.format(BX_WH_URL, method),
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

    async def statuses_list(filters):
        data = []

        # TODO фильтрация
        # for key, value in filters.items():
        #     data.append('filter[{}]={}'.format(key, value))

        # fields = [
        #     'ID', 'ENTITY_ID', 'STATUS_ID', 'NAME', 'NAME_INIT', 'SORT', 'SYSTEM', 'CATEGORY_ID',
        #     'COLOR', 'SEMANTICS', 'EXTRA'
        # ]

        for key, value in filters.items():
            data.append('filter[{}]={}'.format(key, value))

        data = '&'.join(data)

        print(data)
        response = _call_bx_method('crm.status.list', data)
        response_data = response.json()
        return response_data.get('result', [])

    def load_statuses():
        statuses_data = asyncio.run(statuses_list(filters={}))
        print(statuses_data)
        simple_types = (str, bool, int, float, datetime.date, datetime.datetime,)
        force_str_fields = ('EXTRA',)
        force_int_fields = ('ID', 'SORT', 'CATEGORY_ID')

        statuses = []
        deal_statuses = []
        lead_statuses = []
        sources = []
        deal_type = []
        for status in statuses_data:
            entity_id = status.get('ENTITY_ID')
            if not isinstance(entity_id, str):
                continue

            for field, value in status.items():
                if field in force_int_fields:
                    try:
                        status[field] = int(value)
                    except (TypeError, ValueError):
                        status[field] = None
                elif not isinstance(value, simple_types) or field in force_str_fields:
                    status[field] = json.dumps(value)

            if entity_id == 'STATUS':
                # статус лида
                statuses.append(status)
                lead_statuses.append(status)
            elif entity_id.startswith('DEAL_STAGE'):
                # статус сделки
                statuses.append(status)
                deal_statuses.append(status)
            elif entity_id == 'SOURCE':
                # источник
                sources.append(status)
            elif entity_id == 'DEAL_TYPE':
                # тип сделки
                deal_type.append(status)

        print('statuses')
        print(statuses)
        print('deal_statuses')
        print(deal_statuses)
        print('lead_statuses')
        print(lead_statuses)
        print('sources')
        print(sources)
        print('deal_type')
        print(deal_type)

        table_schema = []
        for field in force_int_fields:
            table_schema.append({'name': field, 'type': 'INTEGER'})
        for field in force_str_fields:
            table_schema.append({'name': field, 'type': 'STRING'})

        df = pandas.json_normalize(statuses)
        pandas_gbq.to_gbq(df, STATUS_TABLE_NAME, PROJECT, table_schema=table_schema, if_exists='replace')
        df = pandas.json_normalize(deal_statuses)
        pandas_gbq.to_gbq(df, DEAL_STATUS_TABLE_NAME, PROJECT, table_schema=table_schema, if_exists='replace')
        df = pandas.json_normalize(lead_statuses)
        pandas_gbq.to_gbq(df, LEAD_STATUS_TABLE_NAME, PROJECT, table_schema=table_schema, if_exists='replace')
        df = pandas.json_normalize(sources)
        pandas_gbq.to_gbq(df, SOURCE_TABLE_NAME, PROJECT, table_schema=table_schema, if_exists='replace')
        df = pandas.json_normalize(deal_type)
        pandas_gbq.to_gbq(df, DEAL_TYPE_TABLE_NAME, PROJECT, table_schema=table_schema, if_exists='replace')

        return True

    load_statuses_task = PythonOperator(
        task_id='load_statuses_task',
        python_callable=load_statuses,
    )

    def done_func():
        print('Statuses loaded')
        return 'Statuses loaded'

    done_task = PythonOperator(task_id='done_task', python_callable=done_func)

    load_statuses_task >> done_task