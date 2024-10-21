import asyncio
import datetime
import json

import google
import pandas
import pandas_gbq
import requests
import time
import traceback

from airflow import models
from airflow.operators.python_operator import PythonOperator
from common.common_function import task_fail_telegram_alert

from dateutil import parser
from google.cloud import bigquery
from urllib.parse import quote_plus

from pandas_gbq.schema import to_google_cloud_bigquery

# Step 1. Parameters. Edit it
SOURCE_NAME = 'bitrix_deal_stagehistory'
DAG_NUMBER = '1'

################################## Parameters. Don't edit it! ##################################

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

# Параметры подключения к BQ
LOCATION = 'europe-west3'
PROJECT = 'southern-idea-345503'
DATASET = 'raw_data_bitrix'
TABLE = 'deal_stagehistory'
# TABLE = 'deal_stagehistory2'

# Таблицы для хранения данных
TABLE_NAME = f'{PROJECT}.{DATASET}.{TABLE}'

BX_WH_URL = models.Variable.get('BX_WH_URL_Contacts_1_secret')
BX_PAGE_SIZE = 50
BX_BATCH_COMMANDS = 50

MAX_RUN_ITERATIONS = 50

client = bigquery.Client(project=PROJECT)

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
    schedule_interval=datetime.timedelta(hours=1),
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
        while call_delay < 3:
            print(call_delay)
            try:
                response = _call_bx(method, data)
                if response.ok and response.json():
                    return response
                else:
                    print(response.status_code)
                    print(response.content)
            except:
                tb = traceback.format_exc()
                print(tb)
                pass
            time.sleep(call_delay)
            call_delay += call_delay
        return response

    async def items_list_batch(start_id, filters, fields, page_idx=0):
        data = []
        data.append('halt=0')

        while page_idx < BX_BATCH_COMMANDS:
            cmd = 'cmd[page_{}]=crm.stagehistory.list?entityTypeId=2'.format(page_idx)  # 2 - сделка
            cmd_args = []
            for idx, field in enumerate(fields):
                cmd_args.append('select[{}]={}'.format(idx, field))
            for key, value in filters.items():
                cmd_args.append('filter[{}]={}'.format(key, value))
            cmd_args.append('order[ID]=ASC')
            cmd_args = quote_plus('&' + '&'.join(cmd_args))
            if page_idx == 0:
                if start_id:
                    cmd_args += quote_plus('&filter[>ID]=') + str(start_id)
            else:
                cmd_args += quote_plus('&filter[>ID]=') + "$result[page_{}][items][{}][ID]".format(
                    page_idx - 1, BX_PAGE_SIZE - 1
                )
            # if page_idx > 0:
            #     cmd_args += quote_plus('&start={}'.format(page_idx * BX_PAGE_SIZE))
            cmd_args += quote_plus('&start=-1')
            data.append(cmd + cmd_args)
            page_idx += 1

        data = '&'.join(data)
        print(data)
        response = _call_bx_method('batch', data)
        response_data = response.json()
        items = []
        for cmd_key, cmd_result in response_data.get('result', {}).get('result', {}).items():
            for item in cmd_result.get('items', []):
                if not next((_ for _ in items if _['ID'] == item['ID']), None):
                    items.append(item)

        return items

    def get_last_item():
        try:
            result = client.query(f'SELECT MAX(ID) FROM `{TABLE_NAME}`').result()
            return next(result)[0]
        except google.api_core.exceptions.NotFound:
            return None

    def load_items():
        iteration = 0
        while iteration < MAX_RUN_ITERATIONS:
            iteration += 1
            print('iteration')
            print(iteration)

            filters = {}
            last_item = get_last_item()
            if last_item:
                filters = {'>ID': last_item}

            fields = ['ID', 'TYPE_ID', 'OWNER_ID', 'CREATED_TIME', 'CATEGORY_ID', 'STAGE_SEMANTIC_ID', 'STAGE_ID']

            items = asyncio.run(
                items_list_batch(start_id=last_item, filters=filters, fields=[], page_idx=0)
            )
            print(BX_WH_URL)
            print(len(items))

            simple_types = (str, int, float,)
            force_int_fields = (
                'ID', 'TYPE_ID', 'OWNER_ID', 'CATEGORY_ID',
            )
            force_datetime_fields = (
                'CREATED_TIME',
            )
            for item in items:
                for field, value in item.items():
                    if field in force_int_fields:
                        try:
                            item[field] = int(value)
                        except (TypeError, ValueError):
                            item[field] = None
                    elif field in force_datetime_fields:
                        if value:
                            item[field] = parser.parse(value)
                        else:
                            item[field] = None
                    elif isinstance(value, simple_types):
                        item[field] = str(value)
                    else:
                        item[field] = json.dumps(value)

            print(len(items))
            src_table_schema = []
            for field in fields:
                if field in force_int_fields:
                    src_table_schema.append({'name': field, 'type': 'INTEGER'})
                elif field in force_datetime_fields:
                    src_table_schema.append({'name': field, 'type': 'TIMESTAMP'})
                else:
                    src_table_schema.append({'name': field, 'type': 'STRING'})

            try:
                client.get_table(TABLE_NAME)  # Make an API request.
                print("Table {} already exists.".format(TABLE_NAME))
                query = 'DELETE FROM `{}` WHERE ID IN ({})'.format(
                    TABLE_NAME,
                    ','.join([str(history_item['ID']) for history_item in items])
                )
                client.query(query, project=PROJECT)
            except:
                print('src_table_schema')
                print(src_table_schema)
                schema = to_google_cloud_bigquery(dict(fields=src_table_schema))
                print('schema')
                print(schema)
                table = bigquery.Table(TABLE_NAME)
                table.schema = schema
                time_partitioning = bigquery.TimePartitioning()
                time_partitioning.type = bigquery.TimePartitioningType.DAY
                time_partitioning.field = 'CREATED_TIME'
                table.time_partitioning = time_partitioning
                client.create_table(table)

            df = pandas.json_normalize(items)
            pandas_gbq.to_gbq(
                dataframe=df,
                destination_table=TABLE_NAME,
                project_id=PROJECT,
                table_schema=src_table_schema,
                if_exists='append',
            )

            if len(items) < BX_PAGE_SIZE * BX_BATCH_COMMANDS:
                break

        return True

    load_items_task = PythonOperator(
        task_id='load_items_task',
        python_callable=load_items,
    )

    def done_func():
        print('Deals stagehistory loaded')
        return 'Deals stagehistory loaded'

    done_task = PythonOperator(task_id='done_task', python_callable=done_func)

    load_items_task >> done_task
