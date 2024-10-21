import asyncio
import datetime
import json
import pandas
import pandas_gbq
import requests
import time
from google.cloud import bigquery as bq

from airflow import models
from airflow.operators.python_operator import PythonOperator
from common.common_function import task_fail_telegram_alert

from dateutil import parser
from urllib.parse import quote_plus

# Step 1. Parameters. Edit it


SOURCE_NAME = 'bitrix_user'
DAG_NUMBER = '1'

################################## Parameters. Don't edit it! ##################################

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

# Параметры подключения к BQ
LOCATION = 'europe-west3'
PROJECT = 'southern-idea-345503'
DATASET = 'raw_data_bitrix'
TABLE = 'user'

# Таблицы для хранения данных
TABLE_NAME = f'{PROJECT}.{DATASET}.{TABLE}'

BX_WH_URL = models.Variable.get('BX_WH_URL_Contacts_1_secret')
BX_PAGE_SIZE = 50
BX_BATCH_COMMANDS = 50

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
    schedule_interval=datetime.timedelta(days=1),
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

    async def user_list_batch(filters, fields, page_idx=0):
        data = []
        data.append('halt=0')

        while page_idx < BX_BATCH_COMMANDS:
            cmd = 'cmd[page_{}]=user.search'.format(page_idx)
            cmd_args = []
            for idx, field in enumerate(fields):
                cmd_args.append('select[{}]={}'.format(idx, field))
            for key, value in filters.items():
                cmd_args.append('filter[{}]={}'.format(key, value))
            cmd_args.append('order[ID]=ASC')
            cmd_args = quote_plus('?' + '&'.join(cmd_args))
            if page_idx > 0:
                cmd_args += quote_plus('&start={}'.format(page_idx * BX_PAGE_SIZE))
            data.append(cmd + cmd_args)
            page_idx += 1

        data = '&'.join(data)
        print(data)
        response = _call_bx_method('batch', data)
        response_data = response.json()
        users = []
        for cmd_key, cmd_result in response_data.get('result', {}).get('result', {}).items():
            users.extend(cmd_result)
        return users

    async def extra_users_list(fields, page_idx=0):
        extra_user_ids = [
            109, 170, 4081, 302, 351, 77, 285, 352, 9, 292, 396, 359, 169, 17, 344, 2696, 13699, 159, 838, 76347, 20,
            10128, 2195, 303, 3362, 1585, 62156, 23, 295, 5516, 68, 35, 254, 199, 345, 4012, 82, 341, 805, 134, 268,
            298, 1591, 283, 155, 57, 1963, 277, 297, 22736, 328, 81, 362, 369, 363, 286, 52, 78, 214, 10189, 45, 9124,
            125, 6501, 20470, 281, 26, 227, 0, 3, 46900, 1979, 276, 22195, 347, 70525, 16584, 22, 1, 17625, 344, 70756, 
            105964, 63834, 113102, 108720, 50556, 70, 65, 21, 1947, 206, 357, 3422, 2121, 14092, 1950, 15, 112107, 314,
            71953, 70661, 44, 40, 385, 113104, 113103, 291,10791,404,807,113127,118929,61240,130989,7711,170644
        ]

        data = []
        data.append('halt=0')

        extra_user_ids_parts = [extra_user_ids[i:i + 50] for i in range(0, len(extra_user_ids), 50)]
        for extra_user_ids_part in extra_user_ids_parts:
            cmd = 'cmd[page_{}]=user.search'.format(page_idx)
            cmd_args = []
            for idx, field in enumerate(fields):
                cmd_args.append('select[{}]={}'.format(idx, field))
            for idx, deal_id in enumerate(extra_user_ids_part):
                cmd_args.append('filter[ID][{}]={}'.format(idx, deal_id))
            cmd_args = quote_plus('?' + '&'.join(cmd_args))
            cmd_args += quote_plus('&start=-1')
            data.append(cmd + cmd_args)
            page_idx += 1

        data = '&'.join(data)
        print(data)
        response = _call_bx_method('batch', data)
        response_data = response.json()
        users = []
        for cmd_key, cmd_result in response_data.get('result', {}).get('result', {}).items():
            users.extend(cmd_result)
        return users

    def load_users():
        users = asyncio.run(user_list_batch(filters={}, fields=[], page_idx=0))
        print(users)

        # extra_users = asyncio.run(extra_users_list(fields=[]))
        # for extra_user in extra_users:
        #     if not next((_ for _ in users if _['ID'] == extra_user['ID']), None):
        #         users.append(extra_user)

        fields = [
            'ID', 'XML_ID', 'ACTIVE', 'NAME', 'LAST_NAME', 'SECOND_NAME', 'TITLE', 'EMAIL', 'PERSONAL_PHONE',
            'WORK_PHONE', 'WORK_POSITION', 'WORK_COMPANY', 'IS_ONLINE', 'TIME_ZONE', 'TIMESTAMP_X', 'TIME_ZONE_OFFSET',
            'DATE_REGISTER', 'LAST_ACTIVITY_DATE', 'PERSONAL_PROFESSION', 'PERSONAL_GENDER', 'PERSONAL_BIRTHDAY',
            'PERSONAL_PHOTO', 'PERSONAL_FAX', 'PERSONAL_MOBILE', 'PERSONAL_PAGER', 'PERSONAL_STREET',
            'PERSONAL_MAILBOX', 'PERSONAL_CITY', 'PERSONAL_STATE', 'PERSONAL_ZIP', 'PERSONAL_COUNTRY', 'PERSONAL_NOTES',
            'WORK_DEPARTMENT', 'WORK_WWW', 'WORK_FAX', 'WORK_PAGER', 'WORK_STREET', 'WORK_MAILBOX', 'WORK_CITY',
            'WORK_STATE', 'WORK_ZIP', 'WORK_COUNTRY', 'WORK_PROFILE', 'WORK_LOGO', 'WORK_NOTES', 'UF_DEPARTMENT',
            'UF_DISTRICT', 'UF_SKYPE', 'UF_SKYPE_LINK', 'UF_ZOOM', 'UF_TWITTER', 'UF_FACEBOOK', 'UF_LINKEDIN',
            'UF_XING', 'UF_WEB_SITES', 'UF_PHONE_INNER', 'UF_EMPLOYMENT_DATE', 'UF_TIMEMAN', 'UF_SKILLS',
            'UF_INTERESTS', 'USER_TYPE', 'UF_USR_1655383282545', 'UF_USR_1655383302488', 'UF_USR_1655384043127'
        ]

        simple_types = (str, int, float,)
        force_int_fields = ('ID',)
        force_datetime_fields = (
            'DATE_REGISTER', 'PERSONAL_BIRTHDAY'
        )

        for user in users:
            for field, value in user.items():
                if field in force_int_fields:
                    try:
                        user[field] = int(value)
                    except (TypeError, ValueError):
                        user[field] = None
                elif field in force_datetime_fields:
                    if value:
                        user[field] = parser.parse(value)
                    else:
                        user[field] = None
                elif isinstance(value, simple_types):
                    user[field] = str(value)
                else:
                    user[field] = json.dumps(value)

        src_table_schema = []
        for field in fields:
            if field in force_int_fields:
                src_table_schema.append({'name': field, 'type': 'INTEGER'})
            elif field in force_datetime_fields:
                src_table_schema.append({'name': field, 'type': 'TIMESTAMP'})
            else:
                src_table_schema.append({'name': field, 'type': 'STRING'})

        df = pandas.json_normalize(users)

        client = bq.Client()
        query = 'DELETE FROM `{}` WHERE ID IN ({})'.format(
            TABLE_NAME,
            ','.join([str(item) for item in df['ID'].to_list()])
        )
        client.query(query, project=PROJECT)

        pandas_gbq.to_gbq(
            dataframe=df,
            destination_table=TABLE_NAME,
            project_id=PROJECT,
            table_schema=src_table_schema,
            if_exists='append',
        )
        return True

    load_users_task = PythonOperator(
        task_id='load_users_task',
        python_callable=load_users,
    )

    def done_func():
        print('Users loaded')
        return 'Users loaded'

    done_task = PythonOperator(task_id='done_task', python_callable=done_func)

    load_users_task >> done_task
