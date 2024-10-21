import asyncio
import datetime
import json
import pandas
import pandas_gbq
import requests
import time

from urllib.parse import quote_plus

from airflow import models
from airflow.operators.python_operator import PythonOperator
from common.common_function import task_fail_telegram_alert

# Step 1. Parameters. Edit it
SOURCE_NAME = 'bitrix_lists'
DAG_NUMBER = '1'

################################## Parameters. Don't edit it! ##################################

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

# Параметры подключения к BQ
LOCATION = 'europe-west3'
PROJECT = 'southern-idea-345503'
DATASET = 'technical_tables'

BX_WH_URL = models.Variable.get('BX_WH_URL_Lists_1_secret')
BX_BATCH_COMMANDS = 50
BX_PAGE_SIZE = 50

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
    schedule_interval='10 2 * * *',
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


    async def get_system_lists():
        response = _call_bx_method('lists.get', {'IBLOCK_TYPE_ID': 'lists'})
        response_data = response.json()
        return response_data.get('result', [])


    async def get_deal_userfields():
        response = _call_bx_method('crm.deal.userfield.list', '')
        response_data = response.json()
        return response_data.get('result', [])


    async def get_lead_userfields():
        response = _call_bx_method('crm.lead.userfield.list', '')
        response_data = response.json()
        return response_data.get('result', [])


    async def get_contact_userfields():
        response = _call_bx_method('crm.contact.userfield.list', '')
        response_data = response.json()
        return response_data.get('result', [])


    async def all_list_values(iblock_id):
        print(f'all_list_values({iblock_id})')
        values = []
        start = 0
        while True:
            print(start)
            chunk_values = list_values_batch(iblock_id, start)
            print(len(chunk_values))
            for item in chunk_values:
                if not next((_ for _ in values if _['ID'] == item['ID']), None):
                    values.append(item)
            start = len(values)
            if len(chunk_values) < BX_PAGE_SIZE * BX_BATCH_COMMANDS:
                break
        return values


    async def list_fields_batch(iblock_ids):
        print('iblock_ids')
        print(iblock_ids)
        items = []
        iblock_ids_parts = [iblock_ids[i:i + 50] for i in range(0, len(iblock_ids), 50)]
        for iblock_ids_part in iblock_ids_parts:
            data = []
            data.append('halt=0')
            for iblock_id in iblock_ids_part:
                cmd = 'cmd[list_{}]=lists.field.get'.format(iblock_id)
                cmd_args = []
                cmd_args.append('IBLOCK_TYPE_ID=lists')
                cmd_args.append('IBLOCK_ID={}'.format(iblock_id))
                cmd_args = quote_plus('?' + '&'.join(cmd_args))
                data.append(cmd + cmd_args)

            data = '&'.join(data)
            response = _call_bx_method('batch', data)
            response_data = response.json()
            for cmd_key, cmd_result in response_data.get('result', {}).get('result', {}).items():
                iblock_id = cmd_key.replace('list_', '')
                for field_key, field_value in cmd_result.items():
                    if not next(
                        (_ for _ in items if _['PROPERTY_ID'] == field_key and _['IBLOCK_ID'] == iblock_id), None
                    ):
                        items.append({
                            'PROPERTY_ID': field_key,
                            'NAME': field_value.get('NAME'),
                            'LINK_IBLOCK_ID': field_value.get('LINK_IBLOCK_ID'),
                            'IBLOCK_ID': iblock_id,
                        })
        return items


    def list_values_batch(iblock_id, start=0):
        data = []
        data.append('halt=0')

        page_idx = 0
        while page_idx < BX_BATCH_COMMANDS:
            cmd = 'cmd[page_{}]=lists.element.get'.format(page_idx)
            cmd_args = []
            cmd_args.append('IBLOCK_TYPE_ID=lists')
            cmd_args.append('IBLOCK_ID={}'.format(iblock_id))
            cmd_args = quote_plus('?' + '&'.join(cmd_args))
            cmd_args += quote_plus('&start={}'.format(start + page_idx * BX_PAGE_SIZE))
            data.append(cmd + cmd_args)
            page_idx += 1

        data = '&'.join(data)
        response = _call_bx_method('batch', data)
        response_data = response.json()
        items = []
        for cmd_key, cmd_result in response_data.get('result', {}).get('result', {}).items():
            for item in cmd_result:
                if not next((_ for _ in items if _['ID'] == item['ID']), None):
                    items.append(item)
        return items


    def load_lists():
        system_lists = asyncio.run(get_system_lists())

        userfields_lists = []

        userfields = asyncio.run(get_deal_userfields())
        chunk_userfields_lists = [_ for _ in userfields if _['USER_TYPE_ID'] == 'iblock_element']
        userfields_lists.extend(chunk_userfields_lists)

        # userfields = asyncio.run(get_lead_userfields())
        # chunk_userfields_lists = [_ for _ in userfields if _['USER_TYPE_ID'] == 'iblock_element']
        # userfields_lists.extend(chunk_userfields_lists)
        # for chunk_userfields_list in chunk_userfields_lists:
        #     if not next((_ for _ in userfields_lists if _['SETTINGS']['IBLOCK_ID'] == chunk_userfields_list['SETTINGS']['IBLOCK_ID']), None):
        #         userfields_lists.append(chunk_userfields_list)

        # userfields = asyncio.run(get_contact_userfields())
        # chunk_userfields_lists = [_ for _ in userfields if _['USER_TYPE_ID'] == 'iblock_element']
        # userfields_lists.extend(chunk_userfields_lists)
        # for chunk_userfields_list in chunk_userfields_lists:
        #     if not next((_ for _ in userfields_lists if _['SETTINGS']['IBLOCK_ID'] == chunk_userfields_list['SETTINGS']['IBLOCK_ID']), None):
        #         userfields_lists.append(chunk_userfields_list)

        simple_types = (str, int, float,)

        all_lists = {}

        iblock_ids = [system_list['ID'] for system_list in system_lists]
        iblock_ids.extend([userfield['SETTINGS']['IBLOCK_ID'] for userfield in userfields_lists])
        list_fields = asyncio.run(list_fields_batch(list(set(iblock_ids))))
        df = pandas.json_normalize(list_fields)
        pandas_gbq.to_gbq(
            dataframe=df,
            destination_table=f'{PROJECT}.{DATASET}.list_fields',
            project_id=PROJECT,
            if_exists='replace',
        )

        for system_list in system_lists:
            if system_list['ID'] in ['49','50','93']:
                continue
            else:
                iblock_id = system_list['ID']
                list_name = f'list_{iblock_id}'
                print(list_name)
                print(iblock_id)
                list_values = all_lists.get(iblock_id)
                if not list_values:
                    list_values = asyncio.run(all_list_values(iblock_id))

                    for item in list_values:
                        for field, value in item.items():
                            if isinstance(value, simple_types):
                                item[field] = str(value)
                            else:
                                item[field] = json.dumps(value)

                    all_lists[iblock_id] = list_values

                if len(list_values):
                    TABLE_NAME = f'{PROJECT}.{DATASET}.{list_name}'
                    df = pandas.json_normalize(list_values)
                    pandas_gbq.to_gbq(
                        dataframe=df,
                        destination_table=TABLE_NAME,
                        project_id=PROJECT,
                        if_exists='replace',
                    )
                else:
                    print(f'empty list {list_name}')
        print(userfields_lists)
        for userfield in userfields_lists:
            print(userfield["FIELD_NAME"])
            if userfield["FIELD_NAME"] not in ['UF_CRM_5D6A7B362362C','UF_CRM_1644223273','UF_CRM_1688622883','UF_CRM_1689225727','UF_CRM_1689254720','UF_CRM_1690436041']:
                iblock_id = userfield['SETTINGS']['IBLOCK_ID']
                print(iblock_id)
                list_values = all_lists.get(iblock_id)
                if not list_values:

                    list_values = asyncio.run(all_list_values(iblock_id))

                    for item in list_values:
                        for field, value in item.items():
                            if isinstance(value, simple_types):
                                item[field] = str(value)
                            else:
                                item[field] = json.dumps(value)

                    all_lists[iblock_id] = list_values

                if len(list_values):
                    TABLE_NAME = f'{PROJECT}.{DATASET}.{userfield["FIELD_NAME"]}'
                    df = pandas.json_normalize(list_values)
                    pandas_gbq.to_gbq(
                        dataframe=df,
                        destination_table=TABLE_NAME,
                        project_id=PROJECT,
                        if_exists='replace',
                    )
                else:
                    print(f'empty list {userfield["FIELD_NAME"]} {userfield["SETTINGS"]["IBLOCK_ID"]}')

        return True

    load_lists_task = PythonOperator(
        task_id='load_lists_task',
        python_callable=load_lists,
    )

    def done_func():
        print('Lists loaded')
        return 'Lists loaded'

    done_task = PythonOperator(task_id='done_task', python_callable=done_func)

    load_lists_task >> done_task
