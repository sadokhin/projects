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
from dateutil import parser
from google.cloud import bigquery
from urllib.parse import quote_plus

from pandas_gbq.schema import to_google_cloud_bigquery
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from common.common_function import task_fail_telegram_alert

################################## Parameters. Don't edit it! ##################################
START_DATE_FROM = datetime.datetime(2022, 8, 1, 0, 0, 0, 0)
SOURCE_NAME = 'bitrix_contact'
DAG_NUMBER = '3'
NOW = datetime.datetime.now() 
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
LOCATION = 'europe-west3'
PROJECT = 'southern-idea-345503'
DATASET = 'raw_data_bitrix'
TABLE = 'contact_daily_copy'
TABLE_NAME = f'{PROJECT}.{DATASET}.{TABLE}'
BX_WH_URL = models.Variable.get('BX_WH_URL_Contacts_1_secret')
BX_PAGE_SIZE = 50
BX_BATCH_COMMANDS = 50
MAX_RUN_ITERATIONS = 100

credentials, project = google.auth.default(
    scopes=[
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/cloud-platform",
    ]
)
client = bigquery.Client(credentials=credentials,project=project)

default_args = {
    'owner': 'Composer Example',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'on_failure_callback': task_fail_telegram_alert
}

################################## Parametrs. Don't edit it! ##################################
with models.DAG(
    dag_id=f'{DATASET}_{SOURCE_NAME}_dag_{DAG_NUMBER}',
    default_args=default_args,
    catchup=False,
    start_date=YESTERDAY,
    schedule_interval='15 1,6,10,18 * * *',
    tags = ['load','bitrix','api'],
) as dag:

    def _call_bx_method(method, data):
        def _call_bx(method, data):
            return requests.post(
                url=f'{BX_WH_URL}/{method}',
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

    async def item_list_batch(start_date_modify,filters, fields, page_idx=0):
        data = []
        data.append('halt=0')

        while page_idx < BX_BATCH_COMMANDS:
            cmd = 'cmd[page_{}]=crm.contact.list'.format(page_idx)
            cmd_args = []
            for idx, field in enumerate(fields):
                cmd_args.append('select[{}]={}'.format(idx, field))
            for key, value in filters.items():
                cmd_args.append('filter[{}]={}'.format(key, value))
            cmd_args.append('order[DATE_MODIFY]=ASC')
            cmd_args = quote_plus('?' + '&'.join(cmd_args))
            if page_idx == 0:
                if start_date_modify:
                    cmd_args += quote_plus('&filter[>DATE_MODIFY]=') + str(start_date_modify)
            else:
                cmd_args += quote_plus('&filter[>DATE_MODIFY]=') + "$result[page_{}][{}][DATE_MODIFY]".format(
                    page_idx - 1, BX_PAGE_SIZE - 1)
            # if page_idx > 0:
            #     cmd_args += quote_plus('&start={}'.format(page_idx * BX_PAGE_SIZE))
            cmd_args += quote_plus('&start=-1')
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

    def get_main_last_item():
        try:
            result = client.query(f"SELECT MAX(DATE_MODIFY) FROM `southern-idea-345503.raw_data_bitrix.contact`").result()
            return next(result)[0]
        except google.api_core.exceptions.NotFound:
            return None

    def get_last_item():
        try:
            result = client.query(f"SELECT MAX(DATE_MODIFY) FROM `{TABLE_NAME}`").result()
            return next(result)[0]
        except google.api_core.exceptions.NotFound:
            return None

    def load_items():
        iteration = 0

        while iteration < MAX_RUN_ITERATIONS:
            iteration += 1
            print('iteration =', iteration)

            last_item = get_last_item()
            last_item_main = get_main_last_item()
            print('last_item')
            print(last_item)
            date_modify = last_item if last_item else last_item_main
            date_modify = date_modify - datetime.timedelta(seconds=30)
            date_modify = date_modify.strftime('%Y-%m-%dT%H:%M:%S.%f%Z')

            filters = {
                '>DATE_MODIFY': date_modify
            }
            print('filters')
            print(filters)

            fields = [
                'ID', 'HONORIFIC', 'NAME', 'SECOND_NAME', 'LAST_NAME', 'PHOTO', 'BIRTHDATE', 'TYPE_ID', 'SOURCE_ID',
                'SOURCE_DESCRIPTION', 'POST', 'ADDRESS', 'ADDRESS_2', 'ADDRESS_CITY', 'ADDRESS_POSTAL_CODE',
                'ADDRESS_REGION', 'ADDRESS_PROVINCE', 'ADDRESS_COUNTRY', 'ADDRESS_COUNTRY_CODE', 'ADDRESS_LOC_ADDR_ID',
                'COMMENTS', 'OPENED', 'EXPORT', 'HAS_PHONE', 'HAS_EMAIL', 'HAS_IMOL', 'ASSIGNED_BY_ID', 'CREATED_BY_ID',
                'MODIFY_BY_ID', 'DATE_CREATE', 'DATE_MODIFY', 'COMPANY_ID', 'COMPANY_IDS', 'LEAD_ID', 'ORIGINATOR_ID',
                'ORIGIN_ID', 'ORIGIN_VERSION', 'FACE_ID', 'UTM_SOURCE', 'UTM_MEDIUM', 'UTM_CAMPAIGN', 'UTM_CONTENT',
                'UTM_TERM', 'PHONE', 'EMAIL', 'WEB', 'IM', 'LINK', 'UF_CRM_5D08C658673BD', 'UF_CRM_5D0F0DD726DEE',
                'UF_CRM_1562419394', 'UF_CRM_1562419402', 'UF_CRM_1562419454', 'UF_CRM_1562419966',
                'UF_CRM_5D4D681E3F0AC',
                'UF_CRM_5D4D681E90CDF', 'UF_TAG_TEXT', 'UF_TAG', 'UF_CRM_5D6770B403C20', 'UF_CRM_5D677EB8DDD75',
                'UF_CRM_5D677EB9511D3', 'UF_CRM_5D6A7FCA976B1', 'UF_CRM_5D6B6BC05327E', 'UF_CRM_1567343175',
                'UF_CRM_1567343187', 'UF_CRM_1567343200', 'UF_CRM_5D6E91283EBE3', 'UF_CRM_5DA48B0A58DE1',
                'UF_CRM_1571213287759', 'UF_CRM_5E5B80F310219', 'UF_CRM_5E5B80F4EE7E8', 'UF_CRM_5E68AABC8AD31',
                'UF_CRM_5E707E4C44043', 'UF_CRM_5E805CA994C52', 'UF_CRM_5EA058D259A09', 'UF_CRM_5FAAA77B2E5C0',
                'UF_CRM_FBERP_IGNORE', 'UF_CRM_1616834435540', 'UF_CRM_1622706693', 'UF_CRM_61531BDDC4F52',
                'UF_CRM_615E843A1FFD1', 'UF_CRM_615E845528600', 'UF_CRM_615E846E07861', 'UF_CRM_615E848971BF4',
                'UF_CRM_615E85F82FD99', 'UF_CRM_615E86162ACA5', 'UF_CRM_615E8635949A9', 'UF_CRM_615E8653D52DA',
                'UF_CRM_615E86770155B', 'UF_CRM_615E8692B8B24', 'UF_CRM_615E8698A1435', 'UF_CRM_615E86BC577AF',
                'UF_CRM_615E86DDE1335', 'UF_CRM_615E86FC36361', 'UF_CRM_615E871DBEE48', 'UF_CRM_615E87396ABE6',
                'UF_CRM_615E87473E89B', 'UF_CRM_615E874FB8768', 'UF_CRM_615E876565B11', 'UF_CRM_615E877CDA7A6',
                'UF_CRM_BITCONF_LINK', 'UF_CRM_BITCONF_ZOOM_RECORDINGS', 'UF_CRM_BITCONF_RECORDINGS_VIDEO_YANDEX',
                'UF_CRM_BITCONF_RECORDINGS_AUDIO_YANDEX', 'UF_CRM_BITCONF_RECORDINGS_CHAT_YANDEX',
                'UF_CRM_BITCONF_RECORDINGS_VIDEO_GOOGLE', 'UF_CRM_BITCONF_RECORDINGS_AUDIO_GOOGLE',
                'UF_CRM_BITCONF_RECORDINGS_CHAT_GOOGLE', 'UF_CRM_BITCONF_ZOOM_REGISTRATION_ANSWERS_JSON',
                'UF_CRM_BITCONF_ZOOM_REGISTRATION_ANSWERS', 'UF_CRM_BITCONF_MEETING_ATTENDED',
                'UF_CRM_BITCONF_MEETING_STARTED_AT', 'UF_CRM_BITCONF_MEETING_ENDED_AT', 'UF_CRM_61685405CC4DF',
                'UF_CRM_61685406B64AF', 'UF_CRM_6168540766E52', 'UF_CRM_619B700722839', 'UF_CRM_619B701E4354F',
                'UF_CRM_1642311422', 'UF_CRM_1642311460', 'UF_CRM_1642311493', 'UF_CRM_1641238113299',
                'UF_CRM_1641238127924', 'UF_CRM_620B6329B9EF9', 'UF_CRM_620B6336382DB', 'UF_CRM_620B636811407',
                'UF_CRM_620B63840694B', 'UF_CRM_1645377728837', 'UF_CRM_1645377764281', 'UF_CRM_1645377794610',
                'UF_CRM_BITCONF_PARTICIPANT_JOINED_AT', 'UF_CRM_BITCONF_PARTICIPANT_LEFT_AT', 'UF_CRM_6245A6AB323CD',
                'UF_CRM_6245A6DBB8564', 'UF_CRM_624C482D7C158', 'UF_CRM_624DA9CEAD225', 'UF_CRM_5D2F5043965AB',
                'UF_CRM_5D8BC3F469FF4', 'UF_CRM_5DC11F9457325', 'UF_CRM_5DC11F9CBA0A0', 'UF_CRM_5DC11F9EDB3E6',
                'UF_CRM_5DC11FACEE457', 'UF_CRM_5DC11FB249D18', 'UF_CRM_5DC11FB4712AB', 'UF_CRM_5DC11FB6AE009',
                'UF_CRM_5DC11FB7BF279', 'UF_CRM_5E805CAB3F405', 'UF_CRM_5E81CF338E905', 'UF_CRM_5E81CF538FC7B',
                'UF_CRM_5D9452E058796', 'UF_CRM_1615968803', 'UF_CRM_5D9452E1593C1', 'UF_CRM_1668491070', 
                'UF_CRM_INSTAGRAM', 'UF_CRM_TELEGRAM', 'UF_CRM_TIKTOK', 'UF_CRM_I2CRM_CHAT', 'UF_CRM_FBERP_STATUS', 
                'UF_CRM_VKONTAKTE', 'UF_CRM_63A2E6F9B82ED', 'UF_CRM_63A2E7148B1FD', 'UF_CRM_1672212957', 
                'UF_CRM_RS_UTM_CAMP', 'UF_CRM_RS_GOOGLE_CID', 'UF_CRM_RS_UTM_MEDIUM', 'UF_CRM_RS_UTM_SOURCE', 
                'UF_CRM_RS_UTM_TERM', 'UF_CRM_RS_UTM_CONT', 'UF_CRM_DUPLICATE_TEST', 'UF_CRM_DUPLICATE_TEST_BOOLEAN', 
                'UF_TYPE_ID_NEW', 'UF_OBSERVERS_PART'
            ]

            items = asyncio.run(
                item_list_batch(start_date_modify=date_modify,filters=filters, fields=fields, page_idx=0)
            )
            print("Webhook query = ", BX_WH_URL)
            print("Elements in response = ", len(items))

            simple_types = (str, int, float,)
            force_int_fields = (
                'ID',  # None
                'ADDRESS_LOC_ADDR_ID',  # None
                'FACE_ID',  # None
                'UF_CRM_61685405CC4DF',  # UF_CRM_DEAL_ID
                'UF_CRM_61685406B64AF',  # UF_CRM_DEAL_ID
                'UF_CRM_6168540766E52',  # UF_CRM_DEAL_ID
                'LEAD_ID',
                'COMPANY_ID',
                'ASSIGNED_BY_ID',
                'CREATED_BY_ID',
                'MODIFY_BY_ID',
                'HONORIFIC',
            )
            force_datetime_fields = (
                'DATE_CREATE', 'DATE_MODIFY',
            )
            force_date_fields = (
                'BIRTHDATE',  # None
                'UF_CRM_1567343187',  # Residency ID expiry date
                'UF_CRM_1567343200',  # Residency Visa expiry   date
                'UF_CRM_5E68AABC8AD31',  # Lead Added Date (auto)
                'UF_CRM_1641238127924',  # Residency Expiry Date
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
                    elif field in force_date_fields:
                        if value:
                            item[field] = parser.parse(value)
                        else:
                            item[field] = None
                    elif isinstance(value, simple_types):
                        item[field] = str(value)
                    else:
                        item[field] = json.dumps(value)
                for field in fields:
                    if field not in item:
                        item[field] = None
            print(len(items))

            src_table_schema = []
            for field in fields:
                if field in force_int_fields:
                    src_table_schema.append({'name': field, 'type': 'INTEGER'})
                elif field in force_date_fields:
                    src_table_schema.append({'name': field, 'type': 'TIMESTAMP'})
                elif field in force_datetime_fields:
                    src_table_schema.append({'name': field, 'type': 'TIMESTAMP'})
                else:
                    src_table_schema.append({'name': field, 'type': 'STRING'})

            try:
                client.get_table(TABLE_NAME)  # Make an API request.
                print(f"Table {TABLE_NAME} already exists.")
            except:
                schema = to_google_cloud_bigquery(dict(fields=src_table_schema))
                table = bigquery.Table(TABLE_NAME)
                table.schema = schema
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
                print(len(items), '<', BX_PAGE_SIZE * BX_BATCH_COMMANDS)
                break

        return True

    load_items_task = PythonOperator(
        task_id='load_items_task',
        python_callable=load_items,
    )

    update_contact = BigQueryInsertJobOperator(
        task_id='update_contact',
        gcp_conn_id='google_cloud_default',
        configuration={
            "query": {
                "query":
                f"""
                    DELETE 
                    FROM `southern-idea-345503.raw_data_bitrix.contact`
                    WHERE ID IN (SELECT DISTINCT ID FROM `southern-idea-345503.raw_data_bitrix.contact_daily_copy`);

                    INSERT INTO `southern-idea-345503.raw_data_bitrix.contact`
                    SELECT DISTINCT * EXCEPT(SOURCE_ID),SOURCE_ID FROM  `southern-idea-345503.raw_data_bitrix.contact_daily_copy`;

                    TRUNCATE TABLE `southern-idea-345503.raw_data_bitrix.contact_daily_copy`;
                    
                """,
            "useLegacySql": False
            }
        }
    )

    load_items_task >> update_contact