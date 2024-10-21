import datetime
import logging

from airflow import models
from airflow.operators.python_operator import PythonOperator

from common.common_function import task_fail_telegram_alert

'''
CREATE TABLE `southern-idea-345503`.raw_data.moizvonki_data_api_dag_1 (
    direction INT64,
    user_account STRING(65535),
    client_number STRING(65535),
    start_time TIMESTAMP,
    src_number STRING(65535),
    duration INT64,
    user_id INT64,
    answer_time TIMESTAMP,
    upload_time TIMESTAMP,
    src_id STRING(65535),
    client_name STRING(65535),
    src_slot STRING(65535),
    recording STRING(65535),
    answered INT64,
    db_call_id INT64,
    event_pbx_call_id STRING(65535),
    end_time TIMESTAMP
);
'''

default_args = {
    'owner': 'Composer Example',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'on_failure_callback': task_fail_telegram_alert
}

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
SOURCE_NAME = 'moizvonki'
DATASET = 'raw_data'
PROJECT = 'southern-idea-345503'
DAG_NUMBER = '1'
TABLE_NAME = f'{PROJECT}.{DATASET}.{SOURCE_NAME}_data_api_dag_{DAG_NUMBER}'

################################## Parametrs. Don't edit it! ##################################
with models.DAG(
        dag_id=f'{DATASET}_{SOURCE_NAME}_dag_{DAG_NUMBER}',
        default_args=default_args,
        catchup=False,
        start_date=YESTERDAY,
        schedule_interval=datetime.timedelta(minutes=15),
        tags = ['load','moizvonki','api'],
) as dag:
    ####################################################################
    def receive_data():
        from airflow.hooks.base_hook import BaseHook
        from google.cloud import bigquery

        import requests
        import json
        import pandas

        import db_dtypes
        from decimal import Decimal
        import numpy as np

        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

        def optimize_df_to_bq(dataframe, table_schema):
            dtype_map = {
                'INTEGER': "Int64",
                'FLOAT': np.dtype(float),
                'NUMERIC': Decimal,
                'DECIMAL': Decimal,
                'BIGNUMERIC': Decimal,
                'BIGDECIMAL': Decimal,
                'TIME': db_dtypes.TimeDtype(),
                'DATE': db_dtypes.DateDtype(),
                'DATETIME': "datetime64[ns]",
                'TIMESTAMP': "datetime64[s]",
                "BOOLEAN": "boolean",
            }

            new_df = pandas.DataFrame()
            for field in table_schema:
                field_type = field.field_type.upper()
                field_name = field.name
                dtype = dtype_map.get(field_type)
                if dtype:
                    if pandas.api.types.is_object_dtype(dataframe[field_name]):
                        if field_type in {"NUMERIC", "DECIMAL", "BIGNUMERIC", "BIGDECIMAL"}:
                            dataframe[field_name] = dataframe[field_name].map(Decimal, na_action='ignore')
                        else:
                            dataframe[field_name] = dataframe[field_name].astype(dtype, errors="ignore")
                    else:
                        dataframe[field_name] = dataframe[field_name].astype(dtype)
                else:
                    dataframe[field_name] = dataframe[field_name].apply(lambda x: str(x) if x else None)
                new_df[field_name] = dataframe[field_name]
            return new_df

        current_offset = 1

        query_deal = f"""SELECT MAX(db_call_id) FROM `{TABLE_NAME}`;"""
        from_id = str(pandas.read_gbq(query_deal, PROJECT).iloc[0, 0])

        moizvonki_hook = BaseHook.get_connection('moizvonki')
        moizvonki_user = moizvonki_hook.login
        moizvonki_token = moizvonki_hook.password
        moizvonki_host = moizvonki_hook.host
        headers = {"Content-Type": "application/json"}
        # start_date = int(round(datetime(2023,1,1).timestamp()))
        default_size = 100

        result_data_json = []
        while True:
            body = {"user_name": moizvonki_user, "api_key": moizvonki_token,
                    "action": "calls.list", "from_id": from_id, "max_results": default_size,
                    "from_offset": current_offset, "supervised": 1}
            request = json.dumps(body, ensure_ascii=False).encode('utf8')
            result = requests.post(moizvonki_host, request, headers=headers, timeout=100)
            result_data_json += result.json()['results']
            current_size = len(result_data_json)

            rests = result.json()['results_remains']
            if current_size >= 10000 or (not rests and current_size):
                bq_client = bigquery.Client()
                table = bq_client.get_table(TABLE_NAME)

                final_df = pandas.DataFrame(result_data_json)
                final_df = optimize_df_to_bq(dataframe=final_df, table_schema=table.schema)
                final_df.reset_index(drop=True, inplace=True)

                # Set the job config
                job_config = bigquery.LoadJobConfig()
                job_config.write_disposition = 'WRITE_APPEND'
                job_config.schema = table.schema
                job_config.autodetect = False
                
                logging.info(f"{final_df.shape[0]} строк.")
                # Write to BQ
                load = bq_client.load_table_from_dataframe(
                    dataframe=final_df, destination=TABLE_NAME,
                    job_config=job_config)
                load.result()
                result_data_json = []
                logging.info("Данные успешно записаны в BigQuery.")
                result_data_json = []

            if not rests:
                break
            current_offset = result.json()['results_next_offset']
            logging.info(f"- Текущее количество строк на запись {str(current_offset)}")
        return


    receive_data = PythonOperator(
        task_id='receive_data',
        python_callable=receive_data,
        dag=dag,
    )

    receive_data