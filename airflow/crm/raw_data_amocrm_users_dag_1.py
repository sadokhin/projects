"""
load data about users from users in amocrm and push this data into google bigquery
"""

import asyncio
import datetime
import pandas
import pandas_gbq
import requests
import time
import json
from google.cloud import bigquery
import google.auth
import logging

from airflow import models
from airflow.operators.python_operator import PythonOperator

# Step 1. Parameters. Edit it
SOURCE_NAME = 'users'
DAG_NUMBER = '1'

################################## Parameters. Don't edit it! ##################################

# Параметры подключения к BQ
LOCATION = 'europe-west3'
PROJECT = 'intermark-analytics-prod'
DATASET = 'raw_data_amocrm'
TABLE = 'users'
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
MAX_RUN_ITERATIONS = 100

# Таблицы для хранения данных
TABLE_NAME = f'{PROJECT}.{DATASET}.{TABLE}'

AMO_REFRESH_TOKEN = {
    'client_id' : models.Variable.get('amocrm_client_id'),
    'client_secret' : models.Variable.get('amocrm_client_secret'),
    'grant_type' : 'refresh_token',
    'refresh_token' : models.Variable.get('amocrm_refresh_token'),
    'redirect_uri' : models.Variable.get('amocrm_redirect_uri')
}
AMO_DOMAIN = models.Variable.get('amocrm_domain')

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
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
}

################################## Parametrs. Don't edit it! ##################################
with models.DAG(
    dag_id=f'{DATASET}_{SOURCE_NAME}_dag_{DAG_NUMBER}',
    default_args=default_args,
    description = __doc__,
    catchup=False,
    start_date=YESTERDAY,
    schedule_interval='5 2 * * *',
    tags = ['load','amocrm','api','rewrite'],
) as dag:

    def load_data_to_bq(table_name, dataframe, mode='WRITE_APPEND'):
        from google.api_core.exceptions import ClientError
        # Drop index from the pandas df
        dataframe.reset_index(drop=True, inplace=True)
        # Set the job config
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = mode
        job_config.schema = client.get_table(table_name).schema
        job_config.autodetect = False
        #job_config.field_delimiter = '#'
        job_config.max_bad_records = 3
        job_config.source_format = bigquery.SourceFormat.CSV
        # Write to BQ
        try:
            load = client.load_table_from_dataframe(
                dataframe,
                table_name,
                job_config=job_config
            )
            load.result()
            logging.info(f'{table_name}: Data successfuly downloaded to target table')
        except ClientError:
            for error in load.errors:
                print(f'{table_name}: {error}') 

        return 

    def optimize_df_to_bq(table_name, dataframe):
        import pandas as pd
        import numpy as np
        from decimal import Decimal

        table_info = bigquery.get_table(table=table_name)
        table_schema = table_info.schema
        schema_columns = {field.name: field for field in table_schema}

        dtype_map = {
            'INTEGER': 'Int64',
            'FLOAT': np.dtype(float),
            'NUMERIC': Decimal,
            'DECIMAL': Decimal,
            'BIGNUMERIC': Decimal,
            'BIGDECIMAL': Decimal,
            'TIME': pd.StringDtype(),
            'DATE': pd.StringDtype(),
            'DATETIME': "datetime64[ns]",
            'TIMESTAMP': "datetime64[s]",
            'BOOLEAN': "boolean",
        }

        for field_name, field in schema_columns.items():
            field_type = field.field_type
            dtype = dtype_map.get(field_type)

            if dtype:
                if field_type in ['NUMERIC', 'DECIMAL', 'BIGNUMERIC', 'BIGDECIMAL']:
                    dataframe[field_name] = dataframe[field_name].applymap(dtype)
                elif field_type in ['TIME', 'DATE', 'DATETIME', 'TIMESTAMP']:
                    dataframe[field_name] = pd.to_datetime(dataframe[field_name], errors='coerce',
                                                        infer_datetime_format=True)            
                else:
                    dataframe[field_name] = dataframe[field_name].astype(dtype, errors='ignore')
            else:
                dataframe.loc[:, field_name] = dataframe.loc[:, field_name].replace(
                    {pd.NA: None, pd.NaT: None, np.NaN: None}).apply(lambda x: str(x) if x else None)

        dataframe = dataframe[schema_columns.keys()]
        logging.info(f'{table_name}: All columns were formatted to BigQuery data types')
        return dataframe

    def authorize():
        token_url = AMO_DOMAIN + 'oauth2/access_token'
        token_request = requests.post(token_url, data=AMO_REFRESH_TOKEN)
        request_dict = json.loads(token_request.text)
        print(token_request.status_code, token_request.text)
        access_token = request_dict['access_token']
        refresh_token = request_dict['refresh_token']

        #save value of new refresh token to airflow variable
        logging.info('access_token={}, refresh_token={}'.format(access_token, refresh_token))
        models.Variable.set('amocrm_refresh_token', refresh_token)
        models.Variable.set('amocrm_access_token', access_token)
        
        return access_token

    def _call_amo_method(method, data):
        def _call_amo(method, data,access_token):
            headers = {'Authorization': 'Bearer ' + access_token}
            return requests.get(
                url='{}api/v4/{}'.format(AMO_DOMAIN, method),
                headers=headers,
                params=data,
                verify=True
            )

        response = None
        access_token = models.Variable.get('amocrm_access_token')
        call_delay = 1
        while call_delay < 3:
            print(call_delay, " try")
            response = _call_amo(method, data, access_token)
            if response.ok:
                return response
            elif response.reason == 'Unauthorized':
                access_token = authorize()
            else:
                print(response.reason, response.status_code,response.text)
                    
            time.sleep(15)
            call_delay += call_delay
        return response

    async def user_list():
        iteration = 0

        response = _call_amo_method('users', '')
        data_resp = response.json()
        users = data_resp['_embedded']['users']
        while 'next' in data_resp.get('_links') and iteration < MAX_RUN_ITERATIONS:
            method = data_resp.get('_links').get('next').get('href').split('api/v4/')[1]
            response = _call_amo_method(method, '')
            logging.info('next page: {} downloaded.'.format(method))
            data_resp = response.json()
            users.extend(data_resp['_embedded']['users'])
            iteration += 1
        return users

    def load_users():
        users = asyncio.run(user_list())
        print(len(users))

        df = pandas.json_normalize(users)
        # Drop the original '_embeded' column and '_links' and 'custom_fields_values'
        df.drop(['_links.self.href'], axis=1, inplace=True)
        df.columns = df.columns.str.replace('.', '_')  

        load_data_to_bq(TABLE_NAME,df, mode = 'WRITE_TRUNCATE')

        return True

    update_users = PythonOperator(
        task_id='update_users',
        python_callable=load_users,
    )

    update_users
