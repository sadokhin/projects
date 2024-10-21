"""
load data from events in amocrm and push this data into google bigquery about:
- out/in bound messages, 
- leads status changes 
- responsible for the leads changes 
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
SOURCE_NAME = 'events'
DAG_NUMBER = '1'

################################## Parameters. Don't edit it! ##################################

# Параметры подключения к BQ
LOCATION = 'europe-west3'
PROJECT = 'intermark-analytics-prod'
DATASET = 'raw_data_amocrm'
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
MAX_RUN_ITERATIONS = 100

# Таблицы для хранения данных
TABLE_NAME_STATUS_CHANGED = f'{PROJECT}.{DATASET}.lead_status_history'
TABLE_NAME_MESSAGES = f'{PROJECT}.{DATASET}.event_messages'
TABLE_NAME_RESP_CHANGED = f'{PROJECT}.{DATASET}.responsible_changed_history'

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
    schedule_interval='10 3-19/2 * * *',
    tags = ['load','amocrm','api','upsert'],
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

    def deduplicate_for_load(dataframe, table_name, id_column_name):
        ids = dataframe[id_column_name].tolist()
        if isinstance(ids[0], str):
            ids = [f"'{id}'" for id in ids]
        else:
            ids = list(map(str, ids))
        query = "delete from {} where {} in ({})".format(
            table_name,
            id_column_name,
            ','.join(ids))
        _ = client.query(query).result()
        return
    
    def optimize_df_to_bq(table_name, dataframe):
        import pandas as pd
        import numpy as np
        from decimal import Decimal

        table_info = client.get_table(table=table_name)
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

    def get_last_item(table_name):
        try:
            result = client.query(f"SELECT MAX(created_at) FROM `{table_name}`").result()
            return next(result)[0]
        except google.api_core.exceptions.NotFound:
            return None
        
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

    async def event_messages():
        iteration = 0

        last_updated_at = get_last_item(TABLE_NAME_MESSAGES)
        logging.info(f'Last updated at: {last_updated_at}; Type: {type(last_updated_at)}')
        last_item = str(int(last_updated_at)+1) if last_updated_at else "1704067200"
        data = F"filter[created_at][from]={last_item}&filter[type][]=incoming_chat_message&filter[type][]=outgoing_chat_message&order[created_at]=asc"
        response = _call_amo_method('events', data)
        data_resp = response.json()
        events = data_resp['_embedded']['events']
        while 'next' in data_resp.get('_links') and iteration < MAX_RUN_ITERATIONS:
            method = data_resp.get('_links').get('next').get('href').split('api/v4/')[1]
            response = _call_amo_method(method, '')
            logging.info('next page: {} downloaded.'.format(method))
            data_resp = response.json()
            events.extend(data_resp['_embedded']['events'])
            iteration += 1
        return events

    async def event_status_changed():
        iteration = 0

        last_updated_at = get_last_item(TABLE_NAME_STATUS_CHANGED)
        logging.info(f'Last updated at: {last_updated_at}; Type: {type(last_updated_at)}')
        last_item = str(int(last_updated_at)+1) if last_updated_at else "1704067200"
        data = F"filter[created_at][from]={last_item}&filter[type]=lead_status_changed&order[created_at]=asc"
        response = _call_amo_method('events', data)
        data_resp = response.json()
        events = data_resp['_embedded']['events']
        while 'next' in data_resp.get('_links') and iteration < MAX_RUN_ITERATIONS:
            method = data_resp.get('_links').get('next').get('href').split('api/v4/')[1]
            response = _call_amo_method(method, '')
            logging.info('next page: {} downloaded.'.format(method))
            data_resp = response.json()
            events.extend(data_resp['_embedded']['events'])
            iteration += 1
        return events


    async def event_responsible_changed():
        iteration = 0

        last_updated_at = get_last_item(TABLE_NAME_RESP_CHANGED)
        logging.info(f'Last updated at: {last_updated_at}; Type: {type(last_updated_at)}')
        last_item = str(int(last_updated_at)+1) if last_updated_at else "1704067200"
        data = F"filter[created_at][from]={last_item}&filter[type]=entity_responsible_changed&order[created_at]=asc"
        response = _call_amo_method('events', data)
        data_resp = response.json()
        events = data_resp['_embedded']['events']
        while 'next' in data_resp.get('_links') and iteration < MAX_RUN_ITERATIONS:
            method = data_resp.get('_links').get('next').get('href').split('api/v4/')[1]
            response = _call_amo_method(method, '')
            logging.info('next page: {} downloaded.'.format(method))
            data_resp = response.json()
            events.extend(data_resp['_embedded']['events'])
            iteration += 1
        return events


    def load_messages():
        events_messages = asyncio.run(event_messages())
        print(len(events_messages))

        df = pandas.json_normalize(events_messages)
        
        df['message_id'] = df['value_after'].apply(lambda x: x[0]['message']['id'] if x and x[0].get('message') else None)
        df['message_origin'] = df['value_after'].apply(lambda x: x[0]['message']['origin'] if x and x[0].get('message') else None)
        df['talk_id'] = df['value_after'].apply(lambda x: x[0]['message']['talk_id'] if x and x[0].get('message') else None)

        # Drop the original '_embeded' column and '_links' and 'custom_fields_values'
        df.drop(['value_after','value_before','account_id','_links.self.href','_embedded.entity.id','_embedded.entity._links.self.href'], axis=1, inplace=True)   

        #Change dataframe type
        df = optimize_df_to_bq(table_name=TABLE_NAME_MESSAGES, dataframe=df)
        logging.info('{}: optimized datatypes.'.format(TABLE_NAME_MESSAGES))    

        load_data_to_bq(TABLE_NAME_MESSAGES,df, mode = 'WRITE_APPEND')

        return True
    
    def load_status_changes():
        status_changes = asyncio.run(event_status_changed())
        print(len(status_changes))

        df = pandas.json_normalize(status_changes)
        
        df['status_after'] = df['value_after'].apply(lambda x: x[0]['lead_status']['id'] if x else None)
        df['pipeline_after'] = df['value_after'].apply(lambda x: x[0]['lead_status']['pipeline_id'] if x else None)
        df['status_before'] = df['value_before'].apply(lambda x: x[0]['lead_status']['id'] if x else None)
        df['pipeline_before'] = df['value_before'].apply(lambda x: x[0]['lead_status']['pipeline_id'] if x else None)

        # Drop the original '_embeded' column and '_links' and 'custom_fields_values'
        df.drop(['_links.self.href','type','entity_type','account_id','value_after','value_before','_embedded.entity._links.self.href','_embedded.entity.id'], axis=1, inplace=True)

        #Change dataframe type
        df = optimize_df_to_bq(table_name=TABLE_NAME_STATUS_CHANGED, dataframe=df)
        logging.info('{}: optimized datatypes.'.format(TABLE_NAME_STATUS_CHANGED))         

        load_data_to_bq(TABLE_NAME_STATUS_CHANGED,df, mode = 'WRITE_APPEND')

        return True

    def load_responsible_changes():
        resp_changes = asyncio.run(event_responsible_changed())
        print(len(resp_changes))

        df = pandas.json_normalize(resp_changes)
        
        df['responsible_after'] = df['value_after'].apply(lambda x: x[0]['responsible_user']['id'] if x else None)
        df['responsible_before'] = df['value_before'].apply(lambda x: x[0]['responsible_user']['id'] if x else None)

        # Drop the original '_embeded' column and '_links' and 'custom_fields_values'
        df.drop(['_links.self.href','type','account_id','value_after','value_before','_embedded.entity._links.self.href','_embedded.entity.id'], axis=1, inplace=True)

        #Change dataframe type
        df = optimize_df_to_bq(table_name=TABLE_NAME_RESP_CHANGED, dataframe=df)
        logging.info('{}: optimized datatypes.'.format(TABLE_NAME_RESP_CHANGED))         

        load_data_to_bq(TABLE_NAME_RESP_CHANGED,df, mode = 'WRITE_APPEND')

        return True

    update_status_changes = PythonOperator(
        task_id='update_status_changes',
        python_callable=load_status_changes,
    )

    update_responsible_changes = PythonOperator(
        task_id='update_responsible_changes',
        python_callable=load_responsible_changes,
    )

    update_messages = PythonOperator(
        task_id='update_messages',
        python_callable=load_messages,
    )

    update_status_changes >> update_responsible_changes >> update_messages
