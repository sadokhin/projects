"""
load data with mapping [lead id -> contact id] from contacts in amocrm and push this data into google bigquery
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

################################## Parameters ##################################

# Common Parameters
DAG_NUMBER = '1'
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
MAX_RUN_ITERATIONS = 100

# BigQuery Parameters
LOCATION = 'europe-west3'
PROJECT = 'intermark-analytics-prod'
DATASET = 'raw_data_amocrm'
TABLE = 'contacts'
TABLE_NAME = f'{PROJECT}.{DATASET}.{TABLE}'
credentials, project = google.auth.default(
    scopes=[
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/cloud-platform",
    ]
)
client = bigquery.Client(credentials=credentials,project=project)

# AmoCRM Parameters
AMO_REFRESH_TOKEN = {
    'client_id' : models.Variable.get('amocrm_client_id'),
    'client_secret' : models.Variable.get('amocrm_client_secret'),
    'grant_type' : 'refresh_token',
    'refresh_token' : models.Variable.get('amocrm_refresh_token'),
    'redirect_uri' : models.Variable.get('amocrm_redirect_uri')
}
AMO_DOMAIN = models.Variable.get('amocrm_domain')

#DAG Parameters
default_args = {
    'owner': 'Composer Example',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
}

################################## Parametrs. Don't edit it! ##################################
with models.DAG(
    dag_id=f'{DATASET}_{TABLE}_dag_{DAG_NUMBER}',
    default_args=default_args,
    description = __doc__,
    catchup=False,
    start_date=YESTERDAY,
    schedule_interval='0 3-19/2 * * *',
    tags = ['load','amocrm','api','upsert'],
) as dag:

    def extract_custom_fields(row):
        """
        extract fields and values from json field which contains a lot of fields
        """
        values_dict = {}
        if row:
            for field in row:
                field_name = "CF_ID_" + str(field['field_id'])
                field_value = field['values'][0]['value']
                values_dict[field_name] = field_value
        return pandas.Series(values_dict)

    def sorted_dataframe_for_bq(table_name,dataframe):
        """
        match with scema table: if leads didnt have custom fields - add column with empty values
        """
        # Fetch the table schema
        table = client.get_table(table_name)

        # Extract column names from the schema
        column_names = [field.name for field in table.schema]
        keep_columns = set(column_names).intersection(dataframe.columns)
        dataframe = dataframe[list(keep_columns)]
        missing_columns = set(column_names) - set(dataframe.columns)
        for col in missing_columns:
            dataframe[col] = None
        dataframe = dataframe[column_names]
        return dataframe
    
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
                if pandas.api.types.is_object_dtype(dataframe[field_name]):
                    if field_type in {"NUMERIC", "DECIMAL", "BIGNUMERIC", "BIGDECIMAL"}:
                        dataframe[field_name] = dataframe[field_name].map(dtype, na_action='ignore')
                    elif field_type in {'TIME', 'DATE', 'DATETIME', 'TIMESTAMP'}:
                        dataframe[field_name] = pandas.to_datetime(dataframe[field_name], errors='coerce').astype(dtype, errors="ignore")
                    else:
                        dataframe[field_name] = dataframe[field_name].astype(dtype, errors="ignore")
                        #print(f"{field_name}: dtype:{dtype} {field_type}: {dataframe[field_name].tolist()}")
                else:
                    if field_type in {"NUMERIC", "DECIMAL", "BIGNUMERIC", "BIGDECIMAL"}:
                        dataframe[field_name] = dataframe[field_name].map(dtype)
                    elif field_type in {'TIME', 'DATE', 'DATETIME', 'TIMESTAMP'}:
                        dataframe[field_name] = pandas.to_datetime(dataframe[field_name], errors='coerce').astype(dtype, errors="ignore")
                    else:
                        dataframe[field_name] = dataframe[field_name].replace('', pandas.NA)
                        dataframe[field_name] = dataframe[field_name].astype(dtype).replace({pandas.NA: None, pandas.NaT: None, np.NaN: None})
            else:
                try:
                    dataframe[field_name] = dataframe[field_name].replace({pandas.NA: None, pandas.NaT: None, np.NaN: None, np.nan: None}).apply(lambda x: str(x) if x else None)
                except:
                    dataframe[field_name] = dataframe[field_name].astype(str)

        dataframe = dataframe[schema_columns.keys()]
        logging.info(f'{table_name}: All columns were formatted to BigQuery data types')
        return dataframe

    def get_last_item():
        try:
            result = client.query(f"SELECT MAX(updated_at) FROM `{TABLE_NAME}`").result()
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

    async def contacts_list():
        iteration = 0

        last_updated_at = get_last_item()
        logging.info(f'Last updated at: {last_updated_at}; Type: {type(last_updated_at)}')
        last_item = str(int(last_updated_at)-100) if last_updated_at else "1704067200"
        data = F"with=leads&filter[updated_at][from]={last_item}&order[updated_at]=asc"
        response = _call_amo_method('contacts', data)
        data_resp = response.json()
        leads = data_resp['_embedded']['contacts']
        while 'next' in data_resp.get('_links') and iteration < MAX_RUN_ITERATIONS:
            method = data_resp.get('_links').get('next').get('href').split('api/v4/')[1]
            response = _call_amo_method(method, '')
            logging.info('next page: {} downloaded.'.format(method))
            data_resp = response.json()
            leads.extend(data_resp['_embedded']['contacts'])
            iteration += 1
        return leads

    def load_contacts():
        contacts = asyncio.run(contacts_list())
        print(len(contacts))

        df = pandas.json_normalize(contacts)

        # def extract_and_join_names(embeded,field):
        #     return ','.join(tag[{field}] for tag in embeded)
        
        df['tags'] = df['_embedded.tags'].apply(lambda x: ','.join(tag['name'] for tag in x))
        df['lead_ids'] = df['_embedded.leads'].apply(lambda x: ','.join(str(lead['id']) for lead in x))

        df_custom_columns = df['custom_fields_values'].apply(extract_custom_fields)
        df = pandas.concat([df, df_custom_columns], axis=1)
        
        df.columns = df.columns.str.replace('.', '_')

        # Sorted columns for correct append
        df = sorted_dataframe_for_bq(table_name=TABLE_NAME, dataframe=df)
        logging.info('{}: sorted columns'.format(TABLE_NAME))

        # Change dataframe type
        df = optimize_df_to_bq(table_name=TABLE_NAME, dataframe=df)
        logging.info('{}: optimized datatypes.'.format(TABLE_NAME))

        # # Remove old record from table
        deduplicate_for_load(
            dataframe=df,
            table_name=TABLE_NAME,
            id_column_name='id')
        logging.info('{}: deduplicated.'.format(TABLE_NAME))          

        load_data_to_bq(TABLE_NAME,df, mode = 'WRITE_APPEND')

        return True

    update_contacts = PythonOperator(
        task_id='update_contacts',
        python_callable=load_contacts,
    )

    update_contacts
