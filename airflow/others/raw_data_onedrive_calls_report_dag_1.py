"""
load data Excel File in OneDrive and push this data into raw layer google bigquery
"""

import datetime
import pandas as pd
import pandas_gbq
import requests
from google.cloud import bigquery
import google.auth
import json

from airflow import models
from airflow.operators.python_operator import PythonOperator

################################## Parameters. Don't edit it! ##################################

SOURCE_NAME = 'excel_calls_report'
DAG_NUMBER = '1'
LOCATION = 'europe-west3'
PROJECT = 'intermark-analytics-prod'
DATASET = 'raw_data'
TABLE = 'excel_calls_report'
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
TABLE_NAME = f'{PROJECT}.{DATASET}.{TABLE}'
WORKSHEETS_MONTH = ['May 2024','June 2024']

### Mircosoft GRAPH API
tenant_id = models.Variable.get('microsoft_tenant_id')
client_id = models.Variable.get('microsoft_client_id') 
client_secret = models.Variable.get('microsoft_client_secret') 
item_id = ''
site_id = ''

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
    'email': ['v.makaryev@intermark.global'],
    'email_on_failure': False,
    'email_on_retry': False,
}

################################## Parametrs. Don't edit it! ##################################
with models.DAG(
    dag_id=f'{DATASET}_{SOURCE_NAME}_dag_{DAG_NUMBER}',
    default_args=default_args,
    description = __doc__,
    catchup=False,
    start_date=YESTERDAY,
    schedule_interval='30 2 * * *',
    tags = ['load','onedrive','api','rewrite'],
) as dag:

    def load_data_to_bq(table_name, dataframe, mode='WRITE_APPEND'):
        from google.api_core.exceptions import ClientError
        # Drop index from the pandas df
        dataframe.reset_index(drop=True, inplace=True)
        # Set the job config
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = mode
        #job_config.schema = client.get_table(table_name).schema
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
            print(f'{table_name}: Data successfuly downloaded to target table')
        except ClientError:
            for error in load.errors:
                print(f'{table_name}: {error}')
        
        return

    def authorization():
        # Obtain access token
        token_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'
        token_data = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret,
            'scope': 'https://graph.microsoft.com/.default'
        }
        token_response = requests.post(token_url, data=token_data)
        token_content = token_response.json()

        access_token = token_content['access_token']
        headers = {
            'Authorization': f'Bearer {access_token}'
        }
        return headers
        
    def excel_serial_to_datetime(serial):
        # Check if the value is an Excel serial number
        if isinstance(serial, int) or isinstance(serial, float):
            # Convert Excel serial number to datetime
            return pd.to_datetime('1899-12-30') + pd.to_timedelta(serial, 'D')
        else:
            # Return the original value if it's already a datetime
            return pd.to_datetime(serial)
    def get_worksheets_from_file(headers):
        get_worksheets_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{item_id}/workbook/worksheets"
        response = requests.get(get_worksheets_url, headers=headers)
        
        if response.status_code == 200:
            worksheets = json.loads(response.text)
            ids = [worksheet["id"] for worksheet in worksheets["value"] if worksheet["name"] in WORKSHEETS_MONTH]
        else:
            raise Exception(f'Error getting file: {response.status_code}, {response.text}')

        return ids
    
    def load_one_drive_file():
        headers = authorization()
        ids = get_worksheets_from_file(headers)

        df_call_qc = pd.DataFrame()
        for id in ids:
            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{item_id}/workbook/worksheets/{id}/usedRange"
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                df = pd.DataFrame(json.loads(response.text).get('values'))
                df.columns = df.iloc[0]
                df = df.drop(0).reset_index(drop=True)
                df = df[['Caller','Lead','Language','Date','Length','Greeting','Primary qualification','Additional qualification','Invitation to UM','Goodbye','Speech','Emotional tone and politeness']]
                df = df[df['Lead'] != '']
                df['Lead'] = df['Lead'].astype('int64')
                df[['Caller','Language','Date','Length','Greeting','Primary qualification','Additional qualification','Invitation to UM','Goodbye','Speech','Emotional tone and politeness']] = df[['Caller','Language','Date','Length','Greeting','Primary qualification','Additional qualification','Invitation to UM','Goodbye','Speech','Emotional tone and politeness']].astype('str')
                df_call_qc = pd.concat([df_call_qc,df])
            else:
                raise Exception(f'Error getting file: {response.status_code}, {response.text}')
        
        # Convert 'datecolumn' to datetime
        df_call_qc['date_column'] = df_call_qc['Date'].apply(excel_serial_to_datetime)    
        print("File was downloaded, shape: ", df_call_qc.shape)
        load_data_to_bq(TABLE_NAME,df_call_qc, mode = 'WRITE_TRUNCATE')

        return True

    update_calls_report = PythonOperator(
        task_id='update_calls_report',
        python_callable=load_one_drive_file,
    )

    update_calls_report
