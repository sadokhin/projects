from facebook_business.api import FacebookAdsApi
from facebook_business.exceptions import FacebookRequestError
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.adaccountuser import AdAccountUser as AdUser
from facebook_business import adobjects
import pandas, pandas_gbq
import pyarrow as pa

import datetime, time
from google.cloud import bigquery as bq

from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from common.common_function import task_fail_telegram_alert
import logging


start_date = str(datetime.date.today() - datetime.timedelta(days=20))
end_date = str(datetime.date.today() - datetime.timedelta(days=0))

################################## Parameters. Don't edit it! ##################################
SOURCE_NAME = 'facebook_api'
DAG_NUMBER = '2'
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
TODAY = datetime.datetime.now()
LOCATION = 'europe-west3'
PROJECT = 'southern-idea-345503'
DATASET = 'raw_data'
TABLE = f'{SOURCE_NAME}_dag_{DAG_NUMBER}'
temp_path = models.Variable.get('temp_path')
TABLE_NAME = f'{PROJECT}.{DATASET}.{TABLE}'

SECRET_FILE = {
    'app_id': '',
    'app_secret': models.Variable.get('facebook_app_secret'),
    'access_token': models.Variable.get('facebook_access_token'),
}

app_id = SECRET_FILE['app_id']
app_secret = SECRET_FILE['app_secret']
access_token = SECRET_FILE['access_token']

default_args = {
    'owner': 'Composer Example',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
    'on_failure_callback': task_fail_telegram_alert,
}
################################## Parametrs. Don't edit it! ##################################
with models.DAG(
    dag_id=f'{DATASET}_{SOURCE_NAME}_dag_{DAG_NUMBER}',
    default_args=default_args,
    catchup=False,
    start_date=YESTERDAY,
    schedule_interval='0 2 * * *',
    tags = ['load','facebook','api'],
) as dag:

    delete_old_facebook_records = BigQueryInsertJobOperator(
            task_id='delete_old_facebook_records',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query":
                    f"""
                        DELETE 
                        FROM 
                            southern-idea-345503.raw_data.facebook_api_dag_2
                        WHERE 
                            date >= '{start_date}'
                    """,
                "useLegacySql": False
            }
        }
    )


    def get_facebook_data():
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
        
        FacebookAdsApi.init(app_id, app_secret, access_token) 
        me = AdUser(fbid='me')
        my_accounts = list(me.get_ad_accounts()) 
        logging.info(my_accounts)
        fields = [
            AdsInsights.Field.ad_id,
            AdsInsights.Field.account_name,
            AdsInsights.Field.campaign_id,
            AdsInsights.Field.campaign_name,
            AdsInsights.Field.adset_name,
            AdsInsights.Field.adset_id,
            AdsInsights.Field.ad_name,
            AdsInsights.Field.impressions,
            AdsInsights.Field.clicks,
            AdsInsights.Field.spend,
            AdsInsights.Field.actions,
            AdsInsights.Field.account_id
        ]

        def wait_for_async_job(async_job):
            async_job = async_job.api_get()
            while async_job[AdReportRun.Field.async_status] != 'Job Completed' or async_job[
                AdReportRun.Field.async_percent_completion] < 100:
                time.sleep(2)
                async_job = async_job.api_get()
            return async_job.get_result()
        
        def types_mappo(table_name):
            import pandas as pd
            import numpy as np
            from decimal import Decimal
            
            client = bq.Client()
            table_info = client.get_table(table=table_name)
            table_schema = table_info.schema
            schema_columns = {field.name: field for field in table_schema}
            
            dtype_map = {
                'INTEGER': "int64",
                'FLOAT': "float",
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
            
            types_mapping = {}
            for field_name, field in schema_columns.items():
                field_type = field.field_type
                dtype = dtype_map.get(field_type, 'str')
                types_mapping[field_name] = dtype
            return types_mapping
        
        params = {'level': 'ad', 
            'time_range': {'since': start_date, 'until': end_date},
            'filtering' : [{'field':'action_type','operator':'IN','value':['lead']}],
            'time_increment': 1}

        df = pandas.DataFrame()
        for elem in my_accounts:
            account = AdAccount(elem["id"]).get_insights_async(params=params, fields=fields)
            df_temp = pandas.DataFrame([dict(item) for item in wait_for_async_job(account)])
            if not df_temp.empty:
                logging.info('id: {}, rows: {}'.format(elem['id'], df_temp.shape[0]))
                if 'actions' not in df_temp.columns:
                    df_temp['actions'] = ''
                df_temp = df_temp.explode('actions')
                df = pandas.concat([df, df_temp])
        logging.info('All data received.')
        df['actions'] = df['actions'].fillna({i: {} for i in df.index})
        df = df.join(pandas.json_normalize(df.actions)).drop(columns=['actions', 'date_stop', 'action_type']).rename(columns={'value':'conversions'})
        df['conversions'] = df['conversions'].fillna('0').astype('float').astype('int64')
        df['ad_id'] = df['ad_id'].astype('int64')
        df['campaign_id'] = df['campaign_id'].astype('int64')
        df['adset_id'] = df['adset_id'].astype('int64')
        df['impressions'] = df['impressions'].astype('int64')
        df['clicks'] = df['clicks'].fillna('0').astype('float').astype('int64')
        df['spend'] = df['spend'].astype('float')
        df = df.rename(columns={'ad_id':'adId', 'account_name':'accountName', 'campaign_id':'campaignId', 'campaign_name':'campaignName', 'adset_name':'adGroupName', 'adset_id':'adGroupId', 'ad_name':'adName', 'spend':'cost', 'date_start':'date', 'account_id':'accountId'})

        generated_schema = [{'name':'date','type': 'DATE'}]
        pandas_gbq.to_gbq(dataframe=df,
                      destination_table=TABLE_NAME,
                      project_id=PROJECT,
                      table_schema=generated_schema,
                      if_exists='append')
        logging.info('All data written.')
        return True
    
    get_data_from_source = PythonOperator(
        task_id='get_data_from_source',
        execution_timeout=datetime.timedelta(hours=1),
        python_callable=get_facebook_data,
    )
    
    delete_old_facebook_records >> get_data_from_source
