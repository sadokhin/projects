import datetime
import os
import pandas
from google.cloud import bigquery, storage
import google.auth

from airflow import models
from airflow.operators.python_operator import PythonOperator
from common.common_function import task_fail_telegram_alert

# Step 1. Parameters. Edit it
SOURCE_NAME = 'yandex_leads_to_k_50'
DAG_NUMBER = '1'
LOCATION = 'europe-west3'
PROJECT = 'southern-idea-345503'
DATASET = 'external_data'
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

TABLE = f'{SOURCE_NAME}'
TABLE_NAME = f'{PROJECT}.{DATASET}.{TABLE}'
BUCKET = ''
blob_file = 'data/temp/external_data_yandex_leads_to_k_50.csv'

storage_client = storage.Client()

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

with models.DAG(
    dag_id=f'create_file_with_leads_for_yandex_dag_1',
    default_args=default_args,
    catchup=False,
    start_date=YESTERDAY,
    schedule_interval='0 8 * * *',
    tags = ['create_file','stotage'],
) as dag:

    def get_yandex_direct_data():
        query = """
        SELECT
            DATE_CREATE,
            ID, UTM_SOURCE,  UTM_MEDIUM, UTM_CAMPAIGN, UTM_CONTENT, UTM_TERM,
            CASE 
                WHEN STAGE_GROUPPED IN ('Qualified','Warm, Hot','Signing, In closure, Closed')
                THEN 1
                ELSE 0
            END as goal_1,
            CASE
                WHEN STAGE_GROUPPED NOT IN ('Qualified','Warm, Hot','Signing, In closure, Closed')
                THEN 1
                ELSE 0
            END as goal_2
        FROM
            (
            SELECT DATE(DATETIME_ADD(DATE_CREATE, INTERVAL 4 HOUR)) as DATE_CREATE,
            ID, UTM_SOURCE,  UTM_MEDIUM, UTM_CAMPAIGN, UTM_CONTENT, UTM_TERM, STAGE_ID,t1.CATEGORY_ID, UF_CRM_1666762747,STAGE_GROUPPED
            FROM southern-idea-345503.raw_data_bitrix.deal t1
            LEFT JOIN `southern-idea-345503.technical_tables.stage_pipeline` t2
            ON t1.STAGE_ID=t2.STATUS_ID    
            WHERE
                DATE(DATETIME_ADD(DATE_CREATE, INTERVAL 4 HOUR)) >= '2022-01-01' AND
                UTM_SOURCE IN ('yandex.context','yandex.search','yandex.banners','yandex.video','yandex.main','yandex.direct','yandex-direct','yandex-rsya')
                AND t1.CATEGORY_ID IN (0,2,4,5,7,9,11) AND STAGE_ID NOT IN ('13','C6:1','C6:PREPARATION','C6:PREPAYMENT_INVOICE','C6:NEW','C6:WON','C6:LOSE','C7:LOSE')
                AND UF_CRM_1666762747 IN ('False', '[]')
            )
        """
        credentials, project = google.auth.default(
            scopes=[
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/cloud-platform",
            ]
        )
        client = bigquery.Client(credentials=credentials, project=project)

        results = client.query(query)  # API request
        data = results.to_dataframe()
        data.rename(columns={'DATE_CREATE':'date', 'ID':'orderId', 'UTM_SOURCE':'utm_source', 'UTM_MEDIUM':'utm_medium',
                            'UTM_CAMPAIGN':'utm_campaign', 'UTM_CONTENT':'utm_content', 'UTM_TERM':'utm_term'}, inplace=True)
        data['date'] = pandas.to_datetime(data['date']).dt.date
        data = data.drop_duplicates()

        bucket = storage_client.bucket(BUCKET)
        blob = bucket.blob(blob_file)
        tmp = data.to_csv(index=False)
        blob.upload_from_string(tmp)
        print(f'File updated with new data')

        blob.make_public()

        return True

    # Step 3. Parameters of get data function . Edit it
    get_data_for_k50 = PythonOperator(
        task_id='get_data_for_k50',
        python_callable=get_yandex_direct_data,
    )

    get_data_for_k50