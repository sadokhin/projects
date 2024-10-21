import pandas, pandas_gbq, json
from google.cloud import bigquery
from airflow import models
from airflow.operators.python_operator import PythonOperator
from mailchimp3 import MailChimp
import datetime
from common.common_function import task_fail_telegram_alert

################################## Parameters. Don't edit it! ##################################
LOCATION = 'europe-west3'
PROJECT = 'southern-idea-345503'
DATASET = 'raw_data'
TABLE = 'mailchimp_api_dag_1'
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
TABLE_NAME_MAILCHIMP = f'{PROJECT}.{DATASET}.{TABLE}'

bq_client = bigquery.Client()

default_args = {
    'owner': 'Composer Example',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=2),
    'on_failure_callback': task_fail_telegram_alert
}

with models.DAG(
        dag_id=f'{DATASET}_{TABLE}',
        default_args=default_args,
        catchup=False,
        start_date=YESTERDAY,
        schedule_interval='0 4 * * *',
        tags = ['load','mailchimp','api'],
) as dag:


    def get_data_mailchimp():

        client = MailChimp(mc_api=models.Variable.get("mailchimp_key"), mc_user='YOUR_USERNAME')
        list_campaigns = client.reports.all(get_all=True)

        tmp_dict = {}
        tmp_dict['campaign_id'] = []
        tmp_dict['title'] = []
        tmp_dict['subject_line'] = []
        tmp_dict['send_time'] = []
        tmp_dict['abuse_reports'] = []
        tmp_dict['unsubscribed'] = []
        tmp_dict['email_sent'] = []
        tmp_dict['opens'] = []
        tmp_dict['unique_opens'] = []
        tmp_dict['clicks'] = []
        tmp_dict['unique_clicks'] = []
        tmp_dict['hard_bounces'] = []
        tmp_dict['soft_bounces'] = []
        for i in list_campaigns['reports']:
            tmp_dict['campaign_id'].append(i['id'])
            tmp_dict['title'].append(i['campaign_title'])
            tmp_dict['subject_line'].append(i['subject_line'])
            tmp_dict['send_time'].append(i['send_time'])
            tmp_dict['abuse_reports'].append(i['abuse_reports'])
            tmp_dict['unsubscribed'].append(i['unsubscribed'])
            tmp_dict['email_sent'].append(i['emails_sent'])
            tmp_dict['opens'].append(i['opens']['opens_total'])
            tmp_dict['unique_opens'].append(i['opens']['unique_opens'])
            tmp_dict['clicks'].append(i['clicks']['clicks_total'])
            tmp_dict['unique_clicks'].append(i['clicks']['unique_clicks'])
            tmp_dict['hard_bounces'].append(i['bounces']['hard_bounces'])
            tmp_dict['soft_bounces'].append(i['bounces']['soft_bounces'])

        df = pandas.DataFrame.from_dict(tmp_dict)
        df['bounces'] = df['soft_bounces'] + df['hard_bounces']
        df['delivered'] = df['email_sent'] - df['bounces']
        df = df.astype(str)
        table = bq_client.get_table(TABLE_NAME_MAILCHIMP)
        generated_schema = [{'name': i.name, 'type': i.field_type} for i in table.schema]
        pandas_gbq.to_gbq(df, TABLE_NAME_MAILCHIMP, PROJECT, table_schema=generated_schema,  if_exists='replace')

    get_mailchimp_data = PythonOperator(
        task_id='get_mailchimp_data',
        execution_timeout=datetime.timedelta(hours=1),
        python_callable=get_data_mailchimp,
    )

    get_mailchimp_data