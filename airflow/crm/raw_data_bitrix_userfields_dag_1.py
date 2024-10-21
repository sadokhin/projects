import datetime, requests
import pandas, pandas_gbq
from google.cloud import bigquery

from airflow import models
from airflow.operators.python_operator import PythonOperator
from common.common_function import task_fail_telegram_alert

# Step 1. Parameters. Edit it
DAG_NUMBER = '1'
LOCATION = 'europe-west3'
PROJECT = 'southern-idea-345503'
DATASET = 'technical_tables'
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
TABLE_NAME_CONSULTING_DEAL_TYPE = 'southern-idea-345503.technical_tables.consulting_deal_type'
TABLE_NAME_PROPERTY_TYPE = 'southern-idea-345503.technical_tables.bitrix_property_type'
TABLE_NAME_BITRIX_NUMBER_OF_BEDROOMS = 'southern-idea-345503.technical_tables.bitrix_number_of_bedrooms'
TABLE_NAME_BITRIX_PROJECT_TYPE = 'southern-idea-345503.technical_tables.bitrix_project_type'
TABLE_NAME_BITRIX_ROLE_TYPE = 'southern-idea-345503.technical_tables.UF_CRM_6_1695379069'
TABLE_NAME_BITRIX_BT_HAS_A_TO_A = 'southern-idea-345503.technical_tables.UF_CRM_1689232909'
TABLE_NAME_BITRIX_BUYER_PAYMENT_TYPE = 'southern-idea-345503.technical_tables.UF_CRM_1670932932'
TABLE_NAME_BITRIX_BT_HAS_EXTERNAL_REFERRAL = 'southern-idea-345503.technical_tables.UF_CRM_1563363025076'
TABLE_NAME_BITRIX_BT_HAS_INTERNAL_REFERRAL = 'southern-idea-345503.technical_tables.UF_CRM_1590606111'
TABLE_NAME_BITRIX_SO_HAS_A_TO_A = 'southern-idea-345503.technical_tables.UF_CRM_1689234692'
TABLE_NAME_BITRIX_SO_HAS_EXTERNAL_REFERRAL = 'southern-idea-345503.technical_tables.UF_CRM_1689753152'
TABLE_NAME_BITRIX_SELLER_PAYMENT_TYPE = 'southern-idea-345503.technical_tables.UF_CRM_1670932823'

bq_client = bigquery.Client(project=PROJECT)
BX_WH_URL = models.Variable.get('BX_WH_URL_Lists_1_secret')

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
    dag_id=f'raw_data_bitrix_userfields_dag_1',
    default_args=default_args,
    catchup=False,
    start_date=YESTERDAY,
    schedule_interval='35 2 * * 6',
    tags = ['load','bitrix','api'],
) as dag:

    def get_userfields_values():
        response = requests.get(BX_WH_URL + '/crm.deal.fields/')
        data = response.json()
        fields = data.get('result')
        df = pandas.DataFrame.from_records(fields)
        
        df_consulting_deal_type = pandas.DataFrame(df.UF_CRM_1639289229['items'])
        df_consulting_deal_type.rename(columns={"VALUE": "NAME"}, inplace = True)
        pandas_gbq.to_gbq(df_consulting_deal_type,TABLE_NAME_CONSULTING_DEAL_TYPE, if_exists='replace')

        df_propety_type = pandas.DataFrame(df.UF_CRM_1560856359['items'])
        df_propety_type.rename(columns={"VALUE": "NAME"}, inplace = True)
        pandas_gbq.to_gbq(df_propety_type,TABLE_NAME_PROPERTY_TYPE, if_exists='replace')

        df_propety_type = pandas.DataFrame(df.UF_CRM_1666261607435['items'])
        df_propety_type.rename(columns={"VALUE": "NAME"}, inplace = True)
        pandas_gbq.to_gbq(df_propety_type,TABLE_NAME_BITRIX_PROJECT_TYPE, if_exists='replace')

        df_propety_type = pandas.DataFrame(df.UF_CRM_1604323289['items'])
        df_propety_type.rename(columns={"VALUE": "NAME"}, inplace=True)
        pandas_gbq.to_gbq(df_propety_type, TABLE_NAME_BITRIX_NUMBER_OF_BEDROOMS, if_exists='replace')

        df_role_type = pandas.DataFrame(df.UF_CRM_6_1695379069['items'])
        df_role_type.rename(columns={"VALUE": "NAME"}, inplace=True)
        pandas_gbq.to_gbq(df_role_type, TABLE_NAME_BITRIX_ROLE_TYPE, if_exists='replace')

        df_BT_has_A_to_A = pandas.DataFrame(df.UF_CRM_1689232909['items'])
        df_BT_has_A_to_A.rename(columns={"VALUE": "NAME"}, inplace=True)
        pandas_gbq.to_gbq(df_BT_has_A_to_A, TABLE_NAME_BITRIX_BT_HAS_A_TO_A, if_exists='replace')

        df_buyer_payment_type = pandas.DataFrame(df.UF_CRM_1670932932['items'])
        df_buyer_payment_type.rename(columns={"VALUE": "NAME"}, inplace=True)
        pandas_gbq.to_gbq(df_buyer_payment_type, TABLE_NAME_BITRIX_BUYER_PAYMENT_TYPE, if_exists='replace')

        df_BT_has_external_referral = pandas.DataFrame(df.UF_CRM_1563363025076['items'])
        df_BT_has_external_referral.rename(columns={"VALUE": "NAME"}, inplace=True)
        pandas_gbq.to_gbq(df_BT_has_external_referral, TABLE_NAME_BITRIX_BT_HAS_EXTERNAL_REFERRAL, if_exists='replace')

        df_BT_has_internal_referral = pandas.DataFrame(df.UF_CRM_1590606111['items'])
        df_BT_has_internal_referral.rename(columns={"VALUE": "NAME"}, inplace=True)
        pandas_gbq.to_gbq(df_BT_has_internal_referral, TABLE_NAME_BITRIX_BT_HAS_INTERNAL_REFERRAL, if_exists='replace')

        df_SO_has_A_to_A = pandas.DataFrame(df.UF_CRM_1689234692['items'])
        df_SO_has_A_to_A.rename(columns={"VALUE": "NAME"}, inplace=True)
        pandas_gbq.to_gbq(df_SO_has_A_to_A, TABLE_NAME_BITRIX_SO_HAS_A_TO_A, if_exists='replace')

        df_SO_has_external_referral = pandas.DataFrame(df.UF_CRM_1689753152['items'])
        df_SO_has_external_referral.rename(columns={"VALUE": "NAME"}, inplace=True)
        pandas_gbq.to_gbq(df_SO_has_external_referral, TABLE_NAME_BITRIX_SO_HAS_EXTERNAL_REFERRAL, if_exists='replace')

        df_seller_payment_type = pandas.DataFrame(df.UF_CRM_1670932823['items'])
        df_seller_payment_type.rename(columns={"VALUE": "NAME"}, inplace=True)
        pandas_gbq.to_gbq(df_seller_payment_type, TABLE_NAME_BITRIX_SELLER_PAYMENT_TYPE, if_exists='replace')

        return True

    load_bitix_userfields_values = PythonOperator(
        task_id='load_bitix_userfields_values',
        python_callable=get_userfields_values,
    )

    load_bitix_userfields_values 