from datetime import date as dt_date, datetime, timedelta
import numpy as np
import pandas
import pandas_gbq

from airflow import models
from airflow.exceptions import AirflowSkipException
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery

#from tapi_yandex_metrika import YandexMetrikaStats

from common.common_function import task_fail_telegram_alert


################################## Parameters. Don't edit it! ##################################
SOURCE_NAME = 'search_queries'
DAG_NUMBER = '1'
START_DATE = datetime.now() - timedelta(days=10)
END_DATE = datetime.now() - timedelta(days=0)
YESTERDAY = datetime.now() - timedelta(days=1)
LOCATION = 'europe-west3'
PROJECT = 'southern-idea-345503'
DATASET = 'raw_data'
TABLE_NAME_YANDEX = f'{PROJECT}.{DATASET}.{SOURCE_NAME}_yandex_api_dag_{DAG_NUMBER}'
TABLE_NAME_GOOGLE = f'{PROJECT}.{DATASET}.{SOURCE_NAME}_google_api_dag_{DAG_NUMBER}'
token = models.Variable.get('yandex_metrika_api_token')
API_SERVICE_NAME = 'searchconsole'
API_VERSION = 'v1'
################################## Parametrs. Don't edit it! ##################################

bq_client = bigquery.Client()
#client = YandexMetrikaStats(access_token=token)


# def get_report(params):
#     df = pandas.DataFrame()
#     limit = 100000
#     offset = limit
#     params['limit'] = limit
#     report = client.stats().get(params=params)
#     sample_size, total_rows = report['sample_size'], report['total_rows']
#     if total_rows > 0:
#         if total_rows < 100000:
#             t = total_rows
#         else:
#             t = sample_size
#         print('offset =', offset, ' sample_size =', sample_size, 'total_rows =', total_rows, 't =', t)
#         temp_df = pandas.DataFrame.from_dict(report().to_dicts())
#         df = pandas.concat([df, temp_df], ignore_index=True)
#         while offset < t:
#             params['offset'] = offset
#             report = client.stats().get(params=params)
#             print('offset =', offset, ' sample_size =', sample_size, 'total_rows =', total_rows, 't =', t)
#             temp_df = pandas.DataFrame.from_dict(report().to_dicts())
#             df = pandas.concat([df, temp_df], ignore_index=True)
#             offset = offset + limit
#         return df
#     else:
#         t = total_rows
#         print('offset =', offset, ' sample_size =', sample_size, 'total_rows =', total_rows, 't =', t)
#         return df


default_args = {
    'owner': 'Composer Example',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_telegram_alert
}

with models.DAG(
        dag_id=f'{DATASET}_{SOURCE_NAME}_dag_{DAG_NUMBER}',
        default_args=default_args,
        catchup=False,
        start_date=YESTERDAY,
        schedule_interval='10 4 * * *',
        tags = ['load','google_search_console','yandex_metrica','api'],
) as dag:
    
    # def yandex_search_queries():
    #     yandex_metrica_counters = ['61700275']

    #     fields1 = 'ym:s:date,ym:s:counterID,ym:s:visitID,ym:s:lastSearchPhrase,ym:s:goal'
    #     fields2 = 'ym:s:date,ym:s:counterID,ym:s:visitID,ym:s:startURL'
    #     fields3 = 'ym:s:date,ym:s:counterID,ym:s:visitID,ym:s:clientID'
    #     columns = ['date', 'counterId', 'visitId', 'searchPhrase', 'goal', 'startURL', 'clientId', 'visits']
    #     df1 = pandas.DataFrame()
    #     df2 = pandas.DataFrame()
    #     df3 = pandas.DataFrame()
    #     df_goals = pandas.DataFrame()

    #     for counter in yandex_metrica_counters:
    #         print(
    #             f'Start report query for {counter} ({yandex_metrica_counters.index(counter)} of {len(yandex_metrica_counters) - 1})')
    #         params = dict(
    #             ids=counter,
    #             dimensions=fields1,
    #             metrics="ym:s:visits",
    #             filters="ym:s:lastSearchPhrase!=''",
    #             date1=START_DATE.strftime('%Y-%m-%d'),
    #             date2=END_DATE.strftime('%Y-%m-%d'),
    #             accuracy="full",
    #             proposed_accuracy='false',
    #         )
    #         df1 = get_report(params)

    #         if not df1.empty:
    #             params['dimensions'] = fields2
    #             params['filters'] = ""
    #             df2 = get_report(params)
    #             df_merged = pandas.merge(df1, df2, how='left', on=['ym:s:date', 'ym:s:counterID', 'ym:s:visitID'])
    #             df_merged.drop(['ym:s:visits_x', 'ym:s:visits_y'], inplace=True, axis=1)

    #             params['dimensions'] = fields3
    #             df3 = get_report(params)
    #             df3.to_csv('df3.csv', index=False)
    #             df_merged = pandas.merge(df_merged, df3, how='left', on=['ym:s:date', 'ym:s:counterID', 'ym:s:visitID'])
    #             df_goals = pandas.concat([df_goals, df_merged], ignore_index=True)
    #     df_goals.columns = columns
    #     df_goals = df_goals[['date', 'counterId', 'clientId', 'visitId', 'searchPhrase', 'startURL', 'goal', 'visits']]

    #     query_deal = f"""SELECT ID, UF_CRM_624C48458A3AF FROM `raw_data_bitrix.deal`"""
    #     deal_df = pandas.read_gbq(query_deal, PROJECT)
    #     deal_df.columns = ['ID', 'clientId']
    #     deal_df.loc[deal_df['clientId'] == 'null', 'clientId'] = np.nan
    #     df_goals = pandas.merge(df_goals, deal_df, how='left', on='clientId')
    #     df_goals = df_goals.astype({'visits': 'int64', 'ID': str})
    #     df_goals['ID'] = df_goals['ID'].str.replace("<NA>", "")
    #     table = bq_client.get_table(TABLE_NAME_YANDEX)
    #     generated_schema = [{'name': i.name, 'type': i.field_type} for i in table.schema]

    #     pandas_gbq.to_gbq(df_goals, TABLE_NAME_YANDEX, PROJECT, table_schema=generated_schema, if_exists='append')
    #     return True


    def google_search_queries():
        from airflow.hooks.base_hook import BaseHook
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
        import json
        import logging
        
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

        analytics_hook = BaseHook.get_connection('analytics-team-sa')
        analytics_auth = analytics_hook.get_extra()
        scopes = ['https://www.googleapis.com/auth/webmasters.readonly', 'https://www.googleapis.com/auth/webmasters']

        credentials = Credentials.from_service_account_info(json.loads(analytics_auth),scopes=scopes)
        search_console = build(API_SERVICE_NAME, API_VERSION, credentials=credentials, cache_discovery=False)
        sites = search_console.sites().list().execute()
        verified_sites_url = [s['siteUrl'] for s in sites['siteEntry']
                              if s['permissionLevel'] != 'siteUnverifiedUser' and s['siteUrl'].startswith('http')]
        logging.info(verified_sites_url)
        dataset = bq_client.query('select distinct date from {}'.format(TABLE_NAME_GOOGLE))
        exist_dates = dataset.to_dataframe().date.tolist()
        
        table = bq_client.get_table(TABLE_NAME_GOOGLE)
        generated_schema = [{'name': i.name, 'type': i.field_type} for i in table.schema]

        END_DATE = datetime.now() - timedelta(days=1)
        d_start = datetime(2023, 10, 1).date()
        d_finish = d_start

        days_list = []
        while d_start <= END_DATE.date():
            if d_start not in exist_dates:
                days_list.append(d_start)
            d_start = d_start + timedelta(days=1)
        
        if not len(days_list):
            raise AirflowSkipException('There is no days without info')
        
        days_chunks = []
        for i in range(0, len(days_list), 10):
            days_chunks.append(days_list[i:i+10])
            
        for chunk in days_chunks:
            final_df = pandas.DataFrame()
            max_rows = 10
            for day_item in chunk:
                logging.info(day_item)
                request = {
                    "startDate": day_item.strftime("%Y-%m-%d"),
                    "endDate": day_item.strftime("%Y-%m-%d"),
                    "dimensions": ["query", "page"],
                    "rowLimit": max_rows
                }
                
                for site in verified_sites_url:
                    logging.info(site)
                    resp_query = search_console.searchanalytics().query(siteUrl=site, body=request).execute()
                    if resp_query.get('rows', None):
                        df = pandas.DataFrame(resp_query['rows'])
                        df = pandas.concat([df, df['keys'].apply(lambda x: pandas.Series(x))], axis=1)
                        df['date'] = day_item
                        df = df[['date', 0, 1, 'clicks', 'impressions', 'ctr', 'position']]
                        df.columns = ['date', 'key', 'page', 'clicks', 'impressions', 'ctr', 'position']
                        final_df = pandas.concat([final_df, df], axis=0)
                    else:
                        continue
            logging.info('{}: {} rows fetched.'.format(chunk, final_df.shape[0]))
            if final_df.shape[0]:
                final_df['date'] = pandas.to_datetime(final_df['date'])
                pandas_gbq.to_gbq(final_df, TABLE_NAME_GOOGLE, PROJECT, table_schema=generated_schema, if_exists='append')
        logging.info('Done.')
        return True
    
    # delete_ym_old_records = BigQueryInsertJobOperator(
    #     task_id='delete_ym_old_records',
    #     gcp_conn_id='google_cloud_default',
    #     configuration={
    #         "query": {
    #             "query":
    #                 f"""
    #                     DELETE 
    #                     FROM 
    #                         `{TABLE_NAME_YANDEX}`
    #                     WHERE 
    #                         date >= '{START_DATE.strftime('%Y-%m-%d')}'
    #                 """,
    #             "useLegacySql": False
    #         }
    #     }
    # )

    # get_yandex_data = PythonOperator(
    #     task_id='get_yandex_data',
    #     execution_timeout=timedelta(hours=5),
    #     python_callable=yandex_search_queries,
    # )

    get_google_data = PythonOperator(
        task_id='get_google_data',
        execution_timeout=timedelta(hours=5),
        python_callable=google_search_queries,
    )

    # delete_ym_old_records >> get_yandex_data
    get_google_data