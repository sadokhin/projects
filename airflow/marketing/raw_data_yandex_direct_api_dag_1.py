import os, io, json, datetime, requests
import pandas, pandas_gbq
from time import sleep
from google.cloud import bigquery
import google.auth

from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from common.common_function import task_fail_telegram_alert


################################## Parameters. Don't edit it! ##################################
SOURCE_NAME = 'yandex_direct_api'
DAG_NUMBER = '1'
LOCATION = 'europe-west3'
PROJECT = 'southern-idea-345503'
DATASET = 'raw_data'
START_DATE = datetime.datetime.now() - datetime.timedelta(days=1)
TODAY = datetime.datetime.now()
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
yesterday_day = YESTERDAY.date()
END_DATE = datetime.datetime.now() - datetime.timedelta(days=0)
#TABLE_AUDIENCE = 'yandex_direct_audience'
TABLE_CAMPAIGN = 'yandex_direct_campaign'
TABLE_PLACEMENT = 'yandex_direct_placement'
TABLE_URL = 'yandex_ads_urls'
#TABLE_NAME_AUDIENCE = f'{PROJECT}.{DATASET}.{TABLE_AUDIENCE}_api_dag_{DAG_NUMBER}'
TABLE_NAME_CAMPAIGN = f'{PROJECT}.{DATASET}.{TABLE_CAMPAIGN}_api_dag_{DAG_NUMBER}'
TABLE_NAME_PLACEMENT = f'{PROJECT}.{DATASET}.{TABLE_PLACEMENT}_api_dag_{DAG_NUMBER}'
TABLE_NAME_URL = f'{PROJECT}.{DATASET}.{TABLE_URL}'
token = models.Variable.get('yandex_direct_api_v5_token')

################################## Parametrs. Don't edit it! ##################################

def u(x):
    if type(x) == type(b''):
        return x.decode('utf8')
    else:
        return x

# Запрос аккаунтов по всем клиентам
ClientsURL = 'https://api.direct.yandex.com/json/v5/agencyclients'
headers = { "Authorization": "Bearer " + token, "Accept-Language": "ru" }
body = { "method": "get", "params": { "SelectionCriteria": {}, "FieldNames": ["Login","ClientId"] } }
request = json.dumps(body, ensure_ascii=False).encode('utf8')
clients = []
result = requests.post(ClientsURL, request, headers=headers)

bq_client = bigquery.Client()
query = f'''
    SELECT DISTINCT cast(AdGroupId as INT) as AdGroupId FROM `southern-idea-345503.raw_data.yandex_direct_campaign_api_dag_1`
    where date = '{yesterday_day}' and AdGroupId != '--' and adId not in (select Id from `southern-idea-345503.raw_data.yandex_ads_urls`)
'''
query_job = bq_client.query(query)
df = query_job.to_dataframe()

adgroup_id_list = df['AdGroupId'].tolist()
# Обработка запроса
if result.status_code != 200 or result.json().get("error", False):
    print("Произошла ошибка при обращении к серверу API Директа.")
    print("Код ошибки: {}".format(result.json()["error"]["error_code"]))
    print("Описание ошибки: {}".format(u(result.json()["error"]["error_detail"])))
else:
    # Вывод списка кампаний
    for client in result.json()['result']['Clients']:
        clients.append(client['Login'])

credentials, project = google.auth.default(
    scopes=[
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/cloud-platform",
    ]
)
bq_client = bigquery.Client(credentials=credentials, project=project)


headers = {
    "Authorization": 'Bearer ' + token,
    "Accept-Language": 'ru',
    'skipReportHeader': 'true',
    'skipColumnHeader': 'false',
    'skipReportSummary': 'true',
    'returnMoneyInMicros': 'false'
}

def report_generator(client, url, body):
    resultcsv = ''
    
    requestBody = json.dumps(body, indent=4)
                
    while True:
        try:
            req = requests.post(url, requestBody, headers=headers)
            req.encoding = 'utf-8'
            if req.status_code == 400:
                print("Параметры запроса указаны неверно или достугнут лимит отчетов в очереди")
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код запроса: {}".format(u(body)))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                break
            elif req.status_code == 200:
                print("Отчет для аккаунта {} создан успешно".format(str(client)))
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                resultcsv += format(u(req.text)) + '\n'
                break
            elif req.status_code == 201:
                print("Отчет для аккаунта {} успешно поставлен в очередь в режиме offline".format(str(client)))
                retryIn = int(req.headers.get("retryIn", 60))
                print("Повторная отправка запроса через {} секунд".format(retryIn))
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                sleep(retryIn)
            elif req.status_code == 202:
                print("Отчет формируется в режиме офлайн".format(str(client)))
                retryIn = int(req.headers.get("retryIn", 60))
                print("Повторная отправка запроса через {} секунд".format(retryIn))
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                sleep(retryIn)
            elif req.status_code == 500:
                print("При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее.")
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                break
            elif req.status_code == 502:
                print("Время формирования отчета превысило серверное ограничение.")
                print("Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.")
                print("JSON-код запроса: {}".format(body))
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                break
            else:
                print("Произошла непредвиденная ошибка")
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код запроса: {}".format(body))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                break

        # Обработка ошибки, если не удалось соединиться с сервером API Директа
        except requests.exceptions.ConnectionError:
            # В данном случае мы рекомендуем повторить запрос позднее
            print("Произошла ошибка соединения с сервером API")
            break
        # Если возникла какая-либо другая ошибка
        except:
            # В данном случае мы рекомендуем проанилизировать действия приложения
            print("Произошла непредвиденная ошибка")
            break
    return resultcsv



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
    dag_id=f'{DATASET}_{SOURCE_NAME}_dag_{DAG_NUMBER}',
    default_args=default_args,
    catchup=False,
    start_date=YESTERDAY,
    schedule_interval='30 1 * * *',
    tags = ['load','yandex_direct','api'],
    
) as dag:
    
    # delete_audience_records = BigQueryInsertJobOperator(
    #         task_id='delete_audience_records',
    #         gcp_conn_id='google_cloud_default',
    #         configuration={
    #             "query": {
    #                 "query":
    #                 f"""
    #                     DELETE 
    #                     FROM 
    #                         `{TABLE_NAME_AUDIENCE}`
    #                     WHERE 
    #                         date >= '{START_DATE.strftime('%Y-%m-%d')}'
    #                 """,
    #             "useLegacySql": False
    #         }
    #     }
    # )
    
    delete_placement_records = BigQueryInsertJobOperator(
            task_id='delete_placement_records',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query":
                    f"""
                        DELETE 
                        FROM 
                            `{TABLE_NAME_PLACEMENT}`
                        WHERE 
                            date >= '{START_DATE.strftime('%Y-%m-%d')}'
                    """,
                "useLegacySql": False
            }
        }
    )

    delete_campaign_records = BigQueryInsertJobOperator(
            task_id='delete_campaign_records',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query":
                    f"""
                        DELETE 
                        FROM 
                            `{TABLE_NAME_CAMPAIGN}`
                        WHERE 
                            date >= '{START_DATE.strftime('%Y-%m-%d')}'
                    """,
                "useLegacySql": False
            }
        }
    )

    
    #### Get YD audience data
    # def get_data_audience():
    #     AudienceURL = 'https://api.direct.yandex.com/json/v5/reports'
    #     df_aud = pandas.DataFrame()
    #     data = pandas.DataFrame()
    #     for client in result.json()['result']['Clients']:
    #         print(f'Генерация отчёта Audience для {client["Login"]}: {clients.index(client["Login"])} из {len(clients) - 1}')
    #         headers['Client-Login'] = client["Login"]

    #         body = {
    #             "params": {
    #                 "SelectionCriteria": {
    #                 "DateFrom": START_DATE.strftime('%Y-%m-%d'),
    #                 "DateTo": END_DATE.strftime('%Y-%m-%d'),
    #                 "Filter": [
    #                     {
    #                         "Field": "Impressions",
    #                         "Operator": "GREATER_THAN",
    #                         "Values": [0]
    #                     },
    #                     {
    #                         "Field": "CriterionType",
    #                         "Operator": "EQUALS",
    #                         "Values": ["RETARGETING"]
    #                     }
    #                 ]
    #                 },
    #                 "FieldNames": [
    #                         "Date",
    #                         "ClientLogin",
    #                         "CampaignName",
    #                         "CampaignId",
    #                         "Criterion",
    #                         "CriterionId",
    #                         "Clicks",
    #                         "Impressions",
    #                         "Cost",
    #                         "Conversions"
    #                 ],
    #                 "ReportName": f"YANDEX_DIRECT_AUDIENCE_{TODAY.strftime('%Y-%m-%d')}",
    #                 "ReportType": "CUSTOM_REPORT",
    #                 "DateRangeType": "CUSTOM_DATE",
    #                 "Format": "TSV",
    #                 "IncludeVAT": "NO",
    #                 "IncludeDiscount": "NO"
    #             }
    #         }

    #         resultcsv = report_generator(client['Login'], AudienceURL, body)

    #         # Формирование DataFrame
    #         if resultcsv:
    #             df_aud = pandas.read_csv(io.StringIO(resultcsv), sep="\t", index_col=False, dtype = {
    #                 'Date': 'string',
    #                 'ClientLogin': 'string',
    #                 'CampaignName': 'string',
    #                 'CampaignId': 'string',
    #                 'Criterion': 'string',
    #                 'CriterionId': 'string',
    #                 'Impressions': 'string',
    #                 'Clicks': 'string',
    #                 'Cost': 'string',
    #                 'Conversions': 'string'
    #                 }   
    #             )
    #             df_aud.loc[df_aud.Impressions == '--', 'Impressions'] = '0.0'
    #             df_aud.loc[df_aud.Clicks == '--', 'Clicks'] = '0.0'
    #             df_aud.loc[df_aud.Cost == '--', 'Cost'] = '0.0'
    #             df_aud.loc[df_aud.Conversions == '--', 'Conversions'] = '0.0'
    #             df_aud.astype({'Impressions': 'float', 'Clicks': 'float', 'Cost': 'float', 'Conversions': 'float'})
    #             data = pandas.concat([data, df_aud])
    #             try:
    #                 data.loc[data['ClientLogin'] == client['Login'], 'accountId'] = str(client['ClientId'])
    #             except ValueError as e:
    #                 print(f"Error: {e}")
    #     data.rename(columns=
    #         {
    #             'Date': 'date',
    #             'CriterionId': 'audienceId',
    #             'Criterion': 'audienceName',
    #             'Impressions': 'impressions',
    #             'ClientLogin': 'accountName',
    #             'CampaignName': 'campaignName',
    #             'CampaignId': 'campaignId',
    #             'Clicks': 'clicks',
    #             'Cost': 'costs',
    #             'Conversions': 'conversions',
    #         }, 
    #         inplace=True
    #     )
    #     if data.empty:
    #         print('DataFrame is empty!')
    #         return True
    #     else:
    #         data = data.astype({'impressions': 'float', 'clicks': 'float', 'costs': 'float', 'conversions': 'float'})
    #         data['date'] = pandas.to_datetime(data['date']).dt.date
    #         #data.to_csv(CSV_NAME_AUDIENCE, index = False)
            
    #         table = bq_client.get_table(TABLE_NAME_AUDIENCE)
    #         generated_schema = [{'name':i.name, 'type':i.field_type} for i in table.schema]
    #         pandas_gbq.to_gbq(data, TABLE_NAME_AUDIENCE, PROJECT, table_schema = generated_schema, if_exists='append')

    #         return True


    # Функция запроса данных по местам показа
    def get_data_placement():

        PlacementsURL = 'https://api.direct.yandex.com/json/v5/reports'
        data = pandas.DataFrame()
        df_plac = pandas.DataFrame()
        for client in result.json()['result']['Clients']:
            print(f'Генерация отчёта Placement для {client["Login"]}: {clients.index(client["Login"])} из {len(clients) - 1}')
            headers['Client-Login'] = client["Login"]
            
            body = {
                "params": {
                    "SelectionCriteria": {
                    "DateFrom": START_DATE.strftime('%Y-%m-%d'),
                    "DateTo": END_DATE.strftime('%Y-%m-%d'),
                    "Filter": [
                            {
                                "Field": "Impressions",
                                "Operator": "GREATER_THAN",
                                "Values": [0]
                            }
                        ]
                    },
                    "FieldNames": [
                            "Date",
                            "ClientLogin",
                            "CampaignId",
                            "CampaignName",
                            "Placement",
                            "Clicks",
                            "Impressions",
                            "Cost",
                            "Conversions",
                    ],
                    "ReportName": f"YANDEX_DIRECT_PLACEMENT_{TODAY.strftime('%Y-%m-%d')}",
                    "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
                    "DateRangeType": "CUSTOM_DATE",
                    "Format": "TSV",
                    "IncludeVAT": "NO",
                    "IncludeDiscount": "NO"
                }
            }

            resultcsv = report_generator(client['Login'], PlacementsURL, body)

            # Формирование DataFrame
            if resultcsv:
                df_plac = pandas.read_csv(io.StringIO(resultcsv), sep="\t", index_col=False, dtype = {
                    'Date': 'string',
                    'ClientLogin': 'string',
                    'CampaignName': 'string',
                    'CampaignId': 'string',
                    'Placement': 'string',
                    'Impressions': 'string',
                    'Clicks': 'string',
                    'Cost': 'string',
                    'Conversions': 'string'
                    }
                )
                df_plac.loc[df_plac.Impressions == '--', 'Impressions'] = '0.0'
                df_plac.loc[df_plac.Clicks == '--', 'Clicks'] = '0.0'
                df_plac.loc[df_plac.Cost == '--', 'Cost'] = '0.0'
                df_plac.loc[df_plac.Conversions == '--', 'Conversions'] = '0.0'
                df_plac.astype({'Impressions': 'float', 'Clicks': 'float', 'Cost': 'float', 'Conversions': 'float'})
                data = pandas.concat([data, df_plac])
                try:
                    data.loc[data['ClientLogin'] == client['Login'], 'accountId'] = str(client['ClientId'])
                except ValueError as e:
                    print(f"Error: {e}")
        data.rename(columns={
            'Date': 'date',
            'ClientLogin': 'accountName',
            'CampaignName': 'campaignName',
            'CampaignId': 'campaignId',
            'Placement': 'placement',
            'Impressions': 'impressions',
            'Clicks': 'clicks',
            'Cost': 'costs',
            'Conversions': 'conversions'
            }, 
            inplace=True
        )
        data = data.astype({'impressions': 'float', 'clicks': 'float', 'costs': 'float', 'conversions': 'float'})
        data['date'] = pandas.to_datetime(data['date']).dt.date
        #data.to_csv(CSV_NAME_PLACEMENT, index = False)
        
        table = bq_client.get_table(TABLE_NAME_PLACEMENT)
        generated_schema = [{'name':i.name, 'type':i.field_type} for i in table.schema]
        pandas_gbq.to_gbq(data, TABLE_NAME_PLACEMENT, PROJECT, table_schema = generated_schema, if_exists='append')

        return True


    # Функция запроса данных по кампаниям показа
    def get_data_campaign():
        CampaignsURL = 'https://api.direct.yandex.com/json/v5/reports'
        data = pandas.DataFrame()
        df_camp = pandas.DataFrame()
        

        # def daterange(start_date, end_date):
        #     for n in range(int((end_date - start_date).days)+1):
        #         yield start_date + datetime.timedelta(n)

        # for single_date in daterange(START_DATE, END_DATE):
        for client in result.json()['result']['Clients']:
            print(f'Генерация отчёта Campaign для {client["Login"]}: {clients.index(client["Login"])} из {len(clients) - 1}')
            headers['Client-Login'] = client["Login"]
            
            body = {
                "params": {
                    "SelectionCriteria": {
                    "DateFrom": START_DATE.strftime('%Y-%m-%d'),#single_date.strftime('%Y-%m-%d'),
                    "DateTo": END_DATE.strftime('%Y-%m-%d'),#single_date.strftime('%Y-%m-%d'),
                    },
                    "FieldNames": [
                        "Date",
                        "ClientLogin",
                        "CampaignName",
                        "CampaignId",
                        "AdGroupName",
                        "AdGroupId",
                        "AdId",
                        "Criterion",
                        "Device",
                        "AdNetworkType",
                        "TargetingLocationName",
                        "Clicks",
                        "Impressions",
                        "Cost",
                        "Conversions",
                        "AvgImpressionPosition",
                        "AvgClickPosition"
                    ],
                    "ReportName": f"YANDEX_DIRECT_CAMPAIGN_NOREGION_HISTORY_{datetime.datetime.now()}",
                    "ReportType": "CUSTOM_REPORT",
                    "DateRangeType": "CUSTOM_DATE",
                    "Format": "TSV",
                    "IncludeVAT": "NO",
                    "IncludeDiscount": "NO"
                }
            }

            resultcsv = report_generator(client['Login'], CampaignsURL, body)

            # Формирование DataFrame
            if resultcsv:
                df_camp = pandas.read_csv(io.StringIO(resultcsv), sep="\t", index_col=False, dtype = 
                    {            
                    'Date': 'string',
                    'ClientLogin': 'string',
                    'CampaignName': 'string',
                    'CampaignId': 'string',
                    'AdGroupName': 'string',
                    'AdGroupId': 'string',
                    'AdId': 'string',
                    'Criterion': 'string',
                    'Device': 'string',
                    'AdNetworkType': 'string',
                    'TargetingLocationName': 'string',
                    'Impressions': 'string',
                    'Clicks': 'string',
                    'Cost': 'string',
                    'Conversions': 'string',
                    'AvgClickPosition': 'string',
                    'AvgImpressionPosition': 'string'
                    }
                )
                data = pandas.concat([data, df_camp])
                try:
                    data.loc[data['ClientLogin'] == client['Login'], 'accountId'] = str(client['ClientId'])
                except ValueError as e:
                    print(f"Error: {e}")
                print("Dataset memory size =", data.memory_usage(index=True).sum())


        data.loc[data.Impressions == '--', 'Impressions'] = '0.0'
        data.loc[data.Clicks == '--', 'Clicks'] = '0.0'
        data.loc[data.Cost == '--', 'Cost'] = '0.0'
        data.loc[data.Conversions == '--', 'Conversions'] = '0.0'
        data.loc[data.AvgClickPosition == '--', 'AvgClickPosition'] = '0.0'
        data.loc[data.AvgImpressionPosition == '--', 'AvgImpressionPosition'] = '0.0'
        data.rename(columns={
            'Date': 'date',
            'ClientLogin': 'accountName',
            'CampaignName': 'campaignName',
            'CampaignId': 'campaignId',
            'AdGroupId': 'adGroupId',
            'AdGroupName': 'adGroupName',
            'Criterion': 'keyword',
            'AdId': 'adId',
            'AdNetworkType': 'advertisingChannelType',
            'Device': 'device',
            'TargetingLocationName': 'targetRegion',
            'Impressions': 'impressions',
            'Clicks': 'clicks',
            'Cost': 'costs',
            'Conversions': 'conversions',
            'AvgClickPosition': 'avgClickPosition',
            'AvgImpressionPosition': 'avgImpressionPosition'
            }, 
            inplace=True
        )
        data = data.astype({'impressions': 'float', 'clicks': 'float', 'costs': 'float', 'conversions': 'float', 'avgClickPosition': 'float', 'avgImpressionPosition': 'float'})
        data['date'] = pandas.to_datetime(data['date']).dt.date
        data = data[(data.impressions > 0) | ((data.clicks > 0) & (data.impressions == 0))]

        table = bq_client.get_table(TABLE_NAME_CAMPAIGN)
        generated_schema = [{'name':i.name, 'type':i.field_type} for i in table.schema]
        pandas_gbq.to_gbq(data, TABLE_NAME_CAMPAIGN, PROJECT, table_schema = generated_schema, if_exists='append')

        return True


    def get_ads_urls():
        CampaignsURL = 'https://api.direct.yandex.com/json/v5/reports'

        for client in clients:
            print(f'Генерация отчёта Campaign для {client}: {clients.index(client)} из {len(clients) - 1}')
            headers['Client-Login'] = client

            body1 = {
                "method": "get",
                "params": {
                    "SelectionCriteria": {"AdGroupIds": adgroup_id_list,
                                          },
                    "FieldNames": ["Id", "AdGroupId", "CampaignId"],
                    "TextAdFieldNames": ["Href"],
                    "TextImageAdFieldNames": ["Href"],
                    "CpcVideoAdBuilderAdFieldNames": ["Href"],
                    'TextAdBuilderAdFieldNames': ['Href'],
                    'CpmBannerAdBuilderAdFieldNames': ['Href'],
                    'CpmVideoAdBuilderAdFieldNames': ['Href'],
                    'DynamicTextAdFieldNames': ['AdImageHash']

                }
            }

            # Отправка запроса
            response = requests.post(
                'https://api.direct.yandex.com/json/v5/ads',
                headers=headers,
                json=body1
            )

            # Обработка ответа
            response_data = json.loads(response.text)
            print(response_data)
            ids = []
            adgroup_ids = []
            hrefs = []
            campaign_ids = []
            result = response_data.get('result', {})
            ads = result.get('Ads', [])

            for ad in ads:
                ids.append(ad.get('Id'))
                adgroup_ids.append(ad.get('AdGroupId'))
                campaign_ids.append(ad.get('CampaignId'))
                if ad.get('Type') == 'CPM_BANNER_AD' and ad.get('Subtype') == 'NONE':
                    hrefs.append(ad.get('CpmBannerAdBuilderAd', {}).get('Href'))
                elif ad.get('Type') == 'IMAGE_AD' and ad.get('Subtype') == 'TEXT_IMAGE_AD':
                    hrefs.append(ad.get('TextImageAd', {}).get('Href'))
                elif ad.get('Subtype') == 'TEXT_AD_BUILDER_AD' and ad.get('Type') == 'IMAGE_AD':
                    hrefs.append(ad.get('TextAdBuilderAd', {}).get('Href'))
                elif ad.get('Subtype') == 'TEXT_AD_BUILDER_AD':
                    hrefs.append(ad.get('TextAdBuilderAd', {}).get('Href'))
                elif ad.get('Type') == 'TEXT_AD':
                    hrefs.append(ad.get('TextAd', {}).get('Href'))                
                elif ad.get('Type') == 'IMAGE_AD':
                    hrefs.append(ad.get('TextImageAd', {}).get('Href'))
                elif ad.get('Type') == 'CPM_BANNER_AD':
                    hrefs.append(ad.get('CpmBannerAdBuilderAd', {}).get('Href'))
                elif ad.get('Type') == 'CPC_VIDEO_AD':
                    hrefs.append(ad.get('CpcVideoAdBuilderAd', {}).get('Href'))
                elif ad.get('Type') == 'CPM_VIDEO_AD':
                    hrefs.append(ad.get('CpmVideoAdBuilderAd', {}).get('Href'))
                elif ad.get('Type') == 'DYNAMIC_TEXT_AD' and ad.get('Subtype') == 'NONE':
                    hrefs.append(ad.get('DynamicTextAd', {}).get('AdImageHash'))
                else:
                    hrefs.append('null')
            df = pandas.DataFrame({'Id': ids, 'AdGroupId': adgroup_ids, 'Href': hrefs, "CampaignId": campaign_ids})

            df = df.astype({'Id': 'string', 'AdGroupId': 'string', 'CampaignId': 'string'})
            df = df.rename(columns={'Href': 'finalUrls', 'CampaignId': 'campaignId'})

            df['finalUrls'] = df['finalUrls'].astype(str)

            df['finalUrls'] = df['finalUrls'].str.extract(r'(http[^?]+)')

            # Используйте str.replace() для удаления символов "'" и "]"
            df['finalUrls'] = df['finalUrls'].str.replace("'", "").str.replace("]", "")
            df = df.drop_duplicates()
            table = bq_client.get_table(TABLE_NAME_URL)
            generated_schema = [{'name': i.name, 'type': i.field_type} for i in table.schema]
            pandas_gbq.to_gbq(df, TABLE_NAME_URL, PROJECT, table_schema=generated_schema, if_exists='append')


    # Step 3. Parameters of get data function . Edit it
    # get_audience_data_from_source = PythonOperator(
    #     task_id='get_audience_data_from_source',
    #     execution_timeout=datetime.timedelta(hours=1),
    #     python_callable=get_data_audience,
    # )

    get_placement_data_from_source = PythonOperator(
        task_id='get_placement_data_from_source',
        execution_timeout=datetime.timedelta(hours=1),
        python_callable=get_data_placement,
    )

    get_campaign_data_from_source = PythonOperator(
        task_id='get_campaign_data_from_source',
        execution_timeout=datetime.timedelta(hours=1),
        python_callable=get_data_campaign,
    )

    get_ads_urls_from_source = PythonOperator(
        task_id='get_ads_urls_from_source',
        execution_timeout=datetime.timedelta(hours=1),
        python_callable=get_ads_urls,
    )

# delete_audience_records >> get_audience_data_from_source
delete_placement_records >> get_placement_data_from_source
delete_campaign_records >> get_campaign_data_from_source >> get_ads_urls_from_source
