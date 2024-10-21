import sys
import datetime
from xml.dom.pulldom import START_DOCUMENT
from numpy import int64
from time import sleep
import pandas, pandas_gbq, json
from google.cloud import bigquery
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.v13.enums.types.bidding_strategy_type import BiddingStrategyTypeEnum
from google.ads.googleads.v13.enums.types.change_client_type import ChangeClientTypeEnum
from google.ads.googleads.v13.enums.types.change_event_resource_type import ChangeEventResourceTypeEnum

from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from common.common_function import task_fail_telegram_alert

################################## Parameters. Don't edit it! ##################################
SOURCE_NAME = 'google_ads_api'
DAG_NUMBER = '1'
LOCATION = 'europe-west3'
PROJECT = 'southern-idea-345503'
DATASET = 'raw_data'
START_DATE = datetime.datetime.now() - datetime.timedelta(days=7)
START_DATE = START_DATE.strftime('%Y-%m-%d')
TODAY = datetime.datetime.now()
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
END_DATE = datetime.datetime.now() - datetime.timedelta(days=0)
END_DATE = END_DATE.strftime('%Y-%m-%d')
#END_DATE = '2023-03-02'

TABLE_IS = "raw_data_google_ads_IS"
#TABLE_AUDIENCE = 'google_ads_audience_api_dag_1'
TABLE_CAMPAIGN = 'google_ads_campaign_api_dag_1'
TABLE_PLACEMENT = 'google_ads_placement_api_dag_1'
TABLE_CAMPAIGN_LOCATION = 'google_ads_campaign_location_api_dag_1'
TABLE_CHANGE_HISTORY = 'google_activity'
#TABLE_NAME_AUDIENCE = f'{PROJECT}.{DATASET}.{TABLE_AUDIENCE}'
TABLE_NAME_CAMPAIGN = f'{PROJECT}.{DATASET}.{TABLE_CAMPAIGN}'
TABLE_NAME_PLACEMENT = f'{PROJECT}.{DATASET}.{TABLE_PLACEMENT}'
TABLE_NAME_CAMPAIGN_LOCATION = f'{PROJECT}.{DATASET}.{TABLE_CAMPAIGN_LOCATION}'
TABLE_NAME_IS = f'{PROJECT}.{DATASET}.{TABLE_IS}'
TABLE_NAME_CHANGE_HISTORY = f'{PROJECT}.{DATASET}.{TABLE_CHANGE_HISTORY}'

def google_ads_authentication(cred_dict):
    try:
        client = GoogleAdsClient.load_from_dict(cred_dict)
        print("\nFunction (google_ads_authentication) finished successfully. ")
        return client
    except:
        print("\n*** Function(google_ads_authentication) Failed *** ",sys.exc_info())

######################## Get all accounts in MCC account ########################    
def get_hierarchy(client):
    accounts = {}
    googleads_service = client.get_service("GoogleAdsService")

    query = """
            SELECT
                customer_client.client_customer,
                customer_client.descriptive_name
            FROM customer_client
            """

    # Issues a search request using streaming.
    response = googleads_service.search(customer_id='3788278247', query=query)
    for googleads_row in response:
                customer_client = googleads_row.customer_client.client_customer.split('/')[1]
                accounts[customer_client] = googleads_row.customer_client.descriptive_name
    return accounts

api_client = google_ads_authentication(json.loads(models.Variable.get("google_ads_creds_secret"))) # authorization
accounts = get_hierarchy(api_client) # get accounts
del accounts['3788278247'] # delete MCC account id

bq_client = bigquery.Client()

default_args = {
    'owner': 'Composer Example',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=2),
    'execution_timeout': datetime.timedelta(hours=2),
    'on_failure_callback': task_fail_telegram_alert
}
with models.DAG(
    dag_id=f'{DATASET}_{SOURCE_NAME}_dag_{DAG_NUMBER}',
    default_args=default_args,
    catchup=False,
    start_date=YESTERDAY,
    schedule_interval='30 1 * * *',
    tags = ['load','google_ads','api'],
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
    #                         date >= '{START_DATE}'
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
                            date >= '{START_DATE}'
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
                            date >= '{START_DATE}'
                    """,
                "useLegacySql": False
            }
        }
    )

    delete_campaign_location_records = BigQueryInsertJobOperator(
            task_id='delete_campaign_location_records',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query":
                    f"""
                        DELETE 
                        FROM 
                            `{TABLE_NAME_CAMPAIGN_LOCATION}`
                        WHERE 
                            date >= '{START_DATE}'
                    """,
                "useLegacySql": False
            }
        }
    )

    delete_IS_records = BigQueryInsertJobOperator(
        task_id='delete_IS_records',
        gcp_conn_id='google_cloud_default',
        configuration={
            "query": {
                "query":
                    f"""
                                DELETE 
                                FROM 
                                    `{TABLE_NAME_IS}`
                                WHERE 
                                    date >= '{START_DATE}'
                            """,
                "useLegacySql": False
            }
        }
    )

    delete_changed_history_records = BigQueryInsertJobOperator(
        task_id='delete_changed_history_records',
        gcp_conn_id='google_cloud_default',
        configuration={
            "query": {
                "query":
                    f"""
                                DELETE 
                                FROM 
                                    `{TABLE_NAME_CHANGE_HISTORY}`
                                WHERE 
                                    date >= '{START_DATE}'
                            """,
                "useLegacySql": False
            }
        }
    )

    # Функция запроса данных Audience в Google Ads
    # def get_data_audience():
    #     tmp_dict = {}
    #     tmp_dict['date'] = []
    #     tmp_dict['accountName'] = []
    #     tmp_dict['campaignName'] = []
    #     tmp_dict['campaignId'] = []
    #     tmp_dict['audienceName'] = []
    #     tmp_dict['audienceId'] = []
    #     tmp_dict['types'] = []
    #     tmp_dict['clicks'] = []
    #     tmp_dict['impressions'] = []
    #     tmp_dict['cost'] = []
    #     tmp_dict['conversions'] = []
    #     tmp_dict['accountId'] = []
    #     for account_id in accounts:
    #         try:
    #             query = f"""
    #                 SELECT 
    #                     segments.date, 
    #                     campaign.name,
    #                     campaign.id,
    #                     ad_group_criterion.criterion_id,
    #                     ad_group_criterion.type,
    #                     ad_group_criterion.display_name,
    #                     metrics.impressions,
    #                     metrics.clicks,
    #                     metrics.cost_micros,
    #                     metrics.conversions
    #                 FROM 
    #                     ad_group_audience_view 
    #                 WHERE 
    #                     segments.date BETWEEN '{START_DATE}' AND '{END_DATE}'
    #                 ORDER BY segments.date ASC
    #             """

    #             ga_service = api_client.get_service("GoogleAdsService")
    #             search_request = api_client.get_type("SearchGoogleAdsStreamRequest")
    #             search_request.customer_id = account_id
    #             search_request.query = query
    #             response = ga_service.search_stream(search_request)
                
    #             for batch in response:
    #                 if(batch):
    #                     for row in batch.results:
    #                         tmp_dict['date'].append(row.segments.date)
    #                         tmp_dict['accountName'].append(accounts[account_id])
    #                         tmp_dict['campaignName'].append(row.campaign.name)
    #                         tmp_dict['campaignId'].append(str(row.campaign.id))
    #                         tmp_dict['audienceId'].append(str(row.ad_group_criterion.criterion_id))
    #                         tmp_dict['audienceName'].append(str(row.ad_group_criterion.display_name))
    #                         tmp_dict['types'].append(str(row.ad_group_criterion.type_))
    #                         tmp_dict['clicks'].append(row.metrics.clicks)
    #                         tmp_dict['impressions'].append(row.metrics.impressions)
    #                         tmp_dict['cost'].append(row.metrics.cost_micros/1000000)
    #                         tmp_dict['conversions'].append(row.metrics.conversions)
    #                         tmp_dict['accountId'].append(account_id)
    #         except:
    #             print("\n*** Function (get_ads_data) Failed *** ",sys.exc_info())
    #     df = pandas.DataFrame.from_dict(tmp_dict) 
    #     table = bq_client.get_table(TABLE_NAME_AUDIENCE)
    #     generated_schema = [{'name':i.name, 'type':i.field_type} for i in table.schema]
    #     pandas_gbq.to_gbq(df, TABLE_NAME_AUDIENCE, PROJECT, table_schema = generated_schema, if_exists='append')

    #     return True

    def get_data_campaign_location():
        tmp_dict = {}
        tmp_dict['date'] = []
        tmp_dict['accountName'] = []
        tmp_dict['campaignName'] = []
        tmp_dict['campaignId'] = []
        tmp_dict['targetLocationID'] = []
        tmp_dict['clicks'] = []
        tmp_dict['impressions'] = []
        tmp_dict['cost'] = []
        tmp_dict['conversions'] = []
        tmp_dict['accountId'] = []

        for account_id in accounts:
            try:        
                query = f"""
                    SELECT
                        campaign.name,
                        campaign.id,                       
                        user_location_view.country_criterion_id,
                        metrics.impressions,
                        metrics.clicks,
                        metrics.cost_micros,
                        metrics.conversions,
                        segments.date
                    FROM 
                        user_location_view
                    WHERE 
                        segments.date BETWEEN '{START_DATE}' AND '{END_DATE}'"""
                    
                ga_service = api_client.get_service("GoogleAdsService")
                search_request = api_client.get_type("SearchGoogleAdsStreamRequest")
                search_request.customer_id = account_id
                search_request.query = query
                response = ga_service.search_stream(search_request)
                
                for batch in response:
                    for row in batch.results:
                        tmp_dict['date'].append(row.segments.date)
                        tmp_dict['accountName'].append(accounts[account_id])
                        tmp_dict['campaignName'].append(str(row.campaign.name))
                        tmp_dict['campaignId'].append(str(row.campaign.id))
                        tmp_dict['targetLocationID'].append(str(row.user_location_view.country_criterion_id))
                        tmp_dict['clicks'].append(row.metrics.clicks)
                        tmp_dict['impressions'].append(row.metrics.impressions)
                        tmp_dict['cost'].append(row.metrics.cost_micros/1000000)
                        tmp_dict['conversions'].append(row.metrics.conversions)
                        tmp_dict['accountId'].append(account_id)
            except:
                print("\n*** Function (get_ads_data) Failed *** ",sys.exc_info())
        df_location = pandas.DataFrame.from_dict(tmp_dict)
        table = bq_client.get_table(TABLE_NAME_CAMPAIGN_LOCATION)
        generated_schema = [{'name':i.name, 'type':i.field_type} for i in table.schema]
        pandas_gbq.to_gbq(df_location, TABLE_NAME_CAMPAIGN_LOCATION, PROJECT, table_schema = generated_schema, if_exists='append')

        return True 



    # Фуекция запроса Placement в Google Ads
    def get_data_placement():
        tmp_dict = {}
        tmp_dict['date'] = []
        tmp_dict['accountName'] = []
        tmp_dict['campaignName'] = []
        tmp_dict['campaignId'] = []
        tmp_dict['placement'] = []
        tmp_dict['clicks'] = []
        tmp_dict['impressions'] = []
        tmp_dict['costs'] = []
        tmp_dict['conversions'] = []
        tmp_dict['accountId'] = []

        for account_id in accounts:
            try:
                query = f"""
                    SELECT 
                        segments.date, 
                        campaign.name,
                        campaign.id,
                        group_placement_view.placement,
                        metrics.clicks, 
                        metrics.impressions, 
                        metrics.cost_micros, 
                        metrics.conversions
                    FROM 
                        group_placement_view 
                    WHERE 
                        segments.date BETWEEN '{START_DATE}' AND '{END_DATE}'
                    ORDER BY segments.date ASC
                """

                ga_service = api_client.get_service("GoogleAdsService")
                search_request = api_client.get_type("SearchGoogleAdsStreamRequest")
                search_request.customer_id = account_id
                search_request.query = query
                response = ga_service.search_stream(search_request)
                
                for batch in response:
                    if(batch):
                        for row in batch.results:
                            tmp_dict['date'].append(row.segments.date)
                            tmp_dict['accountName'].append(accounts[account_id])
                            tmp_dict['campaignName'].append(row.campaign.name)
                            tmp_dict['campaignId'].append(str(row.campaign.id))
                            tmp_dict['placement'].append(str(row.group_placement_view.placement))
                            tmp_dict['clicks'].append(row.metrics.clicks)
                            tmp_dict['impressions'].append(row.metrics.impressions)
                            tmp_dict['costs'].append(row.metrics.cost_micros/1000000)
                            tmp_dict['conversions'].append(row.metrics.conversions)
                            tmp_dict['accountId'].append(account_id)
            except:
                print("\n*** Function (get_ads_data) Failed *** ",sys.exc_info())
        df = pandas.DataFrame.from_dict(tmp_dict) 
        table = bq_client.get_table(TABLE_NAME_PLACEMENT)
        generated_schema = [{'name':i.name, 'type':i.field_type} for i in table.schema]
        pandas_gbq.to_gbq(df, TABLE_NAME_PLACEMENT, PROJECT, table_schema = generated_schema, if_exists='append')

        return True
        

    # Функция запроса кампаний из Google Ads
    def get_data_campaign():
        tmp_dict = {}
        tmp_dict['date'] = []
        tmp_dict['startDate'] = []
        tmp_dict['accountName'] = []
        tmp_dict['campaignName'] = []
        tmp_dict['campaignId'] = []
        tmp_dict['adGroupName'] = []
        tmp_dict['adGroupId'] = []
        tmp_dict['adId'] = []
        tmp_dict['keyword'] = []
        tmp_dict['keywordMatchType'] = []
        tmp_dict['advertisingChannelType'] = []
        tmp_dict['clicks'] = []
        tmp_dict['impressions'] = []
        tmp_dict['cost'] = []
        tmp_dict['conversions'] = []
        tmp_dict['finalUrls'] = []
        tmp_dict['accountId'] = []
        tmp_dict['bidding_type'] = []
        bidding_strategy_type_enum = BiddingStrategyTypeEnum()

        df6 = pandas.DataFrame()

        for account_id in accounts:
            try:
                query = f"""
                                SELECT
                                    ad_group_ad.ad.id,
                                    campaign.advertising_channel_type,                            
                                    metrics.conversions,
                                    campaign.id,
                                    campaign.name,
                                    ad_group.name,
                                    ad_group.id, 
                                    segments.keyword.info.match_type,
                                    segments.keyword.info.text,
                                    metrics.impressions,
                                    metrics.clicks,
                                    metrics.cost_micros,                       
                                    campaign.start_date,
                                    segments.date,
                                    ad_group_ad.ad.final_urls,
                                    campaign.bidding_strategy_type

                                FROM 
                                    ad_group_ad
                                WHERE 
                                    segments.date BETWEEN '{START_DATE}' AND '{END_DATE}' 
            """

                ga_service = api_client.get_service("GoogleAdsService")
                search_request = api_client.get_type("SearchGoogleAdsStreamRequest")
                search_request.customer_id = account_id
                search_request.query = query
                response = ga_service.search_stream(search_request)

                for batch in response:

                    for row in batch.results:
                        bidding_type_str = bidding_strategy_type_enum.BiddingStrategyType(
                            row.campaign.bidding_strategy_type).name
                        tmp_dict['date'].append(row.segments.date)
                        tmp_dict['startDate'].append(row.campaign.start_date)
                        tmp_dict['accountName'].append(accounts[account_id])
                        tmp_dict['campaignName'].append(str(row.campaign.name))
                        tmp_dict['campaignId'].append(str(row.campaign.id))
                        tmp_dict['adGroupName'].append(str(row.ad_group.name))
                        tmp_dict['adGroupId'].append(str(row.ad_group.id))
                        tmp_dict['adId'].append(str(row.ad_group_ad.ad.id))
                        tmp_dict['keyword'].append(str(row.segments.keyword.info.text))
                        tmp_dict['keywordMatchType'].append(str(row.segments.keyword.info.match_type))
                        tmp_dict['advertisingChannelType'].append(str(row.campaign.advertising_channel_type))
                        tmp_dict['clicks'].append(row.metrics.clicks)
                        tmp_dict['impressions'].append(row.metrics.impressions)
                        tmp_dict['cost'].append(row.metrics.cost_micros / 1000000)
                        tmp_dict['conversions'].append(row.metrics.conversions)
                        tmp_dict['finalUrls'].append(str(row.ad_group_ad.ad.final_urls))
                        tmp_dict['accountId'].append(account_id)
                        tmp_dict['bidding_type'].append(bidding_type_str)

                df1 = pandas.DataFrame.from_dict(tmp_dict)

                for k in tmp_dict:
                    tmp_dict[k].clear()

                query = f"""
                            SELECT
                                segments.date,
                                campaign.start_date,
                                campaign.name,
                                campaign.id,
                                ad_group.name,
                                ad_group.id,
                                ad_group_ad.ad.id,                        
                                campaign.advertising_channel_type,
                                metrics.impressions,
                                metrics.clicks,
                                metrics.cost_micros,
                                metrics.conversions,                    
                                ad_group_ad.ad.final_urls,
                                campaign.bidding_strategy_type
                            FROM 
                                ad_group_ad
                            WHERE  
                                segments.date BETWEEN '{START_DATE}' AND '{END_DATE}'  """

                ga_service = api_client.get_service("GoogleAdsService")
                search_request = api_client.get_type("SearchGoogleAdsStreamRequest")
                search_request.customer_id = account_id
                search_request.query = query

                response = ga_service.search_stream(search_request)

                for batch in response:

                    for row in batch.results:
                        bidding_type_str = bidding_strategy_type_enum.BiddingStrategyType(
                            row.campaign.bidding_strategy_type).name
                        tmp_dict['date'].append(row.segments.date)
                        tmp_dict['startDate'].append(row.campaign.start_date)
                        tmp_dict['accountName'].append(accounts[account_id])
                        tmp_dict['campaignName'].append(str(row.campaign.name))
                        tmp_dict['campaignId'].append(str(row.campaign.id))
                        tmp_dict['adGroupName'].append(str(row.ad_group.name))
                        tmp_dict['adGroupId'].append(str(row.ad_group.id))
                        tmp_dict['adId'].append(str(row.ad_group_ad.ad.id))
                        tmp_dict['keyword'].append('--')
                        tmp_dict['keywordMatchType'].append('--')
                        tmp_dict['advertisingChannelType'].append(str(row.campaign.advertising_channel_type))
                        tmp_dict['clicks'].append(row.metrics.clicks)
                        tmp_dict['impressions'].append(row.metrics.impressions)
                        tmp_dict['cost'].append(row.metrics.cost_micros / 1000000)
                        tmp_dict['conversions'].append(row.metrics.conversions)
                        tmp_dict['finalUrls'].append(str(row.ad_group_ad.ad.final_urls))
                        tmp_dict['accountId'].append(account_id)
                        tmp_dict['bidding_type'].append(bidding_type_str)
                df2 = pandas.DataFrame.from_dict(tmp_dict)

                df1_sum = df1.groupby(['date', 'startDate', 'campaignName', 'adGroupName', 'adId'])[
                    'cost', 'clicks', 'conversions', 'impressions'].sum().reset_index()
                df3 = df2.merge(df1_sum, on=['date', 'startDate', 'campaignName', 'adGroupName', 'adId'], how='left')
                df3['new_cost'] = df3['cost_x'] - df3['cost_y'].fillna(0)
                df3['new_clicks'] = df3['clicks_x'] - df3['clicks_y'].fillna(0)
                df3['new_conversions'] = df3['conversions_x'] - df3['conversions_y'].fillna(0)
                df3['new_impressions'] = df3['impressions_x'] - df3['impressions_y'].fillna(0)
                df3 = df3.drop(
                    ['cost_x', 'cost_y', 'clicks_x', 'clicks_y', 'conversions_x', 'conversions_y', 'impressions_x',
                     'impressions_y'], axis=1)
                df3 = df3.rename(columns={'new_cost': 'cost', 'new_clicks': 'clicks', 'new_conversions': 'conversions',
                                          'new_impressions': 'impressions'})
                df4 = pandas.concat([df1, df3])

                for k in tmp_dict:
                    tmp_dict[k].clear()

                query = f"""
                            SELECT
                                    segments.date,
                                    campaign.start_date,
                                    campaign.name,
                                    campaign.id,
                                    metrics.impressions,
                                    metrics.clicks,
                                    metrics.cost_micros,
                                    metrics.conversions,
                                    landing_page_view.unexpanded_final_url,
                                    campaign.advertising_channel_type,
                                    campaign.bidding_strategy_type

                                FROM 
                                    landing_page_view  
                            WHERE 
                                segments.date BETWEEN '{START_DATE}' AND '{END_DATE}' AND campaign.advertising_channel_type = PERFORMANCE_MAX """

                ga_service = api_client.get_service("GoogleAdsService")
                search_request = api_client.get_type("SearchGoogleAdsStreamRequest")
                search_request.customer_id = account_id
                search_request.query = query
                response = ga_service.search_stream(search_request)

                for batch in response:
                    for row in batch.results:
                        bidding_type_str = bidding_strategy_type_enum.BiddingStrategyType(
                            row.campaign.bidding_strategy_type).name
                        tmp_dict['date'].append(row.segments.date)
                        tmp_dict['startDate'].append(row.campaign.start_date)
                        tmp_dict['accountName'].append(accounts[account_id])
                        tmp_dict['campaignName'].append(str(row.campaign.name))
                        tmp_dict['campaignId'].append(str(row.campaign.id))
                        tmp_dict['adGroupName'].append(str(row.ad_group.name))
                        tmp_dict['adGroupId'].append(str(row.ad_group.id))
                        tmp_dict['adId'].append(str(row.ad_group_ad.ad.id))
                        tmp_dict['keyword'].append('--')
                        tmp_dict['keywordMatchType'].append('--')
                        tmp_dict['advertisingChannelType'].append(str(row.campaign.advertising_channel_type))
                        tmp_dict['clicks'].append(row.metrics.clicks)
                        tmp_dict['impressions'].append(row.metrics.impressions)
                        tmp_dict['cost'].append(row.metrics.cost_micros / 1000000)
                        tmp_dict['conversions'].append(row.metrics.conversions)
                        tmp_dict['finalUrls'].append(row.landing_page_view.unexpanded_final_url)
                        tmp_dict['accountId'].append(account_id)
                        tmp_dict['bidding_type'].append(bidding_type_str)
                df5 = pandas.DataFrame.from_dict(tmp_dict)
                df6 = pandas.concat([df6, df4, df5]).drop_duplicates()
            except:
                print("\n*** Function (get_ads_data) Failed *** ", sys.exc_info())
        table = bq_client.get_table(TABLE_NAME_CAMPAIGN)
        generated_schema = [{'name':i.name, 'type':i.field_type} for i in table.schema]
        pandas_gbq.to_gbq(df6, TABLE_NAME_CAMPAIGN, PROJECT, table_schema = generated_schema, if_exists='append')

        return True

    def get_data_impression_share():
        tmp_dict = {}
        tmp_dict['date'] = []
        tmp_dict['accountName'] = []
        tmp_dict['campaignName'] = []
        tmp_dict['campaignId'] = []
        tmp_dict['avgImpressionPosition'] = []
        tmp_dict['avgClickPosition'] = []
        tmp_dict['accountId'] = []

        for account_id in accounts:
            try:
                query = f"""
                    SELECT
                        segments.date,                    
                        campaign.name,
                        campaign.id,                        
                        metrics.absolute_top_impression_percentage,
                        metrics.top_impression_percentage
                    FROM 
                        campaign
                    WHERE 
                        segments.date BETWEEN '{START_DATE}' AND '{END_DATE}' """


                ga_service = api_client.get_service("GoogleAdsService")
                search_request = api_client.get_type("SearchGoogleAdsStreamRequest")
                search_request.customer_id = account_id
                search_request.query = query
                response = ga_service.search_stream(search_request)

                for batch in response:

                    for row in batch.results:
                        tmp_dict['date'].append(row.segments.date)
                        tmp_dict['accountName'].append(accounts[account_id])
                        tmp_dict['campaignName'].append(str(row.campaign.name))
                        tmp_dict['campaignId'].append(str(row.campaign.id))
                        tmp_dict['avgImpressionPosition'].append(row.metrics.absolute_top_impression_percentage)
                        tmp_dict['avgClickPosition'].append(row.metrics.top_impression_percentage)
                        tmp_dict['accountId'].append(account_id)
            except:
                print("\n*** Function (get_ads_data) Failed *** ", sys.exc_info())

        df = pandas.DataFrame.from_dict(tmp_dict)
        print(df.head)

        table = bq_client.get_table(TABLE_NAME_IS)
        generated_schema = [{'name': i.name, 'type': i.field_type} for i in table.schema]
        pandas_gbq.to_gbq(df, TABLE_NAME_IS, PROJECT, table_schema=generated_schema, if_exists='append')

        return True

    def get_data_activity():
        tmp_dict = {}
        tmp_dict['date'] = []
        tmp_dict['client_type'] = []
        tmp_dict['change_resource_name'] = []
        tmp_dict['campaign'] = []
        tmp_dict['ad_group'] = []
        tmp_dict['changed_fields'] = []
        tmp_dict['asset'] = []
        tmp_dict['change_resource_type'] = []
        tmp_dict['feed'] = []
        tmp_dict['feed_item'] = []
        tmp_dict['new_resource'] = []
        tmp_dict['old_resource'] = []
        tmp_dict['resource_change_operation'] = []
        tmp_dict['resource_name'] = []
        tmp_dict['user_email'] = []
        tmp_dict['accountName'] = []

        client_type_enum = ChangeClientTypeEnum()

        resource_type_enum = ChangeEventResourceTypeEnum()

        for account_id in accounts:
            print(account_id)

            try:
                query = f"""
                    SELECT
    
            change_event.change_date_time,
            change_event.change_resource_name,
            change_event.campaign,
            change_event.ad_group,
            change_event.client_type,
            change_event.changed_fields,
            change_event.asset,
            change_event.change_resource_type,
            change_event.feed,
            change_event.feed_item,
            change_event.new_resource,
            change_event.old_resource,
            change_event.resource_change_operation,
            change_event.resource_name,
            change_event.user_email
    
            FROM change_event
            WHERE change_event.change_date_time between  '{START_DATE}' AND '{END_DATE}'  limit 10000
                """

                ga_service = api_client.get_service("GoogleAdsService")
                search_request = api_client.get_type("SearchGoogleAdsStreamRequest")
                print(search_request)
                search_request.customer_id = account_id
                search_request.query = query
                response = ga_service.search_stream(search_request)

                for batch in response:
                    if(batch):
                        for row in batch.results:
                            print(row)
                            client_type_str = client_type_enum.ChangeClientType(
                                row.change_event.client_type).name
                            resource_type_str = resource_type_enum.ChangeEventResourceType(
                                row.change_event.change_resource_type).name
                            tmp_dict['date'].append(row.change_event.change_date_time)
                            tmp_dict['client_type'].append(client_type_str)
                            tmp_dict['change_resource_name'].append(str(row.change_event.change_resource_name))
                            tmp_dict['campaign'].append(str(row.change_event.campaign))
                            tmp_dict['ad_group'].append(str(row.change_event.ad_group))
                            tmp_dict['changed_fields'].append(str(row.change_event.changed_fields))
                            tmp_dict['asset'].append(str(row.change_event.asset))
                            tmp_dict['change_resource_type'].append(resource_type_str)
                            tmp_dict['feed'].append(str(row.change_event.feed))
                            tmp_dict['feed_item'].append(str(row.change_event.feed_item))
                            tmp_dict['new_resource'].append(str(row.change_event.new_resource))
                            tmp_dict['old_resource'].append(str(row.change_event.old_resource))
                            tmp_dict['resource_change_operation'].append(str(row.change_event.resource_change_operation))
                            tmp_dict['resource_name'].append(str(row.change_event.resource_name))
                            tmp_dict['user_email'].append(str(row.change_event.user_email))
                            tmp_dict['accountName'].append(accounts[account_id])


            except:
                print("\n*** Function (get_ads_data) Failed *** ",sys.exc_info())

        df = pandas.DataFrame.from_dict(tmp_dict)
        df = df.astype(str)
        table = bq_client.get_table(TABLE_NAME_CHANGE_HISTORY)
        generated_schema = [{'name': i.name, 'type': i.field_type} for i in table.schema]
        pandas_gbq.to_gbq(df, TABLE_NAME_CHANGE_HISTORY, PROJECT, table_schema=generated_schema, if_exists='append')

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

    get_campaign_location_data_from_source = PythonOperator(
        task_id='get_campaign_location_data_from_source',
        execution_timeout=datetime.timedelta(hours=1),
        python_callable=get_data_campaign_location,
    )

    get_data_impression_share = PythonOperator(
        task_id='get_data_impression_share',
        execution_timeout=datetime.timedelta(hours=1),
        python_callable=get_data_impression_share,
    )

    get_data_activity_from_source = PythonOperator(
        task_id='get_changed_history_data',
        execution_timeout=datetime.timedelta(hours=1),
        python_callable=get_data_activity,
    )

    #delete_audience_records >> get_audience_data_from_source
    delete_placement_records >> get_placement_data_from_source
    delete_campaign_records >> get_campaign_data_from_source
    delete_campaign_location_records >> get_campaign_location_data_from_source
    delete_IS_records >> get_data_impression_share
    delete_changed_history_records >> get_data_activity_from_source