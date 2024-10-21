from datetime import datetime, date, timedelta
import logging

from airflow import models
from airflow.operators.python_operator import PythonOperator

from custom_modules.internal_data_processing import task_fail_slack_alert
from airflow.hooks.base_hook import BaseHook
from custom_modules import internal_clickhouse
from custom_modules import internal_slack

# Fill DAG's parameters
default_args = {
    'owner': '@U06LQGHAKBN',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'on_failure_callback': task_fail_slack_alert
}

START_DATE = datetime(2024, 5, 2, 0, 0, 0)
REPORT_DATE = date.today()
DAG_NAME = 'slack_alert_hourly'
DAG_NUMBER = '1'
DESCRIPTION = 'Alert to Slack that shoud be executed every hour'

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Slack connection
slack_con = internal_slack.Slack(models.Variable.get('slack_token_analyticsbot'))

# Clickhouse connections
ch_analytics_hook = BaseHook.get_connection('clickhouse_analytics')
int_client_ch_analytics = internal_clickhouse.Clickhouse(
    host=ch_analytics_hook.host, 
    port=ch_analytics_hook.port, 
    username=ch_analytics_hook.login, 
    password=ch_analytics_hook.password
)

ch_qb_hook = BaseHook.get_connection('clickhouse_qb')
int_client_ch_qb = internal_clickhouse.Clickhouse(
    host=ch_qb_hook.host, 
    port=ch_qb_hook.port, 
    username=ch_qb_hook.login, 
    password=ch_qb_hook.password
)

################################## Parametrs. Don't edit it! ##################################
with models.DAG(
        dag_id=f'{DAG_NAME}_dag_{DAG_NUMBER}',
        default_args=default_args,
        catchup=False,
        start_date=START_DATE,
        description=DESCRIPTION,
        schedule_interval='30 0-23/1 * * *',
        tags = ['slack','alert','api', 'sql'],
) as dag:

    def push_pager_duty_call(message):
        '''
        Send event with call to pager duty
        '''   
        import requests
        
        # Send pagerduty call
        url = ''
        headers = {
            'Content-Type': 'application/json',
        }

        data = {
            "payload": {
                "summary": message,
                "severity": "critical",
                "source": "Alert source"
            },
            "routing_key": "",
            "event_action": "trigger"
        }

        response = requests.post(url, headers=headers, json=data)

        return True

    def send_threshold_hedger_latency():
        '''
        Send alert to slack with tx info if latency higher than threshold values
        '''
        ################################## THRESHOLDS #################################################

        TRADE_SENT_THRESHOLD = 500 # Milliseconds
        TRADE_FOUND_THRESHOLD = 50 # Milliseconds

        ################################## THRESHOLDS #################################################

        # Query
        latency_query = f"""
            with hedge_deals as (
                select 
                    tx_hash,
                    orbit_instance,
                    toDateTime(tx_sent_at) as time,
                    tx_sent,
                    chappie_instance,
                    trade_found
                from mart_data_roxana.hedge_body
                where 
                    toStartOfHour(tx_sent_at) = toStartOfHour(toDateTime(now())) - INTERVAL 1 HOUR
                    and (tx_sent > {TRADE_SENT_THRESHOLD} or trade_found > {TRADE_FOUND_THRESHOLD})
                order by tx_sent
                limit 1
            )

            select * from hedge_deals
        """

        # Get values from CH
        df = int_client_ch_analytics.query_to_df(latency_query)
        logging.info(f"Hedger Alert: Get hedger metrics, rows: {df.shape[0]}")
        
        # If df is empty - we dont have threshold values
        if not df.empty:
            # Get parameters of threshold value
            time = df['time'].iloc[0]
            tx_hash = df['tx_hash'].iloc[0]
            orbit_instance = df['orbit_instance'].iloc[0]
            chappie_instance = df['chappie_instance'].iloc[0]
            tx_sent = int(round(df['tx_sent'].iloc[0],0))
            trade_found = int(round(df['trade_found'].iloc[0],0))
            logging.info(f"Hedger Alert: Found threshold values, trade sent: {tx_sent} (ms), trade found: {trade_found} (ms)")

            # Prepare the message
            message = f"| _*Time*_: {time} \n" \
                    f"| _*Tx Hash*_: {tx_hash.decode('utf-8')} \n" \
                    f"| _*Orbit Instance*_: {orbit_instance} \n" \
                    f"| _*Chappie Instance*_: {chappie_instance} \n"
            
            # We have two metrics - check what metric is higher than threshold and add to message
            if tx_sent > TRADE_SENT_THRESHOLD:
                trash_metric_sentence = f"| _*Trade Sent:*_: :bangbang: *{tx_sent}* (ms) :bangbang:"
                header = f"Alert | Trade Sent = {tx_sent} (ms)"
            else:
                trash_metric_sentence = f"| _*Trade Found:*_: :bangbang: *{trade_found}* (ms) :bangbang:"
                header = f"Alert | Trade Found = {trade_found} (ms)"
            
            message += trash_metric_sentence

            # Send message
            slack_con.post_blocks_message(
                date=REPORT_DATE,
                header=header,
                text=message,
                push_text=header,
                context_mark='Monitoring Alert',
                channel_name=''
            )
            logging.info(f"Hedger Alert: Threshold values was send to Slack")
        else:
            logging.info(f"Hedger Alert: No threshold values")
        return True

    def send_net_profit_loss():
        '''
        Send alert to slack with deals with loss in NET profit with slippage
        '''
        import pandas as pd

        ################################## THRESHOLDS #################################################

        threshold_loss_usd = -10

        ################################## THRESHOLDS #################################################

        # Query
        net_profit_query = f"""
            select
                tx_hash,
                host,
                pair,
                round(gross_profit,2) as gross_profit,
                round(gross_profit_with_slippage, 2) as gross_profit_with_slippage,
                round(base_fee, 2) as base_fee,
                round(bribe, 2) as bribe,
                gross_profit - bribe - base_fee as net,
                gross_profit_with_slippage - bribe - base_fee as net_with_slippage
            from mart_data_roxana.roxana_deals
            where
                toStartOfHour(timestamp) = toStartOfHour(toDateTime(now())) - INTERVAL 1 HOUR
                and not tx_failed
                and net_with_slippage <= {threshold_loss_usd}
            limit 1  
        """

        # Get values from CH
        df = int_client_ch_analytics.query_to_df(net_profit_query)
        logging.info(f"NET loss alert: Get tx with loss, rows: {df.shape[0]}")
        
        if not df.empty:
            
            tx_hash = df['tx_hash'].iloc[0]
            host = df['host'].iloc[0]
            pair = df['pair'].iloc[0]
            base_fee = df['base_fee'].iloc[0]
            bribe = df['bribe'].iloc[0]
            net = round(df['net'].iloc[0],2)
            net_with_slippage = round(df['net_with_slippage'].iloc[0],2)

            #push_pager_duty_call(f"Loss from {host}. The loss is {net_with_slippage}")

            # Prepare the message
            message = f"*Hash*: {tx_hash.decode('utf-8')}\n" \
                f"*Pair*: {pair}\n" \
                f"*Base_fee*: {base_fee}$ + *Bribe*: {bribe}$ \n" \
                f"*P&L*: {net}$, *With Slippage*: {net_with_slippage}$\n" \
                f"*Instance*: {host}"

            header = f"Alert | Net profit loss: {net_with_slippage}"
            # Send message
            slack_con.post_blocks_message(
                date=REPORT_DATE,
                header=header,
                text=message,
                push_text=header,
                context_mark='Monitoring Alert',
                reciever='@U06LQGHAKBN',
                channel_name=''
            )
            logging.info(f"NET loss alert: loss was sent to Slack")
        else:
            logging.info(f"NET loss alert: No loss txs")
        
        return True
    
    def send_out_of_litetime_price():
        '''
        Send alert to slack with deals that have high difference between price timestamp and sent to otc hedge
        '''
        import pandas as pd

        ################################## THRESHOLDS #################################################

        threshold_out_of_lifetime = -15

        ################################## THRESHOLDS #################################################

        # Query
        out_of_lifetime_query = f"""
            with a as (
                select 
                    tx_hash,
                    start,
                    parseDateTime64BestEffortOrNull(JSON_VALUE(JSON_VALUE(post, '$.additional_info'),'$.price_timestamp')) as price_ts,
                    JSON_VALUE(JSON_VALUE(post, '$.additional_info'),'$.ticker_index') as ticker,
                    JSON_VALUE(JSON_VALUE(post, '$.additional_info'),'$.pricer') as pricer,
                    timestamp_diff('ms',price_ts,start) / 1000 as diff,
                    right(ticker,1) as lifetime
                from raw_ch_qb.http_dumps
                where 
                    status=200
                    and (body like '%-r-3%') 
                    and body like '%"success":true%'
                    and JSON_VALUE(body, '$.result[0].type') = ''
                    and toStartOfHour(start) = toStartOfHour(now()) - INTERVAL 1 HOUR
                    and url like '%add-internal-otc%'

                union all 

                select 
                    tx_hash,
                    start,
                    parseDateTime64BestEffortOrNull(JSON_VALUE(JSON_VALUE(post, '$.additional_info'),'$.price_timestamp')) as price_ts,
                    JSON_VALUE(JSON_VALUE(post, '$.additional_info'),'$.ticker_index') as ticker,
                    JSON_VALUE(JSON_VALUE(post, '$.additional_info'),'$.pricer') as pricer,
                    timestamp_diff('ms',price_ts,start) / 1000 as diff,
                    right(ticker,1) as lifetime
                from raw_ch_qb.eth_tokyo_http_dumps
                where 
                    status=200
                    and (body like '%-r-3%') 
                    and body like '%"success":true%'
                    and JSON_VALUE(body, '$.result[0].type') = ''
                    and toStartOfHour(start) = toStartOfHour(now()) - INTERVAL 1 HOUR
                    and url like '%add-internal-otc%'
                
                union all 
        
                select 
                    tx_hash,
                    start,
                    parseDateTime64BestEffortOrNull(JSON_VALUE(JSON_VALUE(post, '$.additional_info'),'$.price_timestamp')) as price_ts,
                    JSON_VALUE(JSON_VALUE(post, '$.additional_info'),'$.ticker_index') as ticker,
                    JSON_VALUE(JSON_VALUE(post, '$.additional_info'),'$.pricer') as pricer,
                    timestamp_diff('ms',price_ts,start) / 1000 as diff,
                    right(ticker,1) as lifetime
                from raw_ch_qb.eth_graph_http_dumps
                where 
                    status=200
                    and (body like '%-r-3%') 
                    and body like '%"success":true%'
                    and JSON_VALUE(body, '$.result[0].type') = ''
                    and toStartOfHour(start) = toStartOfHour(now()) - INTERVAL 1 HOUR
                    and url like '%add-internal-otc%'
            )

            select 
                start,
                tx_hash,
                toUInt32OrNull(lifetime) - diff as out_of_lifetime,
                pricer
            from a
            where 
                toDate(price_ts) != '1970-01-01'
                and out_of_lifetime < {threshold_out_of_lifetime}
            limit 1
        """

        # Get values from CH
        df = int_client_ch_analytics.query_to_df(out_of_lifetime_query)
        logging.info(f"Out of lifetime price alert: Get tx with out of lifetime, rows: {df.shape[0]}")
        
        if not df.empty:
            
            tx_hash = df['tx_hash'].iloc[0]
            pricer = df['pricer'].iloc[0]
            time = df['start'].iloc[0]
            out_of_lifetime = round(df['out_of_lifetime'].iloc[0],2)

            # Prepare the message
            message = f"*Hash*: {tx_hash.decode('utf-8')}\n" \
                f"*Pricer*: {pricer}\n" \
                f"*Time*: {time} \n" \
                f"*Out of lifetime*: {out_of_lifetime} (s)"

            header = f"Alert | Out of price lifetime: {out_of_lifetime}"
            # Send message
            slack_con.post_blocks_message(
                date=REPORT_DATE,
                header=header,
                text=message,
                push_text=header,
                context_mark='Monitoring Alert',
                reciever='@U06LQGHAKBN',
                channel_name=''
            )
            logging.info(f"Out of lifetime price alert: sent to Slack")
        else:
            logging.info(f"Out of lifetime price alert: No threshold")
        
        return True

    def send_threshold_algo_premiums():
        '''
        Send alert to slack with ticker info if algo premium more than threshold values
        '''
        import pandas as pd
        import json

        ################################## THRESHOLDS #################################################

        # Specify the file path with config thresholds
        threshold_prems_path = '/opt/airflow/dags/repo/configs/threshold_algo_premiums.json'

        # Load JSON data from the file
        with open(threshold_prems_path, 'r') as file:
            threshold_config = json.load(file)

        threshold_config_df = pd.json_normalize(threshold_config['tickers'])
        logging.info(f"Algo Prem Alert: Get threshold config, tickers: {threshold_config_df.shape[0]}")

        ################################## THRESHOLDS #################################################

        # Query
        algo_prems_query = f"""
            with prem as (
                select
                    toStartOfHour(timestamp) as date,
                    side,
                    left(ticker,7) as otcTicker,
                    right(ticker,1) as time,
                    host,
                    size,
                    quantile(0.9)(algo_prem) as perc_90,
                    count() as observs
                from mart_data_roxana.algo_premiums
                where toStartOfHour(timestamp) = toStartOfHour(now()) - toIntervalHour(1)
                group by 1,2,3,4,5,6
            )

            select * from prem
        """

        # Get values from CH
        df = int_client_ch_analytics.query_to_df(algo_prems_query)
        logging.info(f"Algo Prem Alert: Get algo prems for all tickers, rows: {df.shape[0]}")
        
        if not df.empty:
            # Merge thresholds and algo prems
            df = pd.merge(df, threshold_config_df, on='otcTicker', how='left')
        else:
            logging.info(f"Algo Prem Alert: No prem data")
            return True
        # Find OTC Tickers that havent thresholds and send alert
        missing_threshold_df = df[df['threshold_perc90'].isnull()].head(1)
        if not missing_threshold_df.empty:
            otcTicker = df['otcTicker'].iloc[0]
            logging.info(f"Algo Prem Alert: Found ticker w/o threshold: {otcTicker}")
            
            # Prepare the message
            message = f"| _*OTC Ticker*_: {otcTicker}"
            header = f"Alert | Missing ticker for threshold"
            # Send message
            slack_con.post_blocks_message(
                date=REPORT_DATE,
                header=header,
                text=message,
                push_text=header,
                context_mark='Monitoring Alert',
                reciever='@U06LQGHAKBN',
                channel_name=''
            )
            logging.info(f"Algo Prem Alert: ticker w/o threshold was sent to Slack")
        else:
            logging.info(f"Algo Prem Alert: All tickers have threshold")

        # Filter rows by enables pairs in threshold config and keep only row with threshold value
        df = df[df['enabled']]
        df = df[df['threshold_perc90'] < df['perc_90']].sort_values(by='observs', ascending=False).head(1)

        # If df is empty - we dont have threshold values
        if not df.empty:

            # Get parameters of threshold value
            otcTicker = df['otcTicker'].iloc[0]
            side = df['side'].iloc[0]
            time = df['time'].iloc[0]
            host = df['host'].iloc[0]
            size = df['size'].iloc[0]
            perc_90 = int(round(df['perc_90'].iloc[0],0))
            observs = df['observs'].iloc[0]
            logging.info(f"Algo Prem Alert: Found threshold algo prem, ticker: {otcTicker}{time}, value: {perc_90}")

            # Prepare the message
            message = f"| _*OTC Ticker*_: {otcTicker}{time} \n" \
                    f"| _*Side*_: {side} \n" \
                    f"| _*Host*_: {host} \n" \
                    f"| _*Size*_: {size} \n" \
                    f"| _*90% Value*_: :bangbang: {perc_90} bps :bangbang: \n" \
                    f"| _*#observations*_: {observs} for the last hour"
            
            header = f"Alert | High algo prem for ticker {otcTicker}"

            # Send message
            slack_con.post_blocks_message(
                date=REPORT_DATE,
                header=header,
                text=message,
                push_text=header,
                context_mark='Monitoring Alert',
                channel_name=''
            )
            logging.info(f"Algo Prem Alert: Threshold algo prem was sent to Slack")
        else:
            logging.info(f"Algo Prem Alert: No threshold values")
        return True


    def send_trade_monitoring_heartbeat():
        '''
        Send alert to slack with all proposal problems with trading (no deals, no arb ops, no prices)
        '''
        import pandas as pd
        import json

        ################################## THRESHOLDS #################################################

        # Specify the file path with config for monitoring trading
        monitoring_config_path = '/opt/airflow/dags/repo/configs/trade_monitoring_heartbeat.json'
        main_message = ''
        counter = 1

        # Load JSON data from the file
        with open(monitoring_config_path, 'r') as file:
            monitoring_heartbeat_config = json.load(file)

        monitoring_config_df = pd.json_normalize(monitoring_heartbeat_config['setups'])
        monitoring_config_df = monitoring_config_df[monitoring_config_df['enabled']]
        logging.info(f"Trade Heartbeat Alert: Get threshold config, setups: {monitoring_config_df['description'].unique().tolist()}")

        ################################## THRESHOLDS #################################################

        # Start to loop through all setups in config
        for index, row in monitoring_config_df.iterrows():
            found_thresholds = False
            # Get main info about setup
            host = row['host']
            desc = row['description']
            arb_ops_table = row['arb_ops_table']
            otc_prices_table = row['otc_prices_table']
            bundles_table = row['bundle_table']
            max_minute_otc_prices = row['prices_threshold']
            deals_threshold = row['deals_threshold']

            # Prepare monitoring pairs for setup
            pairs_config = pd.DataFrame(row['pairs'])
            pairs_config = pairs_config[pairs_config['enabled']]
            pairs_config['prices'] = max_minute_otc_prices
            enables_pairs = pairs_config['name'].unique().tolist()

            # Query for fetch last arb op
            arb_ops_query = f"""
            with arbs as (
                select
                    concat(symbol_base,symbol_quote) as name,
                    max(timestamp) as last_timestamp
                from {arb_ops_table}
                where 
                    is_canceled = False
                    and name in ({enables_pairs})
                    and host = '{host}'
                group by 1
            )

            select
                name,
                timestamp_diff('h',arbs.last_timestamp,now()) as threshold_diff
            from arbs
            """

            failed_bundles_query = f"""
                with bundles as (
                    select
                        hash,
                        max(is_success) as success
                    from {bundles_table}
                    where 
                        parseDateTimeBestEffort(timestamp) >= now() - toIntervalMinute(30)
                    group by 1
                )

                select
                    count(hash) as bundles,
                    countIf(hash, success = False) as failed_bundles,
                    failed_bundles / bundles * 100 as failed_share
                from bundles
            """

            # Query for fetch last deals
            deals_query = f"""
            with deals as (
                select
                    pair as name,
                    host,
                    max(timestamp) as last_timestamp
                from mart_data_roxana.roxana_deals
                where 
                    not tx_failed
                group by 1,2
            )

            select
                name,
                timestamp_diff('h',last_timestamp,now()) as threshold_diff
            from deals
            where
                host = '{host}'
            """

            # Fetch data from clickhouse
            df_arbs = int_client_ch_qb.query_to_df(arb_ops_query)
            df_deals = int_client_ch_analytics.query_to_df(deals_query)
            # if bundles_table:
            #     df_bundles = int_client_ch_qb.query_to_df(failed_bundles_query)
            # else:
            #     df_bundles = pd.DataFrame()


            logging.info(f"Trade Heartbeat Alert, host: {host}: fetch ch data")

            # Start to prepare host info
            host_info = f"*{counter}. {desc}* \n"

            ### 1. Check thresholds for arb ops
            df = pd.merge(pairs_config, df_arbs, on='name', how='left')
            df = df[(df['arb_ops'] <= df['threshold_diff']) | (df['threshold_diff'].isnull())]

            if not df.empty:   
                found_thresholds = True  
                threshold = df['threshold_diff'].min()
                pairs = df['name'].unique().tolist()

                if pd.notna(threshold):  # Check if last_time is not NaT
                    arbs_message = f"{int(threshold)} hours ago"
                else:
                    arbs_message = "more than 30 days ago"

                # Add info about arb ops
                arb_info = f"| _No arbs_: {', '.join(pairs)} - {arbs_message} \n"
                host_info += arb_info

                logging.info(f"Trade Heartbeat Alert, host: {host}: found arb op threshold")
            else:  
                logging.info(f"Trade Heartbeat Alert, host: {host}: no arb ops thresholds")

            ### 2. Check thresholds for deals
            df = pd.merge(pairs_config, df_deals, on='name', how='left')
            df = df[(df['deals'] <= df['threshold_diff']) | (df['threshold_diff'].isnull())]

            last_deal = df_deals['threshold_diff'].min()
            if last_deal >= deals_threshold:
                found_thresholds = True 
                last_deal_info = f"| _No deals (setup)_: The last is {int(last_deal)} hours ago \n"
                host_info += last_deal_info

                #push_pager_duty_call(f"No deals from {desc} setup. The last is {int(last_deal)} hours ago")

            if not df.empty:    
                found_thresholds = True 
                threshold = df['threshold_diff'].min()
                pairs = df['name'].unique().tolist()

                # Prepare the message
                deals_info = f"| _No deals (pairs)_: {', '.join(pairs)} - {int(threshold)} hours ago \n"
                host_info += deals_info

                logging.info(f"Trade Heartbeat Alert, host: {host}: found deals threshold")
            else:
                logging.info(f"Trade Heartbeat Alert, host: {host}: no deals thresholds")
            
            ### 3. Check failed bundles share
            # if not df_bundles.empty:
            #     failed_bundle_share = round(df_bundles['failed_share'].iloc[0],2)
            #     bundles = df_bundles['bundles'].iloc[0]
            #     if failed_bundle_share >= 99.9:
            #         print('hello')
            #         #push_pager_duty_call(f"High bundle failed share from {desc} setup. The value is {failed_bundle_share}")
            #     elif failed_bundle_share >= 95:
            #         found_thresholds = True
            #         bundles_info = f"| _Failed Bundle Share_: {failed_bundle_share}% , bundles: {bundles}\n"
            #         host_info += bundles_info

            #     logging.info(f"Trade Heartbeat Alert, host: {host}: found high failed bundle share")
            # else:
            #     logging.info(f"Trade Heartbeat Alert, host: {host}: no threshold failed bundle share, current {failed_bundle_share}")

            if found_thresholds:
                counter += 1
                main_message = main_message + host_info + '\n'

        if main_message:
            # Send message
            header = 'Alert | Trading Heartbeat'
            slack_con.post_blocks_message(
                date=REPORT_DATE,
                header=header,
                text=main_message,
                push_text=header,
                context_mark='Monitoring Alert',
                channel_name='roxana-analytics-alerts'
            )

        return True

    # Define task
    send_hedger_latency_alert = PythonOperator(
        task_id='send_hedger_latency_alert',
        python_callable=send_threshold_hedger_latency,
        dag=dag,
    )

    send_algo_premium_alert = PythonOperator(
        task_id='send_algo_premium_alert',
        python_callable=send_threshold_algo_premiums,
        dag=dag,
    )

    send_profit_loss_alert = PythonOperator(
        task_id='send_profit_loss_alert',
        python_callable=send_net_profit_loss,
        dag=dag,
    )

    send_trading_heartbeat_alert = PythonOperator(
        task_id='send_trading_heartbeat_alet',
        python_callable=send_trade_monitoring_heartbeat,
        dag=dag,
    )

    send_out_of_lifetime_alert = PythonOperator(
        task_id='send_out_of_lifetime_alert',
        python_callable=send_out_of_litetime_price,
        dag=dag,
    )

    # Define task relation
    send_hedger_latency_alert
    send_algo_premium_alert
    send_profit_loss_alert
    send_trading_heartbeat_alert
    send_out_of_lifetime_alert
    