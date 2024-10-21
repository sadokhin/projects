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

START_DATE = datetime(2024, 9, 9, 0, 0, 0)
REPORT_DATE = date.today()
DAG_NAME = 'slack_alert_3hours'
DAG_NUMBER = '1'
DESCRIPTION = 'Alert to Slack that shoud be executed every 3 hours'

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
        schedule_interval='30 0-23/3 * * *',
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

    def send_traded_conversion():
            '''
            Send alert to slack with small traded conversion
            '''
            import pandas as pd

            ################################## THRESHOLDS #################################################

            threshold_conversion = 2

            ################################## THRESHOLDS #################################################

            # Query
            traded_conversion_query = f"""
                with dex_volume as (
                    select
                        date_hour,
                        pair,
                        dex_size,
                        graph_mark,
                        max(dex_volume) as dex_vol
                    from mart_data_roxana.trading_volumes
                    where 
                        toStartOfHour(date_hour) between toStartOfHour(now()) - toIntervalHour(7) and toStartOfHour(now()) - toIntervalHour(1)
                        and blockchain = 'ETH'
                    group by 1,2,3,4
                ),

                main as (
                    select
                        (select sum(dex_vol) from dex_volume where dex_size = 'between') as dex_volume,
                        sum(traded_volume) as traded_volume,
                        sum(arb_volume) as arb_volume,
                        if(isNull(traded_volume / dex_volume * 100), 0.00, traded_volume / dex_volume) * 100 as traded_dex_conv,
                        if(isNull(traded_volume / arb_volume * 100), 0.00, traded_volume / arb_volume) * 100 as traded_arb_conv,
                        if(isNull(arb_volume / dex_volume * 100), 0.00, arb_volume / dex_volume) * 100 as arb_dex_conv
                    from mart_data_roxana.trading_volumes
                    where 
                        toStartOfHour(date_hour) between toStartOfHour(now()) - toIntervalHour(7) and toStartOfHour(now()) - toIntervalHour(1)
                        and blockchain = 'ETH'                
                )

                select * from main where traded_dex_conv < {threshold_conversion}
            """

            # Get values from CH
            df = int_client_ch_analytics.query_to_df(traded_conversion_query)
            logging.info(f"Traded Conversion alert: Get conversion, rows: {df.shape[0]}")
            
            if not df.empty: 
                dex_volume = df['dex_volume'].iloc[0]
                arb_volume = df['arb_volume'].iloc[0]
                traded_volume = df['traded_volume'].iloc[0]
                conversion = round(df['traded_dex_conv'].iloc[0],2)
                traded_arb_conv = round(df['traded_arb_conv'].iloc[0],2)
                arb_dex_conv = round(df['arb_dex_conv'].iloc[0],2)

                dex_vol = str(round(dex_volume / 1_000_000, 2)) + ' mln' if dex_volume < 1_000_000_000 else str(round(dex_volume / 1_000_000_000, 2)) + ' bln'
                
                # Prepare the message
                message = f"*Traded / DEX:*: :bangbang: *{conversion}* (%) :bangbang: \n\n" \
                    f"_*for the last 6 hours_ \n" \
                    f"    | _*DEX Volume:*_: ${dex_vol} _(100%)_ \n" \
                    f"    | _*Arb Volume:*_: ${round(arb_volume / 1_000_000, 2)} mln _({arb_dex_conv}%)_ \n" \
                    f"    | _*Traded Volume:*_: ${round(traded_volume / 1_000_000, 2)} mln _({traded_arb_conv}%)_ \n" 

                header = f"Alert | Poor Conversion Traded / DEX"
                # Send message
                message_info = slack_con.post_blocks_message(
                    date=REPORT_DATE,
                    header=header,
                    text=message,
                    push_text=header,
                    context_mark='Monitoring Alert',
                    channel_name=''
                )
                logging.info(f"Traded Conversion alert: threshold was sent to Slack")

                thread_ts = message_info.get('ts')

                # Fetch data for trading funnel
                trading_funnel_query = f"""
                    with all_arb_ops as (
                        select
                            x,
                            hash,
                            block_number as block_number,
                            pair,
                            is_dex_buying,
                            if(is_dex_buying = 0, 'S', 'B') as dex_side,
                            source_dex,
                            pair_type,
                            setup,
                            arb_ops_flag,
                            size_usd,
                            base_fee,
                            bribe_usd,
                            profit_usd,
                            rival_hash,
                            price_cex,
                            rival_dex_price,
                            treasury_mark,
                        from mart_data_roxana.arb_ops_bug_report
                        where 
                            toStartOfHour(timestamp) between toStartOfHour(now()) - toIntervalHour(7) and toStartOfHour(now()) - toIntervalHour(1)
                            and chain = 'ETH'
                            and arb_strategy != 'graph'
                    ),

                    dex_volume as (
                        select
                            tx_hash,
                            pair,
                            lower(pool_id) as pool_id,
                            block_number,
                            lower(replace(replace(internal_dex_name,'_',''),'ARB','')) as internal_dex_name,
                            dex_side,
                            amountUSD,
                            base_fee,
                            bribe_usd,
                            flag,
                            row_number() over (partition by block_number, pool_id, dex_side, pair order by amountUSD desc) as rival_rn
                        from mart_data_roxana.roxana_rivals
                        where 
                            toStartOfHour(timestamp) between toStartOfHour(now()) - toIntervalHour(7) and toStartOfHour(now()) - toIntervalHour(1)
                            and chain = 'ETH'
                            and our_pools
                    ),

                    dex_ops as (
                        select
                            dex_volume.tx_hash as tx_hash,
                            dex_volume.pair as pair,
                            dex_volume.pool_id as pool_id,
                            dex_volume.block_number as block_number,
                            dex_volume.dex_side as dex_side,
                            dex_volume.amountUSD as amountUSD,
                            dex_volume.flag as rival_flag,
                            dex_volume.base_fee + dex_volume.bribe_usd as rival_fees,
                            all_arb_ops.x as x,
                            all_arb_ops.arb_ops_flag as arb_ops_flag,
                            all_arb_ops.size_usd as arb_ops_size_usd,
                            all_arb_ops.base_fee + all_arb_ops.bribe_usd as our_fees,
                            all_arb_ops.treasury_mark as treasury_mark,
                            if(rival_flag in ('less size', 'more size', 'trading disabled'), False, True) as flag_our_config,
                            if(pair like '%DAI%' or pair like '%PEPE%', False, True) as flag_cex_price_available,
                            if(rival_flag = 'not_fb_block' or arb_ops_flag in ('not_fb_block','private_block'), False, True) as flag_fb_block,
                            multiIf(
                                arb_ops_flag = 'our_deal', 'our_deal',
                                treasury_mark = 'treasury_failed', 'treasury_failed',
                                arb_ops_flag in ('not_our_market','bad_cex_price'), 'not_our_market',
                                arb_ops_flag = 'late_in_block', 'late_in_block',
                                arb_ops_flag in ('overpaid_base_fee','bad_dex_price'), concat('lower_bribe_', arb_ops_flag),
                                rival_fees > our_fees and x!= '', 'lower_bribe',
                                arb_ops_flag in ('not_txs_data','on_research','no rival','more size'), 'on_research',
                                arb_ops_flag
                            ) as super_flag
                        from dex_volume
                        left join all_arb_ops 
                        on dex_volume.block_number = all_arb_ops.block_number + 1
                            and dex_volume.dex_side = all_arb_ops.dex_side
                            and dex_volume.pair = all_arb_ops.pair
                            and dex_volume.internal_dex_name = all_arb_ops.source_dex
                        where rival_rn = 1
                    ),

                    volumes as (
                        select 
                            countIf(tx_hash,flag_our_config and flag_cex_price_available and flag_fb_block) as dex_fb_blocks_txs,
                            sumIf(amountUSD,flag_our_config and flag_cex_price_available and flag_fb_block) as dex_fb_blocks_vol,
                            countIf(tx_hash,flag_our_config and flag_cex_price_available and flag_fb_block and isNull(super_flag)) as arb_unprofit_txs,
                            sumIf(amountUSD,flag_our_config and flag_cex_price_available and flag_fb_block and isNull(super_flag)) as arb_unprofit_vol
                        from dex_ops 
                    ),

                    metrics as (
                        select 
                            arrayJoin([
                                ('dex_fb_blocks', dex_fb_blocks_txs, dex_fb_blocks_vol, 100),
                                ('unprofitable_arb_ops', arb_unprofit_txs, arb_unprofit_vol, cast(round(arb_unprofit_vol / dex_fb_blocks_vol * 100, 0) as UInt8))
                            ]) as metrics_array
                        from volumes
                    ),

                    final as (
                        select
                            metrics_array.1 as flag,
                            metrics_array.2 as txs,
                            metrics_array.3 as volume,
                            metrics_array.4 as share
                        from metrics
                        
                        union all 
                        
                        select
                            super_flag,
                            count(tx_hash) as txs,
                            sum(amountUSD) as volume,
                            cast(round(volume / (select dex_fb_blocks_vol from volumes) * 100, 0) as UInt8) as share
                        from dex_ops
                        where
                            isNotNull(arb_ops_flag)
                            and (flag_our_config and flag_cex_price_available and flag_fb_block or super_flag='our_deal')
                        group by 1
                    )

                    select * from final 
                    order by volume desc
                """

                # Get values from CH
                arb_bugs = int_client_ch_analytics.query_to_df(trading_funnel_query)

                if not arb_bugs.empty:
                    bugs_dict = {
                        'on_research' : 'On research',
                        'late_in_block': 'Late in block',
                        'sandwich': 'Sandwich',
                        'not_txs_data': 'Not txs data for getting tx info',
                        'more_than_1swap': 'More than 1 swap in tx',
                        'private_block': 'Private block',
                        'start_of_the_block': 'Lower bribe, Sent at the beginning of the block',
                        'minProfit_limit': 'Rival profit less then our minimum',
                        'bribeShare_limit': 'Rival sent bribe more than our maximum',
                        'dex': 'DEX',
                        'dex_our_sizes': 'DEX (our sizes)',
                        'arb_ops_treasury_available': 'Arb ops (treasury available)',
                        'arb_ops': 'Arb ops',
                        'mined': 'Mined',
                        'our_deal': 'Mined',
                        'not_our_market': 'Uncompetitive price',
                        'dex_fb_blocks_main': 'DEX (fb blocks)',
                        'dex_fb_blocks': 'DEX (fb blocks)',
                        'unprofitable_arb_ops': 'Unprofitable arb ops',
                        'treasury_failed': 'Treasury Failed',
                        'lower_bribe_bad_dex_price': 'Lower bribe, bad DEX price',
                        'lower_bribe_overpaid_base_fee': 'Lower bribe, not optimal contract',
                        'not_optimal_bribe': 'Not optimal bribe',
                        'lower_bribe': 'Lower bribe',
                        'dex_cex_prices': 'DEX (CEX prices)',
                        'arb_ops_competitive_prices': 'Arb ops (competitive price)'
                    }
                    # Sum all 'volume' and 'count' values
                    flags = arb_bugs['flag'].tolist()
                    direct_rivals_message = []
                    for flag in flags:

                        flag_name = bugs_dict.get(flag)

                        txs = arb_bugs[arb_bugs['flag']==flag]['txs'].iloc[0]
                        txs = "{:,.0f}".format(txs)

                        volume = arb_bugs[arb_bugs['flag']==flag]['volume'].iloc[0]
                        volume = str(round(volume / 1_000_000,2)) + ' mln' if volume < 1_000_000_000 else str(round(volume / 1_000_000_000, 2)) + ' bln'

                        share = arb_bugs[arb_bugs['flag']==flag]['share'].iloc[0]
                        message = f"*{share}%* | _*{flag_name}*_: {txs} ops _(${volume})_"
                        
                        direct_rivals_message.append(message)

                    # Prepare the message with report
                    delimeter = f"\n ------------------------------ \n" \
                                f"DEX (fb blocks) (Detailed view): \n\n"
                    direct_rivals = "\n".join(direct_rivals_message)

                    message = delimeter + direct_rivals
                    # If for function header is not provided. Save it as default
                    header = f'Trading Funnel | w/o PROD3/LND'
        
                    slack_con.post_blocks_message(
                        date=REPORT_DATE,
                        header=header,
                        text=message,
                        push_text=header,
                        context_mark='Monitoring Alert',
                        channel_name='',
                        thread_ts=thread_ts
                    )

                    logging.info(f"Traded Conversion alert: trading funnel was sent to Slack")
                else:
                    logging.info(f"Traded Conversion alert: trading funnel is empty")

            else:
                logging.info(f"Traded Conversion alert: No threshlod")
            
            return True

    # Define task
    send_traded_conversion_alert = PythonOperator(
        task_id='send_traded_conversion_alert',
        python_callable=send_traded_conversion,
        dag=dag,
    )

    # Define task relation
    send_traded_conversion_alert
    