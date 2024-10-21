from datetime import datetime, date, timedelta
import logging

from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from custom_modules import internal_clickhouse
from custom_modules import internal_slack
import pandas as pd
from custom_modules.internal_data_processing import task_fail_slack_alert

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

ch_prod_hook = BaseHook.get_connection('clickhouse_prod')
int_client_ch_prod = internal_clickhouse.Clickhouse(
    host=ch_prod_hook.host, 
    port=ch_prod_hook.port, 
    username=ch_prod_hook.login, 
    password=ch_prod_hook.password
)

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

START_DATE = datetime(2024, 4, 19, 0, 0, 0)
REPORT_DATE = date.today() - timedelta(days=1)
DAG_NAME = 'slack_report_roxana_daily'
DAG_NUMBER = '1'
DESCRIPTION = 'Send All Daily Report for Roxana to Slack for the previous day'
SLACK_CHANNEL = ''

################################## Parametrs. Don't edit it! ##################################
with models.DAG(
        dag_id=f'{DAG_NAME}_dag_{DAG_NUMBER}',
        default_args=default_args,
        catchup=False,
        start_date=START_DATE,
        description=DESCRIPTION,
        schedule_interval='30 2 * * *',
        tags = ['slack','report','api', 'sql'],
) as dag:
################## FINANCIAL GENERAL REPORT ##################

    def fetch_trading_volumes_data(chain, graph_mark):
        '''
        Get data from roxana financial mart data with breakdown by instances for provided chain
        '''

        # Query
        trading_query = f"""
            with dex_volume as (
                select
                    date_hour,
                    pair,
                    dex_size,
                    graph_mark,
                    max(dex_volume) as dex_vol
                from mart_data_roxana.trading_volumes
                where 
                    toDate(date_hour) = '{REPORT_DATE}'
                    and blockchain = '{chain}'
                    and (False = {graph_mark} or graph_mark = {graph_mark})
                group by 1,2,3,4
            )

            select
                multiIf(
                    instance = 'prod-qb-rox-eth-chapie-arb-graph', 'london-graph',
                    instance = 'prod-qb-rox-eth-tokyo-chappie-01', 'tokyo', 
                    instance = 'prod-qb-rox-eth-chappie-01', 'london',
                    instance = 'prod-qb-rox-eth-chappie-02', 'gigachappie',
                    instance
                ) as instance,
                (select sum(dex_vol) from dex_volume where dex_size = 'between') as dex_volume,
                sum(arb_volume) as arb_volume,
                if(dex_volume = 0, 0.00, arb_volume / dex_volume * 100) as arb_cr,
                sum(tradable_volume) as tradable_volume,
                if(dex_volume = 0, 0.00, tradable_volume / dex_volume * 100) as tradable_cr,
                sum(traded_volume) as traded_volume,
                if(dex_volume = 0, 0.00, traded_volume / dex_volume * 100) as traded_cr,
                sum(ops_count) as ops_count,
                sum(deals) as deals,
                if(ops_count = 0, 0.00, deals / ops_count * 100) as deals_cr,
                sum(gross_profit) as gross_profit,
                sum(gross_profit_with_slippage) as gross_profit_with_slippage,
                sum(gross_profit_with_hedge_prices) as gross_profit_with_hedge_prices,
                sum(base_fee) as base_fee,
                sum(bribe) as bribe,
                gross_profit - base_fee - bribe as net,
                gross_profit_with_slippage - base_fee - bribe as net_with_slippage,
                gross_profit_with_hedge_prices - base_fee - bribe as net_with_hedge_prices,
                sum(base_fee_failed) as base_fee_failed,
                sum(txs) as txs,
                if(ops_count = 0, 0.00, txs / ops_count * 100) as txs_cr,
                gross_profit - base_fee_failed - base_fee as net_failed,
                gross_profit_with_slippage - base_fee_failed - base_fee as net_failed_with_slippage,
                gross_profit_with_hedge_prices - base_fee_failed - base_fee as net_failed_with_hedge_prices
            from mart_data_roxana.trading_volumes
            where 
                toDate(date_hour) = '{REPORT_DATE}'
                and instance != ''
                and blockchain = '{chain}'
                and (False = {graph_mark} or graph_mark = {graph_mark})
            group by 1
            """
        
        # Read in dataframe and group by instances / Skip if there is no data for instance+chain
        trading_df = int_client_ch_analytics.query_to_df(trading_query)
        if not trading_df.empty:
            totals_by_instance = trading_df.groupby('instance').sum().reset_index()
            
            return totals_by_instance
        else:
            logging.info(f"{chain}: No Trading Data")
            return trading_df

    def send_general_report(trading_volumes, chain = '', graph = '', instance = '', thread_ts = 0.0):
        '''
        Get volumes from group (instance+chain) and sent report to slack
        '''

        # Get volumes from group
        dex_volume = trading_volumes['dex_volume'].iloc[0]
        arb_volume = trading_volumes['arb_volume'].iloc[0]
        arb_cr = round(trading_volumes['arb_cr'].iloc[0],2)
        tradable_volume = trading_volumes['tradable_volume'].iloc[0]
        tradable_cr = round(trading_volumes['tradable_cr'].iloc[0],2)
        traded_volume = trading_volumes['traded_volume'].iloc[0]
        traded_cr = round(trading_volumes['traded_cr'].iloc[0],2)
        
        ops_count = trading_volumes['ops_count'].iloc[0]
        deals = trading_volumes['deals'].iloc[0]
        deals_cr = int(round(deals / ops_count * 100,0)) if ops_count != 0 else 0
        gross_profit = round(trading_volumes['gross_profit'].iloc[0],2)
        gross_profit_with_slippage = round(trading_volumes['gross_profit_with_slippage'].iloc[0],2)
        gross_profit_with_hedge_prices = round(trading_volumes['gross_profit_with_hedge_prices'].iloc[0],2)
        bribe = round(trading_volumes['bribe'].iloc[0],2)
        base_fee = round(trading_volumes['base_fee'].iloc[0],2)
        net = gross_profit - base_fee - bribe
        net_bps = round(net / traded_volume * 10000,1) if traded_volume != 0 else 0
        net_with_slippage = gross_profit_with_slippage - base_fee - bribe
        net_with_slippage_bps = round(net_with_slippage / traded_volume * 10000,1) if traded_volume != 0 else 0
        net_with_hedge_prices = gross_profit_with_hedge_prices - base_fee - bribe
        net_with_hedge_prices_bps = round(net_with_hedge_prices / traded_volume * 10000,1) if traded_volume != 0 else 0

        base_fee_failed = round(trading_volumes['base_fee_failed'].iloc[0],2)
        txs = trading_volumes['txs'].iloc[0]
        txs_cr = int(round(txs / ops_count * 100,0)) if ops_count != 0 else 0
        net_failed = gross_profit - base_fee_failed - base_fee
        net_failed_bps = round(net_failed / traded_volume * 10000,1) if traded_volume != 0 else 0
        net_failed_with_slippage = gross_profit_with_slippage - base_fee_failed - base_fee
        net_failed_with_slippage_bps = round(net_failed_with_slippage / traded_volume * 10000,1) if traded_volume != 0 else 0
        net_failed_with_hedge_prices = gross_profit_with_hedge_prices - base_fee_failed - base_fee
        net_failed_with_hedge_prices_bps = round(net_failed_with_hedge_prices / traded_volume * 10000,1) if traded_volume != 0 else 0

        # Formating integer numbers to readable format
        gross_profit = "{:,.2f}".format(gross_profit)
        gross_profit_with_slippage = "{:,.2f}".format(gross_profit_with_slippage)
        gross_profit_with_hedge_prices = "{:,.2f}".format(gross_profit_with_hedge_prices)
        ops_count = "{:,.0f}".format(ops_count)
        net_failed = "{:,.2f}".format(net_failed)
        net_failed_with_slippage = "{:,.2f}".format(net_failed_with_slippage)
        net_failed_with_hedge_prices = "{:,.2f}".format(net_failed_with_hedge_prices)
        net = "{:,.2f}".format(net)
        net_with_slippage = "{:,.2f}".format(net_with_slippage)
        net_with_hedge_prices = "{:,.2f}".format(net_with_hedge_prices)
        base_and_failed_fee = "{:,.2f}".format(base_fee + base_fee_failed)
        base_and_bribe_fee = "{:,.2f}".format(base_fee + bribe)
        base_fee = "{:,.2f}".format(base_fee)
        base_fee_failed = "{:,.2f}".format(base_fee_failed)
        bribe = "{:,.2f}".format(bribe)
        

        # For BSC and Arbitrum we dont have bribe, and instead of it we provide info about fees for failed tx
        if chain in ('BSC','Arbitrum'):
            fees = f"${base_and_failed_fee} _(${base_fee} *Successful* + ${base_fee_failed} *Failed*)_"
            net_report = f"${net_failed} _({net_failed_bps} bps)_, _*With Slippage*_: ${net_failed_with_slippage} _({net_failed_with_slippage_bps} bps)_, _*Fact*_: ${net_failed_with_hedge_prices} _({net_failed_with_hedge_prices_bps} bps)_"
        else: 
            fees = f"${base_and_bribe_fee} _(${bribe} *Bribe* + ${base_fee} *Base*)_"
            net_report = f"${net} _({net_bps} bps)_, _*With Slippage*_: ${net_with_slippage} _({net_with_slippage_bps} bps)_, _*Fact*_: ${net_with_hedge_prices} _({net_with_hedge_prices_bps} bps)_"

        # Round dex volume to millions or billions
        dex_vol = str(round(dex_volume / 1_000_000, 2)) + ' mln' if dex_volume < 1_000_000_000 else str(round(dex_volume / 1_000_000_000, 2)) + ' bln'

        # Prepare the message
        message = f"| _*DEX Volume*_: {dex_vol} _(100%)_ \n" \
                f"| _*Arb Volume*_: {round(arb_volume / 1_000_000, 2)} mln _({arb_cr}%)_ \n" \
                f"| _*Tradable Volume*_: {round(tradable_volume / 1_000_000, 2)} mln _({tradable_cr}%)_ \n" \
                f"| _*Traded Volume*_: {round(traded_volume / 1_000_000, 2)} mln _({traded_cr}%)_ \n\n" \
                f"| _*Ops Found*_: {ops_count} _(100%)_ \n" \
                f"| _*Txs Sent*_: {txs} _({txs_cr}%)_ \n" \
                f"| _*Successful Deals*_: {deals} _({deals_cr}%)_ \n" \
                f"| _*Gross*_: ${gross_profit} , _*With Slippage*_: ${gross_profit_with_slippage}, _*Fact*_: ${gross_profit_with_hedge_prices} \n" \
                f"| _*Fees*_: {fees} \n" \
                f"| _*Net*_: {net_report}"
        
        # Prepare header for message based on message with all chain data or with instance data
        header = f'General Report | Chain: {chain}'

        if graph:
            header = 'Graph Report'
        if instance:
            header = header + f'| Instance: {instance}'

        # Send message
        result = slack_con.post_blocks_message(
            date=REPORT_DATE,
            header=header,
            text=message,
            push_text=header,
            channel_name=SLACK_CHANNEL,
            thread_ts=thread_ts
        )

        return result  

    def send_trading_volumes_reports():
        '''
        Main function for fetching data and sending general reports
        '''
        import pandas as pd
        # Select chains for reporting (BSC, ETH, Arbitrum)
        CHAINS = [
            #{'chain':'BSC', 'graph': False},
            {'chain':'ETH', 'graph': False},
            {'chain':'Arbitrum', 'graph': False},
            {'chain':'ETH', 'graph': True}
        ]

        for chain_config in CHAINS:
            chain = chain_config['chain']
            graph_mark = chain_config['graph']
            # Get data about trading volumes for chain with group by instances
            volumes_by_instance = fetch_trading_volumes_data(chain=chain,graph_mark=graph_mark)
            if not volumes_by_instance.empty:
                logging.info(f"General Report, {chain}, graph: {graph_mark}: Get volumes from CH. Instances: {volumes_by_instance['instance'].unique()}")

                #If chain has 1 instance - send 1 general report for chain else 1 general and for all instance
                if volumes_by_instance['instance'].nunique() == 1:
                    send_general_report(volumes_by_instance, chain=chain, graph=graph_mark)
                    logging.info(f"General Report, {chain}, graph: {graph_mark}: Sent General Chain Report")

                elif volumes_by_instance['instance'].nunique() > 1:
                    # Prepare aggregation function as of dex_volume we should be get max value, all another - sum
                    agg_functions = {'dex_volume': 'max'}
                    agg_functions.update({metric: 'sum' for metric in volumes_by_instance.columns if metric != 'dex_volume'})

                    # Aggregate volumes and send general report
                    volumes_by_chain = pd.DataFrame(volumes_by_instance.agg(agg_functions)).T
                    message_info = send_general_report(volumes_by_chain, chain=chain, graph=graph_mark)
                    logging.info(f"General Report, {chain}, graph: {graph_mark}: Sent General Chain Report")
                    
                    # Get timestamp of parent message to send reports by instances to thread
                    message_ts = message_info.get('ts')
                    # Send General Report by instances
                    for instance, instance_volumes in volumes_by_instance.groupby('instance'):
                        send_general_report(instance_volumes, chain=chain,graph=graph_mark, instance=instance, thread_ts=message_ts) 
                        logging.info(f"General Report, {chain}.{instance}, graph: {graph_mark}: Sent General Instance Report")

        return True     
################## !FINANCIAL GENERAL REPORT ##################

################## CHAPPIE BUGS REPORT ##################

    def send_arb_bugs_report(arb_bugs, chain = ''):
        '''
        Get all reasons of lost arb ops and their volumes for chain and sent report to slack
        '''

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
        common_stat_message = []
        direct_rivals_message = []
        for flag in flags:

            flag_name = bugs_dict.get(flag)

            txs = arb_bugs[arb_bugs['flag']==flag]['txs'].iloc[0]
            txs = "{:,.0f}".format(txs)

            volume = arb_bugs[arb_bugs['flag']==flag]['volume'].iloc[0]
            volume = str(round(volume / 1_000_000,2)) + ' mln' if volume < 1_000_000_000 else str(round(volume / 1_000_000_000, 2)) + ' bln'

            share = arb_bugs[arb_bugs['flag']==flag]['share'].iloc[0]
            message = f"*{share}%* | _*{flag_name}*_: {txs} ops _(${volume})_"
            
            if flag in ['dex','dex_our_sizes','dex_cex_prices','dex_fb_blocks_main','arb_ops_treasury_available','arb_ops','arb_ops_competitive_prices','mined']:
                common_stat_message.append(message)
            else:
                direct_rivals_message.append(message)

        # Prepare the message with report
        common_stats = "\n".join(common_stat_message)
        delimeter = f"\n ------------------------------ \n" \
                    f"DEX (fb blocks) (Detailed view): \n\n"
        direct_rivals = "\n".join(direct_rivals_message)

        message = common_stats + delimeter + direct_rivals
        # If for function header is not provided. Save it as default
        header = f'Funnel | Direct Arbitrage | Chain: {chain} | w/o gigachappie'

        # Send message
        result = slack_con.post_blocks_message(
            date=REPORT_DATE,
            header=header,
            text=message,
            push_text=header,
            channel_name=SLACK_CHANNEL
        )

        return result

    def send_bug_report():
        '''
        Main function for fetching data about arb ops and sending bug report
        '''
        import pandas as pd
        # Select chains for reporting (BSC, ETH, Arbitrum)
        CHAINS = ['ETH']

        for chain in CHAINS:

            common_stat_arb_query = f"""
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
                        treasury_mark
                    from mart_data_roxana.arb_ops_bug_report
                    where 
                        toDate(timestamp) = '{REPORT_DATE}'
                        and chain = '{chain}'
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
                        toDate(timestamp) = '{REPORT_DATE}'
                        and chain = '{chain}'
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
                            rival_fees > our_fees and x!= '' or arb_ops_flag in ('overpaid_base_fee','bad_dex_price','sandwich','more_than_1swap'), 'lower_bribe',
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
                        count(tx_hash) as dex_txs,
                        sum(amountUSD) as dex_vol,
                        countIf(tx_hash,flag_our_config) as dex_our_config_txs,
                        sumIf(amountUSD,flag_our_config) as dex_our_config_vol,
                        countIf(tx_hash,flag_our_config and flag_cex_price_available) as dex_price_available_txs,
                        sumIf(amountUSD,flag_our_config and flag_cex_price_available) as dex_price_available_vol,
                        countIf(tx_hash,flag_our_config and flag_cex_price_available and flag_fb_block) as dex_fb_blocks_txs,
                        sumIf(amountUSD,flag_our_config and flag_cex_price_available and flag_fb_block) as dex_fb_blocks_vol,
                        countIf(tx_hash,flag_our_config and flag_cex_price_available and flag_fb_block and (super_flag != 'treasury_failed')) as dex_available_treasury_txs,
                        sumIf(amountUSD,flag_our_config and flag_cex_price_available and flag_fb_block and (super_flag != 'treasury_failed')) as dex_available_treasury_vol,
                        countIf(tx_hash,flag_our_config and flag_cex_price_available and flag_fb_block and isNotNull(arb_ops_flag)) as dex_arb_ops_txs,
                        sumIf(amountUSD,flag_our_config and flag_cex_price_available and flag_fb_block and isNotNull(arb_ops_flag)) as dex_arb_ops_vol,
                        countIf(tx_hash,flag_our_config and flag_cex_price_available and flag_fb_block and super_flag not in ('treasury_failed','not_our_market')) as dex_competitive_arb_ops_txs,
                        sumIf(amountUSD,flag_our_config and flag_cex_price_available and flag_fb_block and super_flag not in ('treasury_failed','not_our_market')) as dex_competitive_arb_ops_vol,
                        countIf(tx_hash,arb_ops_flag='our_deal') as dex_mined_txs,
                        sumIf(amountUSD,arb_ops_flag='our_deal') as dex_mined_vol
                    from dex_ops 
                ),

                metrics as (
                    select 
                        arrayJoin([
                            ('dex', dex_txs, dex_vol, 100),
                            ('dex_our_sizes', dex_our_config_txs, dex_our_config_vol, cast(round(dex_our_config_vol / dex_vol * 100, 0) as UInt8)),
                            ('dex_cex_prices', dex_price_available_txs, dex_price_available_vol, cast(round(dex_price_available_vol / dex_vol * 100, 0) as UInt8)),
                            ('dex_fb_blocks_main', dex_fb_blocks_txs, dex_fb_blocks_vol, cast(round(dex_fb_blocks_vol / dex_vol * 100, 0) as UInt8)),
                            ('arb_ops', dex_arb_ops_txs, dex_arb_ops_vol, cast(round(dex_arb_ops_vol / dex_vol * 100, 0) as UInt8)),
                            ('arb_ops_treasury_available', dex_available_treasury_txs, dex_available_treasury_vol, cast(round(dex_available_treasury_vol / dex_vol * 100, 0) as UInt8)),
                            ('arb_ops_competitive_prices', dex_competitive_arb_ops_txs, dex_competitive_arb_ops_vol, cast(round(dex_competitive_arb_ops_vol / dex_vol * 100, 0) as UInt8)),
                            ('mined', dex_mined_txs, dex_mined_vol, cast(round(dex_mined_vol / dex_vol * 100, 0) as UInt8))
                        ]) as metrics_array
                    from volumes
                )

                select
                    metrics_array.1 as flag,
                    metrics_array.2 as txs,
                    metrics_array.3 as volume,
                    metrics_array.4 as share
                from metrics

            """
            # Get data about trading volumes for chain with group by instances
            common_stat_arb_df = int_client_ch_analytics.query_to_df(common_stat_arb_query)
            # arb_bugs = fetch_arb_bugs_data(chain=chain)
            logging.info(f"Bug Report, {chain}: Get common stat for arb ops from CH.")

            direct_rivals_query = f"""
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
                        toDate(timestamp) = '{REPORT_DATE}'
                        and chain = '{chain}'
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
                        toDate(timestamp) = '{REPORT_DATE}'
                        and chain = '{chain}'
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
            direct_rivals_df = int_client_ch_analytics.query_to_df(direct_rivals_query)
            df = pd.concat([common_stat_arb_df,direct_rivals_df])

            logging.info(f"Bug Report, {chain}: Get direct rivals stats from CH.")
            send_arb_bugs_report(df,chain=chain)

        return True
    
################## !CHAPPIE BUGS REPORT ##################

################## MARKET HEDGE REPORT ##################

    def fetch_market_hedge_data():
        '''
        Get data about deal's hedge for yesterday
        '''

        # Query
        mrk_hedge_chprod_query = f"""
            with deals as (
                select 
                    user_deal_id,
                    if(user_deal_id like '%back%', 'REORG',type) as custom_type, 
                    row_number() over (partition by user_comment order by ts desc) as rn
                from roxana.otc_data
                where 
                    toDate(ts) = '{REPORT_DATE}'
                    and (user_deal_id like '%-r-3%' OR user_deal_id like '%-r1back%' or user_deal_id like '%-r-1%')
            )

            select
                custom_type,
                count(distinct user_deal_id) as cnt
            from deals
            where 
                rn = 1
            group by 1
            """
        
        # Read data to df and get number of MRK, RFQ hedge and REORGs
        df = int_client_ch_prod.query_to_df(mrk_hedge_chprod_query)

        total_cnt = df['cnt'].sum()
        rfk_cnt = df[df['custom_type'] == 'SERVICE']['cnt'].sum()
        mrk_cnt = df[df['custom_type'] == 'MRK']['cnt'].sum()
        reorg_cnt = df[df['custom_type'] == 'REORG']['cnt'].sum()

        # Fill 0 if some values are empty
        if pd.isna(rfk_cnt):
            rfk_cnt = 0
        if pd.isna(mrk_cnt):
            mrk_cnt = 0
        if pd.isna(reorg_cnt):
            reorg_cnt = 0

        return total_cnt,rfk_cnt,mrk_cnt,reorg_cnt

    def send_mrk_hedge_report():

        # Get values from CH
        deals, deals_rfk, deals_mrk, deals_reorg = fetch_market_hedge_data()
        logging.info(f"Get hedge data from CH.")

        # As of REORGS deals basically was in RFQ we remove them and display separately
        deals_rfk = deals_rfk - deals_reorg
        deals = deals - deals_reorg

        deals_rfk_pers = int(round(deals_rfk / deals * 100,0))
        deals_mrk_pers = int(round((deals_mrk + deals_reorg) / deals * 100,0))

        # Prepare the message
        message = f"| _*Deals Total*_: {deals}\n" \
                f"| _*RFQ*_: {deals_rfk_pers}%  _({deals_rfk})_ \n" \
                f"| _*MRK*_: {deals_mrk_pers}% _({deals_mrk + deals_reorg})_ " \
        
        # Add REORGS information if exists
        if deals_reorg:
            message = message + f"\n -------- \n - _*REORG*_: _{deals_reorg}_ "
        logging.info(f"Message prepared.")

        # Send message to Slack
        slack_con.post_blocks_message(
            date=REPORT_DATE,
            header='Mrk hedge stats',
            text=message,
            push_text='Mrk hedge stats report',
            channel_name=SLACK_CHANNEL
        )
        logging.info(f"Sent message to Slack.")
        
        return True
    
################## !MARKET HEDGE REPORT ##################

################## GAINERS REPORT ##################

    def send_gainers_report():
        '''
        Main function for fetching TOP Gainers and send report
        '''

        # Query
        gainers_query = f'''
            with dex as (
                select 
                    toDate(parseDateTime64BestEffort(timestamp)) as date, 
                    concat(token0, token1) as pair_first_var, 
                    count(distinct tx_hash) as quant,
                    sum(toFloat64(amountUSD)) as amount_usd
                from raw_external.dex_data_realtime
                where 
                    toDate(parseDateTime64BestEffort(timestamp)) between toDate(now()) - 30 and toDate(now()) - 1 
                    and toFloat64(amountUSD) between 5 and 10000000
                group by date, pair_first_var
            ),

            gainers as (
                select 
                    pair_first_var, 
                    sumIf(quant,date = toDate(now()) - 1) as quant_yesterday,
                    avg(quant) as avg_quant_last30_days,
                    sumIf(amount_usd, date = toDate(now()) - 1) as amount_yesterday,
                    avg(amount_usd) as avg_amount_last30_days
                from dex
                group by pair_first_var
            )

            select
                pair_first_var,
                quant_yesterday,
                amount_yesterday
            from gainers
            where
                quant_yesterday >= avg_quant_last30_days * 1.5
                and quant_yesterday > 500
                and amount_yesterday > 500_000
            order by quant_yesterday desc
            limit 10
        '''

        top_gainers = int_client_ch_analytics.query_to_df(gainers_query)
        logging.info(f"Gainers Report: Fetch Gainers, rows: {top_gainers.shape[0]}")

        if not top_gainers.empty:
            message_rows = []
            for index, row in top_gainers.iterrows():
            # Extract information from the row
                pair = row['pair_first_var']
                quant = row['quant_yesterday']
                amount = round(row['amount_yesterday'] / 1_000_000, 2)
                message = f"{index+1}. {pair}: {quant} txs, {amount}$ mln"
            
                message_rows.append(message)
            logging.info(f"Gainers Report: Report prepared")

            # Prepare message
            message = "\n".join(message_rows)
            header = 'Top Gainers'

            # Send report to slack
            slack_con.post_blocks_message(
                date=REPORT_DATE,
                header=header,
                text=message,
                push_text=header,
                channel_name=SLACK_CHANNEL
            )
            logging.info(f"Gainers Report: Report sent")
        else:
            logging.info(f"Gainers Report: No Gainers Data")

################## !GAINERS REPORT ##################

################## GIGABUILDER REPORT ##################

    def send_gigabuilder_report():
        '''
        Main function for fetching performance data for GIGABUILDER and send report
        '''

        # Query
        gigabulder_query = f'''
                select
                    count(block_number) as blocks,
                    countIf(block_number,is_public_block) as public_blocks,
                    countIf(block_number,lower(miner) = lower('0xC15d2Ba418631FC6c90f61fe4dD9A49229BD514F')) as giga_blocks,
                    sumIf(txs,lower(miner) = lower('0xC15d2Ba418631FC6c90f61fe4dD9A49229BD514F')) as giga_txs,
                    sumIf(private_txs,lower(miner) = lower('0xC15d2Ba418631FC6c90f61fe4dD9A49229BD514F')) as giga_private_txs,
                    sumIf(roxana_txs,lower(miner) = lower('0xC15d2Ba418631FC6c90f61fe4dD9A49229BD514F')) as roxana_txs,
                    sumIf(hostel_txs,lower(miner) = lower('0xC15d2Ba418631FC6c90f61fe4dD9A49229BD514F')) as hostel_txs,
                    sumIf(bribe,lower(miner) = lower('0xC15d2Ba418631FC6c90f61fe4dD9A49229BD514F')) as giga_bribe,
                    sumIf(txs_fees,lower(miner) = lower('0xC15d2Ba418631FC6c90f61fe4dD9A49229BD514F')) as giga_txs_fees,
                    sumIf(burnt_fees,lower(miner) = lower('0xC15d2Ba418631FC6c90f61fe4dD9A49229BD514F')) as giga_burnt,
                    sumIf(txs_fees - burnt_fees,lower(miner) = lower('0xC15d2Ba418631FC6c90f61fe4dD9A49229BD514F')) as giga_reward,
                    sumIf(fees_for_validator_reward,lower(miner) = lower('0xC15d2Ba418631FC6c90f61fe4dD9A49229BD514F')) as giga_fees,
                    sumIf(validator_reward,lower(miner) = lower('0xC15d2Ba418631FC6c90f61fe4dD9A49229BD514F')) as giga_validator_reward,
                    sumIf(roxana_net_profit,lower(miner) = lower('0xC15d2Ba418631FC6c90f61fe4dD9A49229BD514F')) as roxana_net,
                    sumIf(roxana_traded_volume,lower(miner) = lower('0xC15d2Ba418631FC6c90f61fe4dD9A49229BD514F')) as roxana_traded,
                    sumIf(bribe + txs_fees - burnt_fees - validator_reward - fees_for_validator_reward,lower(miner) = lower('0xC15d2Ba418631FC6c90f61fe4dD9A49229BD514F')) as giga_net,
                    sumIf((bribe + txs_fees - burnt_fees - validator_reward - fees_for_validator_reward) * price_eth_usd,lower(miner) = lower('0xC15d2Ba418631FC6c90f61fe4dD9A49229BD514F')) as giga_net_usd
                from mart_data_gigabuilder.eth_blocks
                where toDate(block_timestamp) = '{REPORT_DATE}'
        '''

        gigabulder_data = int_client_ch_analytics.query_to_df(gigabulder_query)
        logging.info(f"GIGABUILDER Report: Fetch data")

        if not gigabulder_data.empty:
            # Get volumes from group
            number_of_blocks = gigabulder_data['blocks'].iloc[0]
            public_blocks = gigabulder_data['public_blocks'].iloc[0]
            public_blocks_cr = round(public_blocks / number_of_blocks * 100,2) if number_of_blocks != 0 else 0
            giga_blocks = gigabulder_data['giga_blocks'].iloc[0]
            giga_blocks_cr = round(giga_blocks / number_of_blocks * 100,3) if number_of_blocks != 0 else 0

            giga_txs = gigabulder_data['giga_txs'].iloc[0]
            giga_private_txs = gigabulder_data['giga_private_txs'].iloc[0]
            giga_public_txs = giga_txs - giga_private_txs
            giga_txs = gigabulder_data['giga_txs'].iloc[0]
            roxana_txs = gigabulder_data['roxana_txs'].iloc[0]
            roxana_txs_share = round(roxana_txs / giga_txs * 100,1) if giga_txs != 0 else 0
            hostel_txs = gigabulder_data['hostel_txs'].iloc[0]
            hostel_txs_share = round(hostel_txs / giga_txs * 100,1) if giga_txs != 0 else 0

            giga_bribe = round(gigabulder_data['giga_bribe'].iloc[0],4)
            giga_reward = round(gigabulder_data['giga_reward'].iloc[0],4)
            giga_validator_reward = round(gigabulder_data['giga_validator_reward'].iloc[0],4)
            giga_fees = round(gigabulder_data['giga_fees'].iloc[0],4)
            roxana_net = round(gigabulder_data['roxana_net'].iloc[0],2)
            roxana_traded = round(gigabulder_data['roxana_traded'].iloc[0],2)
            giga_net = round(gigabulder_data['giga_net'].iloc[0],2)
            giga_net_usd = round(gigabulder_data['giga_net_usd'].iloc[0],2)
            
            # Formating integer numbers to readable format
            number_of_blocks = "{:,.0f}".format(number_of_blocks)
            public_blocks = "{:,.0f}".format(public_blocks)
            giga_txs = "{:,.0f}".format(giga_txs)
            roxana_net = "{:,.2f}".format(roxana_net) 
            roxana_traded = "{:,.2f}".format(roxana_traded)
            giga_net_usd = "{:,.2f}".format(giga_net_usd)
            # Prepare the message
            message = f"| _*Blocks*_: {number_of_blocks} _(100%)_ \n" \
                    f"| _*Public blocks*_: {public_blocks} _({public_blocks_cr}%)_ \n" \
                    f"| _*Gigabuilder blocks*_: {giga_blocks} _({giga_blocks_cr}%)_ \n\n" \
                    f"| _*Mined transactions*_: {giga_txs} ({giga_private_txs} _*Private*_ + {giga_public_txs} _*Public*_) \n" \
                    f"| _*Roxana transactions*_: {roxana_txs} _({roxana_txs_share}%)_ \n" \
                    f"| _*Hostel transactions*_: {hostel_txs} _({hostel_txs_share}%)_ \n\n" \
                    f"| _*Gross (Block Reward)*_: {round(giga_reward+giga_bribe,4)} ETH ({giga_reward} _*Fees*_ + {giga_bribe} _*Bribe*_) \n" \
                    f"| _*Fees*_: {round(giga_validator_reward+giga_fees,4)} ETH \n" \
                    f"| _*Net*_: {giga_net} ETH (_${giga_net_usd}_)\n" \
                    f"| _*Roxana Traded*_: ${roxana_traded} \n" \
                    f"| _*Roxana Net*_: ${roxana_net}"
            
            # Prepare header for message based on message with all chain data or with instance data
            header = f'GIGABUILDER Report'
            logging.info(f"GIGANUILDER Report: Report prepared")

            # Send message
            slack_con.post_blocks_message(
                date=REPORT_DATE,
                header=header,
                text=message,
                push_text=header,
                channel_name='gigabuilder-reports'
            )
        else:
            logging.info(f"GIGABUILDER Report: No data for report")

        return True 

################## !GIGABUILDER REPORT ##################

################## GIGABUILDER LOSS REASONS REPORT ##################

    def send_gigabuilder_loss_reasons():
        '''
        Get all reasons of lost blocks by Gigabuilder in Ethereum and sent report to slack
        '''

        reasons_dict = {
            'on_research' : 'On research',
            'lower_reward': 'Lower Reward',
            'not_build': 'Did Not Build',
            'late_bid': 'Late Submission',
            'rejected_bid': 'Bids Rejected',
            'our_block': 'Gigabuilder Blocks'
        }

        reasons_query = f"""
            with blocks as (
                select
                    flag,
                    is_public_block,
                    count(distinct block_number) as blocks
                from mart_data_gigabuilder.eth_blocks 
                where 
                    toDate(block_timestamp) = '{REPORT_DATE}'
                group by 1,2   
            )

            select 
                flag,
                is_public_block,
                blocks,
                blocks / (select sum(blocks) from blocks) * 100 as share
            from blocks
            order by 3 desc
        """

        reward_diff_query = f"""
            with blocks as (
                select
                    block_timestamp,
                    if(
                        validator_reward = 0, 
                        bribe + txs_fees - burnt_fees - giga_intime_bid_validator_reward, 
                        validator_reward - giga_intime_bid_validator_reward
                    ) as reward_diff
                from mart_data_gigabuilder.eth_blocks 
                where 
                    toDate(block_timestamp) = '{REPORT_DATE}'
                    and not is_public_block
                    and flag = 'lower_reward'
            )

            select
                round(quantile(0.005)(reward_diff),3) as reward_05_perc,
                round(quantile(0.01)(reward_diff),3) as reward_1_perc,
                round(quantile(0.05)(reward_diff),3) as reward_5_perc
            from blocks
        """

        private_flow_query = f"""
            select
                sum(giga_bid_txs) as bid_txs,
                sum(giga_bid_private_txs) as private_txs,
                sum(giga_bid_roxana_txs) as rox_main_txs,
                sum(giga_bid_gigachappie_txs) as rox_gigachappie_txs,
                private_txs / bid_txs * 100 as private_share,
                rox_main_txs / bid_txs * 100 as rox_main_share,
                rox_gigachappie_txs / bid_txs * 100 as rox_gigachappie_share
            from mart_data_gigabuilder.eth_blocks
            where 
                toDate(block_timestamp) = '{REPORT_DATE}'
                and flag != 'not_build'
        """
        # Fetch data from CH and divide df to public and private blocks info
        reasons_df = int_client_ch_analytics.query_to_df(reasons_query)
        reasons_public_df = reasons_df[reasons_df['is_public_block']]
        reasons_private_df = reasons_df[~reasons_df['is_public_block']]
        public_flags = reasons_public_df['flag'].tolist()
        private_flags = reasons_private_df['flag'].tolist()

        logging.info(f"GIGABUILDER Loss Reasons Report: fetch data from CH for loss reasons")

        # 1. Prepare message with private block loss reasons
        private_message = f"*1. Private Blocks: {reasons_private_df['blocks'].sum()} blocks ({round(reasons_private_df['share'].sum(),2)}%)* \n"
        for flag in private_flags:
            flag_name = reasons_dict.get(flag)
            blocks = reasons_private_df[reasons_private_df['flag']==flag]['blocks'].iloc[0]
            blocks = "{:,.0f}".format(blocks)
            share = round(reasons_private_df[reasons_private_df['flag']==flag]['share'].iloc[0],2)
            reason_text = f"  | _*{flag_name}*_: {blocks} blocks _({share}%)_ \n"
            private_message += reason_text

        # 2. Prepare message with public block loss reasons
        public_message = f"\n*2. Public Blocks: {reasons_public_df['blocks'].sum()} blocks ({round(reasons_public_df['share'].sum(),2)}%)* \n"
        for flag in public_flags:
            flag_name = reasons_dict.get(flag)
            blocks = reasons_public_df[reasons_public_df['flag']==flag]['blocks'].iloc[0]
            blocks = "{:,.0f}".format(blocks)
            share = round(reasons_public_df[reasons_public_df['flag']==flag]['share'].iloc[0],2)
            reason_text = f"  | _*{flag_name}*_: {blocks} blocks _({share}%)_ \n"
            public_message += reason_text

        logging.info(f"GIGABUILDER Loss Reasons Report: massage prepared for loss reasons")

        # 3. Prepare message with the lowest reward diff
        reward_diff_df = int_client_ch_analytics.query_to_df(reward_diff_query)
        reward_05_perc = round(reward_diff_df['reward_05_perc'].iloc[0],4)
        reward_1_perc = round(reward_diff_df['reward_1_perc'].iloc[0],4)
        reward_5_perc = round(reward_diff_df['reward_5_perc'].iloc[0],4)

        logging.info(f"GIGABUILDER Loss Reasons Report: fetch data from CH for reward diff")

        reward_diff_message = f"\n*3. The Lowest Reward Diff:* \n" \
                f"  | _*0.5 Percentile*_: {reward_05_perc} ETH \n" \
                f"  | _*1 Percentile*_: {reward_1_perc} ETH \n" \
                f"  | _*5 Percentile*_: {reward_5_perc} ETH \n"

        logging.info(f"GIGABUILDER Loss Reasons Report: message prepared for reward diff")

        # 4. Prepare message with the private flow
        private_flow_df = int_client_ch_analytics.query_to_df(private_flow_query)
        bid_txs = "{:,.0f}".format(private_flow_df['bid_txs'].iloc[0])
        private_txs = "{:,.0f}".format(private_flow_df['private_txs'].iloc[0])
        rox_main_txs = "{:,.0f}".format(private_flow_df['rox_main_txs'].iloc[0])
        rox_gigachappie_txs = "{:,.0f}".format(private_flow_df['rox_gigachappie_txs'].iloc[0])
        private_share = round(private_flow_df['private_share'].iloc[0],1)
        rox_main_share = round(private_flow_df['rox_main_share'].iloc[0],1)
        rox_gigachappie_share = round(private_flow_df['rox_gigachappie_share'].iloc[0],1)

        logging.info(f"GIGABUILDER Loss Reasons Report: fetch data from CH for private flow")

        private_flow_message = f"\n*4. Bid Transactions Flow:* \n" \
                f"  | _*Bid*_: {bid_txs} txs _(100%)_ \n" \
                f"  | _*Private*_: {private_txs} txs _({private_share}%)_ \n" \
                f"  | _*Roxana Gigachappie*_: {rox_gigachappie_txs} txs _({rox_gigachappie_share}%)_ \n" \
                f"  | _*Roxana Main Flow*_: {rox_main_txs} txs _({rox_main_share}%)_"
                
        logging.info(f"GIGABUILDER Loss Reasons Report: message prepared for private flow")

        # Union all parts to one message and send report
        message = private_message + public_message + reward_diff_message + private_flow_message
        # If for function header is not provided. Save it as default
        header = f'GIGABUILDER | Block Loss Reasons'

        # Send message
        slack_con.post_blocks_message(
            date=REPORT_DATE,
            header=header,
            text=message,
            push_text=header,
            channel_name='gigabuilder-reports'
        )

        return True

################## !GIGABUILDER LOSS REASONS REPORT ##################

    # # Define task
    send_general_roxana_report = PythonOperator(
        task_id='send_general_roxana_report',
        python_callable=send_trading_volumes_reports,
        dag=dag,
    )

    # send_roxana_gainers_report = PythonOperator(
    #     task_id='send_roxana_gainers_report',
    #     python_callable=send_gainers_report,
    #     dag=dag,
    # )

    send_gigabuilder_general_report = PythonOperator(
        task_id='send_gigabuilder_general_report',
        python_callable=send_gigabuilder_report,
        dag=dag,
    )

    send_bug_roxana_report = PythonOperator(
        task_id='send_bug_roxana_report',
        python_callable=send_bug_report,
        dag=dag,
    )

    send_roxana_market_hedge_report = PythonOperator(
        task_id='send_roxana_market_hedge_report',
        python_callable=send_mrk_hedge_report,
        dag=dag,
    )

    send_gigabuilder_loss_reasons_report = PythonOperator(
        task_id='send_gigabuilder_loss_reasons_report',
        python_callable=send_gigabuilder_loss_reasons,
        dag=dag,
    )

    #Define task relation
    #send_roxana_gainers_report
    #send_bug_roxana_report
    #send_roxana_market_hedge_report
    #send_general_roxana_report
    #send_gigabuilder_general_report
    
    send_bug_roxana_report >> send_gigabuilder_loss_reasons_report >> send_gigabuilder_general_report >> send_roxana_market_hedge_report >> send_general_roxana_report