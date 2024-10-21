# Import modules that are reqiured to create DAG (not for functions)
from datetime import datetime, timedelta
import logging

from airflow import models
from airflow.operators.python_operator import PythonOperator

from custom_modules.internal_data_processing import task_fail_slack_alert

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

# Fill literals for DAG's descriptions
START_DATE = datetime(2024, 5, 9, 0, 0, 0)
DAG_NAME = 'raw_data_swarm'
DAG_NUMBER = '1'
DESCRIPTION = 'Extract and Load data about latest tx in block from swarm to Analytics DWH from prod2'

################################## Parametrs. Don't edit it! ##################################
with models.DAG(
        dag_id=f'{DAG_NAME}_dag_{DAG_NUMBER}',
        default_args=default_args,
        catchup=False,
        start_date=START_DATE,
        description=DESCRIPTION,
        schedule_interval='20 0-23/1 * * *',
        tags = ['ch','52','extract','load','sql'],
) as dag:

    # Main function
    def fetch_swarm_data(chain,raw_swarm_table):
        # Import modules for function
        from airflow.hooks.base_hook import BaseHook
        from custom_modules import internal_clickhouse
        import pandas as pd
        import time

        # Setup logging
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

        # Clickhouse connections
        ch_52_hook = BaseHook.get_connection('clickhouse_52')
        int_client_ch_52 = internal_clickhouse.Clickhouse(
            host=ch_52_hook.host, 
            port=ch_52_hook.port, 
            username=ch_52_hook.login, 
            password=ch_52_hook.password
        )

        ch_analytics_hook = BaseHook.get_connection('clickhouse_analytics')
        int_client_ch_analytics = internal_clickhouse.Clickhouse(
            host=ch_analytics_hook.host, 
            port=ch_analytics_hook.port, 
            username=ch_analytics_hook.login, 
            password=ch_analytics_hook.password
        )

        # Get last block in swarm data
        cursor_value = int_client_ch_analytics.query_to_df(
            f"""select max(block_number) as block_number from raw_ch_52.swarm where chain = '{chain}'"""
        ).iloc[0, 0]

        # Set default value for cursor if table is empty
        if chain == 'BSC' and cursor_value == 0:
            cursor_value = 38268854
        elif chain == 'ETH' and cursor_value == 0:
            cursor_value = 19842740 
        logging.info(f"{chain}, raw_ch_52.swarm: last block is {cursor_value}")

        # Get transactions for all new blocks after cursor value
        txs_df = int_client_ch_analytics.query_to_df(
            f"""select 
                    tx_hash,
                    block_number,
                    block_timestamp 
                from raw_external.txs_data 
                where block_number > {cursor_value}"""
        )
        # Find the maximum block
        max_block = txs_df['block_number'].max()

        # Remove rows with latest 2 blocks (to exclude logic with delete+insert)
        txs_df = txs_df[txs_df['block_number'] < max_block - 1]

        # Find unique block numbers
        unique_blocks = sorted(txs_df['block_number'].unique())
        logging.info(f"{chain}, raw_ch_52.swarm: Get blocks from txs_data: {len(unique_blocks)} blocks")

        # Find last tx for each block from swarm data
        swarm_data = []
        for block in unique_blocks:
            block_data = {}
            block_df = txs_df[txs_df['block_number'] == block]
            block_timestamp = block_df['block_timestamp'].iloc[0]
            tx_list = [x.decode('utf-8') if isinstance(x, bytes) else x for x in block_df['tx_hash'].unique().tolist()]
            # Query
            swarm_query = f""" 
                select 
                    hash, 
                    min(received_at) as first_seen 
                from {raw_swarm_table} 
                where 
                    hash in {tx_list}
                    and node_id < 50 
                    and received_at between toDateTime('{block_timestamp - timedelta(seconds=15)}') and toDateTime('{block_timestamp}')
                group by hash
                order by first_seen desc
                limit 1
            """
            swarm_df = int_client_ch_52.query_to_df(swarm_query)

            if not swarm_df.empty:
                block_data['last_tx_at'] = swarm_df['first_seen'].iloc[0]
            else:
                block_data['last_tx_at'] = datetime(1970, 1, 1, 0, 0, 0)
            block_data['block_number'] = block
            block_data['chain'] = chain
            swarm_data.append(block_data)
        
        logging.info(f"{chain}, raw_ch_52.swarm: Collect swarm data: rows: {len(swarm_data)}")

        # Create dataframe from collected swarm data
        df = pd.DataFrame(swarm_data)

        # Split the DataFrame into chunks for inserting
        chunks = [df[i:i+5000] for i in range(0, len(df), 5000)]
        logging.info(f"{chain}, raw_ch_52.swarm: Start to insert {len(chunks)} chunks by 5000 rows")
        for i in range(len(chunks)):    
            int_client_ch_analytics.insert_df(f'raw_ch_52.swarm', chunks[i])
            logging.info(f"Inserted {i+1} / {len(chunks)} chunks")
            time.sleep(1)

        return True

    # Define task
    download_eth_swarm_data = PythonOperator(
            task_id='download_eth_swarm_data',
            execution_timeout=timedelta(minutes=30),
            python_callable=fetch_swarm_data,
            op_kwargs={
                'chain': 'ETH',
                'raw_swarm_table': 'geth_log.hash_log'
            },
            dag=dag,
    )
    
    # Define task relation
    download_eth_swarm_data