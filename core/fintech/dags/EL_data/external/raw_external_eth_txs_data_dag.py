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
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': task_fail_slack_alert
}

# Fill literals for DAG's descriptions
START_DATE = datetime(2024, 7, 8, 10, 38, 0)
DAG_NAME = 'raw_external_eth_txs_data'
DAG_NUMBER = '1'
DESCRIPTION = 'Extract and Load all ETH txs data to Analytics DWH'

################################## Parametrs. Don't edit it! ##################################
with models.DAG(
        dag_id=f'{DAG_NAME}_dag_{DAG_NUMBER}',
        default_args=default_args,
        catchup=False,
        start_date=START_DATE,
        description=DESCRIPTION,
        schedule_interval='8,38 * * * *',
        tags = ['extract','load','api','web3'],
) as dag:

    # Main function
    def main_func():
        # Import modules for function
        from airflow.hooks.base_hook import BaseHook
        from custom_modules import internal_clickhouse, etherscan
        import web3
        import pandas as pd
        import time
        import numpy as np
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # Setup logging
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

        # Clickhouse connections
        ch_analytics_hook = BaseHook.get_connection('clickhouse_analytics')
        int_client_ch_analytics = internal_clickhouse.Clickhouse(
            host=ch_analytics_hook.host, 
            port=ch_analytics_hook.port, 
            username=ch_analytics_hook.login, 
            password=ch_analytics_hook.password
        )

        # Provide connection to ETH nodes via web3
        w3 = web3.Web3(web3.Web3.HTTPProvider(models.Variable.get('eth_node_ip')))
        w3_erigon = web3.Web3.HTTPProvider(models.Variable.get('erigon_url'))

        # Query for the last blocks
        last_block = int_client_ch_analytics.query_to_df(
            f'select max(block_number) from raw_external.txs_data'
        ).iloc[0, 0]
        logging.info(f"raw_external.txs_data: Last block is {last_block}")
        last_block += 1
        # Get latest block from ETH Node
        latest_block = w3.eth.get_block('latest')
        latest_block = latest_block['number']

        # If we have missing blocks - start to downloaded data for them
        if latest_block - last_block > 0:
            if latest_block - last_block > 500:
                latest_block = last_block + 500
                
            traced_txs = []

            # Function for parse all txs in block
            def process_tx(tx,block):
                tx_info_list = []
                tx_info = {}

                # Get tx data and collect it in dict
                tx_receipt = w3.eth.get_transaction_receipt(tx)
                tx_data = w3.eth.get_transaction(tx)
                tx_info['block_timestamp'] = pd.to_datetime(block['timestamp'], utc=True, unit='s')
                tx_info['block_number'] = block['number']
                tx_info['miner'] = block['miner']
                tx_info['tx_hash'] = tx.hex()
                tx_info['from'] = tx_receipt['from']
                tx_info['to'] = tx_receipt['to'] if tx_receipt['to'] else '0x0000000000000000000000000000000000000000'
                tx_info['gas_used'] = tx_receipt['gasUsed']
                tx_info['gas_price'] = tx_receipt['effectiveGasPrice']
                tx_info['base_fee'] = block['baseFeePerGas']
                tx_info['num_logs'] = len(tx_receipt['logs'])

                # Get bribe for tx
                if len(tx_receipt['logs']) > 1:
                    # Get internal tx for each tx from Etherscan
                    for i in range(4):
                        try:
                            traced_txs = w3_erigon.make_request('debug_traceTransaction', [
            tx.hex(),{"tracer":'{cs:[{}],ds:!1,step:function(t,e){if(void 0===t.getError()){var s,a,l=240==(240&t.op.toNumber());if(l&&(s=t.op.toString()),l&&("CREATE"==s||"CREATE2"==s)){var o=(n=t.stack.peek(1).valueOf())+t.stack.peek(2).valueOf(),r={type:s,from:toHex(t.contract.getAddress()),input:toHex(t.memory.slice(n,o)),gasIn:t.getGas(),gasCost:t.getCost(),value:"0x"+t.stack.peek(0).toString(16)};return this.cs.push(r),void(this.ds=!0)}if(l&&"SELFDESTRUCT"==s){var i=this.cs.length;return void 0===this.cs[i-1].calls&&(this.cs[i-1].calls=[]),void this.cs[i-1].calls.push({type:s,from:toHex(t.contract.getAddress()),to:toHex(toAddress(t.stack.peek(0).toString(16))),gasIn:t.getGas(),gasCost:t.getCost(),value:"0x"+e.getBalance(t.contract.getAddress()).toString(16)})}if(l&&("CALL"==s||"CALLCODE"==s||"DELEGATECALL"==s||"STATICCALL"==s)){var c=toAddress(t.stack.peek(1).toString(16));if(isPrecompiled(c))return;var n,g="DELEGATECALL"==s||"STATICCALL"==s?0:1,o=(n=t.stack.peek(2+g).valueOf())+t.stack.peek(3+g).valueOf(),r={type:s,from:toHex(t.contract.getAddress()),to:toHex(c),input:toHex(t.memory.slice(n,o)),gasIn:t.getGas(),gasCost:t.getCost(),outOff:t.stack.peek(4+g).valueOf(),outLen:t.stack.peek(5+g).valueOf()};return"DELEGATECALL"!=s&&"STATICCALL"!=s&&(r.value="0x"+t.stack.peek(2).toString(16)),this.cs.push(r),void(this.ds=!0)}this.ds&&(t.getDepth()>=this.cs.length&&(this.cs[this.cs.length-1].gas=t.getGas()),this.ds=!1),l&&"REVERT"==s?this.cs[this.cs.length-1].error="execution reverted":t.getDepth()==this.cs.length-1&&("CREATE"==(r=this.cs.pop()).type||"CREATE2"==r.type?(r.gasUsed="0x"+bigInt(r.gasIn-r.gasCost-t.getGas()).toString(16),delete r.gasIn,delete r.gasCost,(a=t.stack.peek(0)).equals(0)?void 0===r.error&&(r.error="internal failure"):(r.to=toHex(toAddress(a.toString(16))),r.output=toHex(e.getCode(toAddress(a.toString(16)))))):(void 0!==r.gas&&(r.gasUsed="0x"+bigInt(r.gasIn-r.gasCost+r.gas-t.getGas()).toString(16)),(a=t.stack.peek(0)).equals(0)?void 0===r.error&&(r.error="internal failure"):r.output=toHex(t.memory.slice(r.outOff,r.outOff+r.outLen)),delete r.gasIn,delete r.gasCost,delete r.outOff,delete r.outLen),void 0!==r.gas&&(r.gas="0x"+bigInt(r.gas).toString(16)),i=this.cs.length,void 0===this.cs[i-1].calls&&(this.cs[i-1].calls=[]),this.cs[i-1].calls.push(r))}else this.fault(t,e)},fault:function(t,e){if(void 0===this.cs[this.cs.length-1].error){var s=this.cs.pop();s.error=t.getError(),void 0!==s.gas&&(s.gas="0x"+bigInt(s.gas).toString(16),s.gasUsed=s.gas),delete s.gasIn,delete s.gasCost,delete s.outOff,delete s.outLen;t=this.cs.length;if(0<t)return void 0===this.cs[t-1].calls&&(this.cs[t-1].calls=[]),void this.cs[t-1].calls.push(s);this.cs.push(s)}},result:function(t,e){var s={type:t.type,from:toHex(t.from),to:toHex(t.to),value:"0x"+t.value.toString(16),gas:"0x"+bigInt(t.gas).toString(16),gasUsed:"0x"+bigInt(t.gasUsed).toString(16),input:toHex(t.input),output:toHex(t.output),time:t.time};return void 0!==this.cs[0].calls&&(s.calls=this.cs[0].calls),void 0!==this.cs[0].error?s.error=this.cs[0].error:void 0!==t.error&&(s.error=t.error),void 0===s.error||"execution reverted"===s.error&&"0x"!==s.output||delete s.output,this.finalize(s)},scan:function(t,e){var s={type:t.type,from:t.from,to:t.to,value:t.value,gas:t.gas,gasUsed:t.gasUsed,input:t.input,output:t.output,error:t.error,time:t.time};if(e.push(s),void 0!==t.calls)for(var a=0;a<t.calls.length;a++)this.scan(t.calls[a],e)},finalize:function(t){var e=this.f2(t),t=[];return this.scan(e,t),t},f2:function(t){var e,s={type:t.type,from:t.from,to:t.to,value:t.value,gas:t.gas,gasUsed:t.gasUsed,input:t.input,output:t.output,error:t.error,time:t.time,calls:t.calls};for(e in s)void 0===s[e]&&delete s[e];if(void 0!==s.calls)for(var a=0;a<s.calls.length;a++)s.calls[a]=this.f2(s.calls[a]);return s}}'}])
                            traced_txs_df = pd.DataFrame(traced_txs['result'])
                            try:
                                tx_info['bribe'] = int(traced_txs_df[traced_txs_df['to']==block['miner'].lower()]['value'].iloc[0],16)
                            except:
                                tx_info['bribe'] = 0
                            error = None
                            break
                        except Exception as e:
                            error = e
                            time.sleep(0.5)
                            pass
                    if error:
                        raise Exception(error)
                else: 
                    tx_info['bribe'] = 0
                
                tx_info['value'] = tx_data['value']
                try:
                    tx_info['maxPriorityFeePerGas'] = tx_data['maxPriorityFeePerGas']
                except:
                    tx_info['maxPriorityFeePerGas'] = 0
                tx_info['transactionIndex'] = tx_data['transactionIndex']
                tx_info_list.append(tx_info)

                return tx_info_list

            def process_block(block_num):
                block = w3.eth.get_block(block_num)
                txs = block['transactions']
                block_txs = []

                with ThreadPoolExecutor(max_workers=10) as executor:
                    results = executor.map(lambda tx: process_tx(tx, block), txs)

                for result in results:
                    block_txs.extend(result)

                return block_txs

            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(process_block, block_num): block_num for block_num in range(last_block,latest_block)}

                for future in as_completed(futures):
                    block_num = futures[future]
                    try:
                        block_txs = future.result()
                        traced_txs.extend(block_txs)
                        logging.info(f"Processed block {block_num}")
                    except Exception as e:
                        logging.info(f"Error processing block {block_num}: {e}")

            block_df = pd.DataFrame(traced_txs)

            # Replace values that exceed the maximum Uint64 range with max UInt64 value
            block_df['bribe'] = block_df['bribe'] / (10 ** 18)
            block_df['gas_price'] = block_df['gas_price'] / (10 ** 18)
            block_df['base_fee'] = block_df['base_fee'] / (10 ** 18)
            block_df['value'] = block_df['value'] / (10 ** 18)
            block_df['maxPriorityFeePerGas'] = block_df['maxPriorityFeePerGas'] / (10 ** 18)
            
            # Split the DataFrame into chunks for inserting
            chunks = [block_df[i:i+5000] for i in range(0, len(block_df), 5000)]
            logging.info(f"raw_ch_prod2.txs_data: Start to insert {len(chunks)} chunks by 5000 rows")
            for i in range(len(chunks)):    
                int_client_ch_analytics.insert_df(f'raw_external.txs_data', chunks[i])
                logging.info(f"Inserted {i+1} / {len(chunks)} chunks")
                time.sleep(1)
        else:
            logging.info(f"raw_external.txs_data: No missing blocks")
        return True

    # Define task
    download_eth_txs_data = PythonOperator(
            task_id='download_eth_txs_data',
            execution_timeout=timedelta(minutes=30),
            python_callable=main_func,
            dag=dag,
    )
    
    # Define task relation
    download_eth_txs_data