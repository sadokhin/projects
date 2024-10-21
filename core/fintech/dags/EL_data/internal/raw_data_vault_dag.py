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
START_DATE = datetime(2024, 4, 15, 0, 0, 0)
DAG_NAME = 'raw_data_vault'
DAG_NUMBER = '1'
DESCRIPTION = 'Extract config data from vault and load it to Analytics DWH'

################################## Parametrs. Don't edit it! ##################################
with models.DAG(
        dag_id=f'{DAG_NAME}_dag_{DAG_NUMBER}',
        default_args=default_args,
        catchup=False,
        start_date=START_DATE,
        description=DESCRIPTION,
        schedule_interval='20 7 * * *',
        tags = ['vault','extract','load','api'],
) as dag:
    # Function for getting tables from config
    def fetch_vault_configs():
        from airflow.hooks.base_hook import BaseHook
        from custom_modules import internal_clickhouse
        import pandas as pd
        import hvac

        # Setup logging
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

        # Vault connections (hosts and tokens)
        vault_host_qb = models.Variable.get('vault_qb_host')
        vault_ldap_password_qb = models.Variable.get('vault_ldap_password_qb')
        vault_ldap_user_qb = models.Variable.get('vault_ldap_user_qb')

        # Vault connections (config's path)
        vault_path_arbitrum = models.Variable.get('vault_path_arbitrum')
        vault_path_eth_lnd = models.Variable.get('vault_path_eth_lnd')
        vault_path_eth_tokyo = models.Variable.get('vault_path_eth_tokyo')

        # Clickhouse connection
        ch_analytics_hook = BaseHook.get_connection('clickhouse_analytics')
        int_client_ch_analytics = internal_clickhouse.Clickhouse(
            host=ch_analytics_hook.host, 
            port=ch_analytics_hook.port, 
            username=ch_analytics_hook.login, 
            password=ch_analytics_hook.password
        )
        # Define configs that should be transferred to the Analytics Clickhouse
        robot_configs = [
            {
                'desc': 'arbitrum_qb',
                'host': vault_host_qb,
                'auth': {'type':'ldap', 'user':'','password':''},
                'path': vault_path_arbitrum,
                'folder':'arbitrum'
            },
            {
                'desc': 'eth_lnd_qb',
                'host': vault_host_qb,
                'auth': {'type':'ldap', 'user':'','password':''},
                'path': vault_path_eth_lnd,
                'folder':'secret'
            },
            {
                'desc': 'eth_tokyo_qb',
                'host': vault_host_qb,
                'auth': {'type':'ldap', 'user':'','password':''},
                'path': vault_path_eth_tokyo,
                'folder':'secret'
            }
        ]
        # Define fields that will be grabbed from configs
        config_pairs_fields = ['dex_name','feeTier','maxOrderSizeInQuote','minOrderSizeInQuote','minPremiumBps','minPremiumBpsBuy','minPremiumBpsSell','min_profit_quote','otcTicker','pairAddress','stepSize','ticker','tokenBase','tokenQuote','tradingIsEnabled']
        config_fields = ['arbMode','dryRun','chain','slippage']
        trading_configs = []

        # Grab data from vault configs
        for config in robot_configs:
            client_hvac = hvac.Client(url=config['host'])
            if config['auth']['type'] == 'token':
                client_hvac.token = config['auth']['token']
            elif config['auth']['type'] == 'ldap':
                client_hvac.auth.ldap.login(username=config['auth']['user'],password=config['auth']['password'])
            logging.info(f"{config['desc']}: Authenticated by {config['auth']['type']} - {client_hvac.is_authenticated()}")

            vault = client_hvac.secrets.kv.v2.read_secret_version(path=config['path'], raise_on_deleted_version=True, mount_point=config['folder'])
            
            # Some config have data: parameters, others have parameters w/o data in level1:
            try:
                parsed_config = vault['data']['data']['data']
            except:
                parsed_config = vault['data']['data']

            for i in parsed_config['tradingPairs']:
                config_values = {}
                for field in config_fields:
                        if field == 'slippage':
                            try:
                                config_values[field] = parsed_config[field]
                            except:
                                config_values[field] = 0
                        else:
                            config_values[field] = parsed_config[field]
                for field in config_pairs_fields:
                        if field == 'minPremiumBps':
                            try:
                                config_values[field] = i[field]
                            except:
                                config_values[field] = 0
                        elif field == 'feeTier' and config['desc'] in ('bsc_prod2','arbitrum_qb'):
                            try:
                                config_values[field] = i['FeeTier']
                            except:
                                config_values[field] = 0
                        else:
                            config_values[field] = i[field]

                config_values['host'] = config['desc']
                trading_configs.append(config_values)
            logging.info(f"{config['desc']}: Parsed")

        # Create dataframe from list with configs
        df = pd.DataFrame(trading_configs)

        # Define the mapping for replacements
        replace_map = {
            'ETH': 'WETH',
            'BNB': 'WBNB',
            '/': ''
        }

        # Apply the replacements using the replace method with the mapping
        df['pair'] = df['ticker'].replace(replace_map, regex=True).replace('/', '')

        if df.shape[0] > 1:
            int_client_ch_analytics.truncate_table('raw_external.vault_trading_config')
            logging.info(f'raw_external.vault_trading_config: Table truncated')
            
            # Insert data to Analytics DWH table
            int_client_ch_analytics.insert_df('raw_external.vault_trading_config', df)
            logging.info(f'raw_external.vault_trading_config: inserted {df.shape[0]} rows')
        else:
            logging.info(f'Configs: Dataframe is empty, rows: {df.shape[0]}')

        return True

    # Define task
    download_vault_robot_configs = PythonOperator(
            task_id='download_vault_robot_configs',
            execution_timeout=timedelta(minutes=30),
            python_callable=fetch_vault_configs,
            dag=dag,
    )
    
    # Define task relation
    download_vault_robot_configs