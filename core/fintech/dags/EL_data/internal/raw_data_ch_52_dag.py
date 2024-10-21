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
START_DATE = datetime(2024, 4, 23, 0, 0, 0)
DAG_NAME = 'raw_data_ch_52'
DAG_NUMBER = '1'
DESCRIPTION = 'Extract and load data from Clickhouse .52 to Analytics DWH'

################################## Parametrs. Don't edit it! ##################################
with models.DAG(
        dag_id=f'{DAG_NAME}_dag_{DAG_NUMBER}',
        default_args=default_args,
        catchup=False,
        start_date=START_DATE,
        description=DESCRIPTION,
        schedule_interval='17 0-23/1 * * *',
        tags = ['ch','52','extract','load', 'sql'],
) as dag:
    # Function for getting tables from config
    def read_exported_tables():
        # Import modules for function
        import yaml

        with open('/opt/airflow/dags/repo/configs/extract_clickhouse.yml', 'r') as f:
            data = yaml.safe_load(f)
        prod_config = next((entry for entry in data['clickhouse'] if entry['instance'] == 'ch_52'), None)

        return prod_config['databases']
    # Main function for extract and load data from CH prod to Analytics DWH
    def main_func(database,table_config):
        # Import modules for function
        from airflow.hooks.base_hook import BaseHook
        from custom_modules import internal_clickhouse
        import pandas as pd
        import time

        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

        # Clickhouse connections
        ch_prod_hook = BaseHook.get_connection('clickhouse_52')
        int_client_ch_prod = internal_clickhouse.Clickhouse(
            host=ch_prod_hook.host, 
            port=ch_prod_hook.port, 
            username=ch_prod_hook.login, 
            password=ch_prod_hook.password
        )
        ch_analytics_hook = BaseHook.get_connection('clickhouse_analytics')
        int_client_ch_analytics = internal_clickhouse.Clickhouse(
            host=ch_analytics_hook.host, 
            port=ch_analytics_hook.port, 
            username=ch_analytics_hook.login, 
            password=ch_analytics_hook.password
        )

        # Parse config data about table
        table_name = table_config['name']
        dest_table_name = table_config['dest_table_name']
        fields = table_config['fields']
        cursor_field = table_config['cursor']
        chunk_size = table_config['chunk_size']

        # Get current max cursor value from analytics table
        cursor_value = int_client_ch_analytics.query_to_df(
            f'select max({cursor_field}) from raw_ch_52.{dest_table_name}'
        ).iloc[0, 0]
        logging.info(f"{database}.{table_name}: cursor values is {cursor_value}")
        
        # If table is empty - get cursor from config file
        if str(cursor_value.date())=='1970-01-01':
            cursor_value = table_config['cursor_value']

        # Generate the query
        query_extract = f"""
            SELECT {fields} 
            FROM {database}.{table_name} 
            WHERE {cursor_field} > '{cursor_value}'
        """

        # Add filters if applicable to the query
        try:
            filters = table_config['filters']
            query_extract += f' AND {filters}'
        except:
            logging.info(f"{database}.{table_name}: didn't have additional filters.")

        # Add group by if applicable to the query
        try:
            group_by = table_config['group_by']
            query_extract += f' GROUP BY {group_by}'
        except:
            logging.info(f"{database}.{table_name}: didn't have group by aggregation.")
        
        # Order by cursor field
        order_by = table_config['order_by']
        query_extract += f' ORDER BY {order_by}'
        df = int_client_ch_prod.query_to_df(query_extract)
        logging.info(f"{database}.{table_name}: downloaded {df.shape[0]} rows.")
        
        # Change data types accordinally types in the destination table
        df = int_client_ch_analytics.optimize_data_types_to_ch_format(
            table_name=f'raw_ch_52.{dest_table_name}',
            dataframe=df
        )
        logging.info(f"Dataframe was optimized for inserting to Clickhouse")
        
        # Split the DataFrame into chunks for inserting
        chunks = [df[i:i+chunk_size] for i in range(0, len(df), chunk_size)]
        logging.info(f"{database}.{table_name}: Start to insert {len(chunks)} chunks by {chunk_size} rows")
        for i in range(len(chunks)):    
            int_client_ch_analytics.insert_df(f'raw_ch_52.{dest_table_name}', chunks[i])
            logging.info(f"Inserted {i+1} / {len(chunks)} chunks")
            time.sleep(1)

    # Read config
    prod_config = read_exported_tables()
    for db_config in prod_config:
        database = db_config['name']

        # Define tasks
        run_export = [PythonOperator(
            task_id='exporting_{}'.format(table_config['name']),
            execution_timeout=timedelta(minutes=30),
            python_callable=main_func,
            op_kwargs={
                'table_config': table_config,
                'database': database
            },
        ) for table_config in db_config['tables']]
    
    # Define task relation
    run_export