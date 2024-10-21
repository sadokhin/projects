import logging
import clickhouse_connect
import pandas as pd
import datetime
from dateutil import parser
import numpy as np

logger = logging.getLogger(__name__)

class Clickhouse:
    def __init__(self, host, port, username, password):
        self.client = clickhouse_connect.get_client(
            host=host, 
            port=port, 
            username=username, 
            password=password
        )
    
    def query_to_df(self, query):
        '''
        Return data from Clickhouse in Dataframe
        '''
        data = self.client.query(query)
        df_data = data.result_rows
        df_col = data.column_names
        df = pd.DataFrame(data=df_data, columns=df_col)

        return df
    
    def insert_df(self,table_name,df):
        '''
        Insert dataframe into clickhouse table 'table_name'
        Args:
            - table_name: destination table for inserting
            - df: DataFrame with data that should be inserted
        '''
        self.client.insert(table_name, df)

        return True
    
    def truncate_table(self,table_name):
        '''
        Truncate table with name
        Args:
            - table_name: The name of the table that should be truncated
        '''
        self.client.query(f'TRUNCATE TABLE {table_name}')
        return True
      
    def optimize_data_types_to_ch_format(self, table_name, dataframe):
        '''
        Return DataFrame with types for each column that match with destination table in CLickhouse
        Args:
            - table_name: The name of the destination table from which function will get columns and types
            - dataframe: dataframe with columns that should be changed to table's types
        '''
        # Execute SQL query to get column types
        query_data_types = f'SHOW CREATE TABLE {table_name}'
        table_data_types = self.client.query(query_data_types)
        table_data_types = table_data_types.result_rows

        # Extract column types from the result     
        column_types = {}
        for line in table_data_types[0][0].splitlines():
            if line.strip().startswith('`'):
                column_name, column_type = line.split('`')[1], (line.split('`')[2]).strip()
                if column_type.endswith(','):
                    column_type = column_type[:-1]
                column_types[column_name] = column_type
        
        type_mapping = {
            'UInt8': 'uint8',
            'UInt16': 'uint16',
            'UInt32': 'uint32',
            'UInt64': 'uint64',
            'Int8': 'int8',
            'Int16': 'int16',
            'Int32': 'int32',
            'Int64': 'int64',
            'Float32': 'float32',
            'Float64': 'float64',
            'String': 'str',
            #"DateTime64(9, 'UTC')" : ''
            # Add more mappings as needed
        }

        # Convert DataFrame columns to desired data types based on ClickHouse column types
        for column_name, clickhouse_type in column_types.items():
            if column_name in dataframe.columns:
                if clickhouse_type == "DateTime64(9, 'UTC')":
                    try:
                        dataframe[column_name] = pd.to_datetime(dataframe[column_name], format='%Y-%m-%dT%H:%M:%S.%fZ')
                    except:
                        try:
                            dataframe[column_name] = pd.to_datetime(dataframe[column_name], format='%Y-%m-%dT%H:%M:%S.%f')
                        except:
                            dataframe[column_name] = pd.to_datetime(dataframe[column_name], format='%Y-%m-%d %H:%M:%S.%f')
                elif clickhouse_type == "DateTime('UTC')":
                    dataframe[column_name] = pd.to_datetime(dataframe[column_name], format='%Y-%m-%d %H:%M:%S')
                elif column_name == 'bribe':
                    dataframe['bribe'] = dataframe['bribe'].apply(lambda x: np.iinfo(np.uint64).max if int(x) > np.iinfo(np.uint64).max else x)
                    dataframe['bribe'] = dataframe['bribe'].astype('uint64')
                else:
                    pandas_type = type_mapping.get(clickhouse_type, 'object')  # Default to 'object' if mapping not found
                    dataframe[column_name] = dataframe[column_name].astype(pandas_type)
        
        return dataframe
