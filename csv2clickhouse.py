import os
import glob
from dotenv import load_dotenv
import pandas as pd
import clickhouse_connect

# Load environment variables from .env file
load_dotenv()

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Configuration variables
database_name = os.getenv('DB_NAME', 'testdb')
database_host = os.getenv('DB_HOST', 'localhost')
database_port = os.getenv('DB_PORT', '8443')
database_user = os.getenv('DB_USER', 'default')
database_password = os.getenv('DB_PASSWORD', '')
database_secure = os.getenv('DB_SECURE', 'True').lower() == 'true'
replace_flag = os.getenv('REPLACE_FLAG', 'False').lower() == 'true'

# Type mapping from Pandas to ClickHouse
type_mapping = {
    'int8': 'Int8', 'int16': 'Int16', 'int32': 'Int32', 'int64': 'Int64',
    'int128': 'Int128', 'int256': 'Int256',
    'uint8': 'UInt8', 'uint16': 'UInt16', 'uint32': 'UInt32', 'uint64': 'UInt64',
    'uint128': 'UInt128', 'uint256': 'UInt256',
    'float16': 'Float32', 'float32': 'Float32', 'float64': 'Float64', 'float128': 'Float128',
    'bool': 'Boolean',
    'datetime64': 'DateTime'
}


def csv2clickhouse(csv_dir, dbhost, dbport, dbname, dbuser, dbpassword, dbsecure, replace_tables):
    # Connect to ClickHouse
    client = clickhouse_connect.get_client(
        host=dbhost, 
        port=dbport, 
        username=dbuser, 
        password=dbpassword, 
        secure=dbsecure
    )
    
    # Get list of CSV files
    csv_files = glob.glob(os.path.join(csv_dir, '*.csv'))
    
    # Create database if not exists
    client.command(f'create database if not exists {dbname}')
    
    # Process each CSV file
    for csv_file in csv_files:
        filename = os.path.basename(csv_file)
        table_name = filename[:-4]  # Remove .csv extension
        
        # Read CSV file
        df = pd.read_csv(csv_file)
        
        # Map column types from Pandas to ClickHouse using dictionary
        column_types = {}
        for col, dtype in df.dtypes.items():
            clickhouse_type = type_mapping.get(str(dtype), 'String')
            column_types[col] = clickhouse_type
        
        # Build column definitions for create table statement
        columns_def = ', '.join([f'{col} Nullable({col_type})' for col, col_type in column_types.items()])
        
        # Create table query
        if replace_tables:
            query = f'create or replace table {dbname}.{table_name} ({columns_def}) engine = MergeTree() order by tuple()'
        else:
            query = f'create table if not exists {dbname}.{table_name} ({columns_def}) engine = MergeTree() order by tuple()'
        
        # Execute table creation
        client.command(query)
        print(f'Table {dbname}.{table_name} created')
        
        # Insert data
        client.insert_df(f'{dbname}.{table_name}', df)
        print(f'Data loaded into {dbname}.{table_name}: {len(df)} rows\n')


if __name__ == '__main__':
    csv2clickhouse(
        csv_dir=script_dir,
        dbhost=database_host,
        dbport=database_port,
        dbname=database_name,
        dbuser=database_user,
        dbpassword=database_password,
        dbsecure=database_secure,
        replace_tables=replace_flag
    )
