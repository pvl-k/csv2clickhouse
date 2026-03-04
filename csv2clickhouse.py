import os
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import clickhouse_connect



# Configuration keys
REQUIRED_VARS = ['DB_NAME', 'DB_HOST', 'DB_PORT', 'DB_USER', 'DB_PASSWORD', 'DB_SECURE', 'REPLACE_FLAG']

# Type mapping from Pandas to ClickHouse
TYPE_MAPPING = {
    'int8': 'Int8',
    'int16': 'Int16',
    'int32': 'Int32',
    'int64': 'Int64',
    'int128': 'Int128',
    'int256': 'Int256',
    'uint8': 'UInt8',
    'uint16': 'UInt16',
    'uint32': 'UInt32',
    'uint64': 'UInt64',
    'uint128': 'UInt128',
    'uint256': 'UInt256',
    'float16': 'Float32',
    'float32': 'Float32',
    'float64': 'Float64',
    'float128': 'Float64',
    'bool': 'Boolean',
    'datetime64': 'DateTime'
}

def get_config():
    """Retrieve and validate configuration from environment variables."""
    load_dotenv()
    missing = [var for var in REQUIRED_VARS if not os.getenv(var)]
    if missing:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing)}\n"
            "Please create a .env file from .env.example and fill in all values."
        )
    return {
        'database': os.getenv('DB_NAME'),
        'replace_tables': os.getenv('REPLACE_FLAG').lower() == 'true',
        'host': os.getenv('DB_HOST'),
        'port': int(os.getenv('DB_PORT')),
        'username': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'secure': os.getenv('DB_SECURE').lower() == 'true'
    }

def csv2clickhouse(csv_dir, dbname, replace_tables, **ch_kwargs):
    """Load all CSVs from the given directory into ClickHouse."""
    client = clickhouse_connect.get_client(**ch_kwargs)
    client.command(f'create database if not exists `{dbname}`')
    
    for csv_file in Path(csv_dir).glob('*.csv'):
        df = pd.read_csv(csv_file)
        if df.empty:
            print(f'Skipping empty file: {csv_file.name}')
            continue
        
        table_name = csv_file.stem
        fqn = f'`{dbname}`.`{table_name}`'
        
        # Build column definitions
        columns_def = ', '.join(
            f"`{col}` Nullable({TYPE_MAPPING.get(str(dtype), 'String')})"
            for col, dtype in df.dtypes.items()
        )
        
        # Execute table creation
        mode = 'or replace table' if replace_tables else 'table if not exists'
        client.command(f'create {mode} {fqn} ({columns_def}) engine = MergeTree() order by tuple()')
        print(f'Table {fqn} created')
        
        # Insert data
        client.insert_df(f'{dbname}.{table_name}', df)
        print(f'Data loaded into {fqn}: {len(df)} rows\n')

if __name__ == '__main__':
    config = get_config()
    csv2clickhouse(
        csv_dir=Path(__file__).resolve().parent,
        dbname=config.pop('database'),
        replace_tables=config.pop('replace_tables'),
        **config
    )
