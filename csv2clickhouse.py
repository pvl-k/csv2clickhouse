import glob
import pandas as pd
import clickhouse_connect

dirpath = '/Users/pk/Library/CloudStorage/OneDrive-Личная/GitHub/csv2clickhouse/'   # absolute path to dir with csv-files and credentials
database_name = 'testdb7'                                                           # name of database
database_port = '8443'                                                              # ClickHouse port
database_secure = True                                                              # ClickHouse secure conection setting
action_flag = 'APPEND'                                                              # Action if data exists: APPEND for append or EMPTY for cleaning data before insert 

# read ClickHouse username from 'db_user.txt' file
with open(dirpath + 'db_user.txt', 'r', encoding='utf-8') as fp:
    database_username = fp.read().rstrip()

# read ClickHouse password from 'db_user_password.txt' file
with open(dirpath + 'db_user_password.txt', 'r', encoding='utf-8') as fp:
    database_password = fp.read().rstrip()

# read ClickHouse host from 'db_hostname.txt' file
with open(dirpath + 'db_hostname.txt', 'r', encoding='utf-8') as fp:
    database_host = fp.read().rstrip()


def csv2clickhouse(localpath, dbhost, dbport, dbname, dbuser, dbpassword, dbsecure, flag):
    client = clickhouse_connect.get_client(host=dbhost, port=dbport, 
                                           username=dbuser, password=dbpassword, 
                                           secure=dbsecure)
    
    fileslist = [filename.rsplit('/', 1)[1] for filename in glob.glob(localpath + '*.csv')]

    client.command(f'CREATE DATABASE IF NOT EXISTS {dbname}')

    for filename in fileslist:
        df = pd.read_csv(dirpath + filename)
        columnName = list(df.columns.values)
        columnType = [str(elem) for elem in df.dtypes]
        
        for i in range(len(columnType)):
            if columnType[i] == 'object':
                columnType[i] = 'String'
            elif columnType[i] == 'int8':
                columnType[i] = 'Int8'
            elif columnType[i] == 'int16':
                columnType[i] = 'Int16'
            elif columnType[i] == 'int32':
                columnType[i] = 'Int32'
            elif columnType[i] == 'int64':
                columnType[i] = 'Int64'
            elif columnType[i] == 'uint8':
                columnType[i] = 'UInt8'
            elif columnType[i] == 'uint16':
                columnType[i] = 'UInt16'
            elif columnType[i] == 'uint32':
                columnType[i] = 'UInt32'
            elif columnType[i] == 'uint64':
                columnType[i] = 'UInt64'            
            elif columnType[i] == 'float16':
                columnType[i] = 'Float32'
            elif columnType[i] == 'float32':
                columnType[i] = 'Float32'
            elif columnType[i] == 'float64':
                columnType[i] = 'Float64'

            elif columnType[i] == 'bool':
                columnType[i] = 'UInt8'
            elif columnType[i] == 'datetime64ns':
                columnType[i] = 'DateTime'
            elif columnType[i] == 'timedeltans':
                columnType[i] = 'Int64'     

        columnNameTypes = dict(zip(columnName, columnType))
        
        query = ''
        for item in columnNameTypes:
            query += item + ' ' + columnNameTypes[item] + ', '
        query = f'CREATE TABLE IF NOT EXISTS {dbname}.{filename[:-4]} ({query[:-2]}) ENGINE Memory'
        client.command(query)

        client.insert_df(f'{dbname}.{filename[:-4]}', df)

        
if __name__ == '__main__':
    csv2clickhouse(dirpath, database_host, 
                   database_port, database_name, 
                   database_username, database_password, 
                   database_secure, action_flag)

    
    
    
