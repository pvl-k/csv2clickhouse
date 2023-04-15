import glob
import pandas as pd
import clickhouse_connect

dirpath = '/Users/pk/Library/GitHub/csv2clickhouse/'                                # absolute path to dir with csv-files and credentials
database_name = 'testdb'                                                            # name of database
database_port = '8443'                                                              # ClickHouse port
database_secure = True                                                              # ClickHouse secure conection setting
replace_flag = False                                                                # Replace table or append data if table already exists


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
        # print(filename)
        # print(df.dtypes)
        # print()
        # print(df)
        # print()
        columnName = list(df.columns.values)
        columnType = [str(elem) for elem in df.dtypes]
        
        for i in range(len(columnType)):
                
            if columnType[i] == 'int8':
                columnType[i] = 'Int8'
            elif columnType[i] == 'int16':
                columnType[i] = 'Int16'
            elif columnType[i] == 'int32':
                columnType[i] = 'Int32'
            elif columnType[i] == 'int64':
                columnType[i] = 'Int64'
            elif columnType[i] == 'int128':
                columnType[i] = 'Int128'
            elif columnType[i] == 'int256':
                columnType[i] = 'Int256'

            elif columnType[i] == 'uint8':
                columnType[i] = 'UInt8'
            elif columnType[i] == 'uint16':
                columnType[i] = 'UInt16'
            elif columnType[i] == 'uint32':
                columnType[i] = 'UInt32'
            elif columnType[i] == 'uint64':
                columnType[i] = 'UInt64'
            elif columnType[i] == 'uint128':
                columnType[i] = 'UInt128'
            elif columnType[i] == 'uint256':
                columnType[i] = 'UInt256'            
            
            elif columnType[i] == 'float16':
                columnType[i] = 'Float32'
            elif columnType[i] == 'float32':
                columnType[i] = 'Float32'
            elif columnType[i] == 'float64':
                columnType[i] = 'Float64'
            elif columnType[i] == 'float128':
                columnType[i] = 'Float128'
            
            elif columnType[i] == 'bool':
                columnType[i] = 'Boolean'
            
            elif columnType[i] == 'datetime64':
                columnType[i] = 'DateTime'
                        
            else:
                columnType[i] = 'String'    

        columnNameTypes = dict(zip(columnName, columnType))
        
        query = ''
        for item in columnNameTypes:
            query += item + ' Nullable(' + columnNameTypes[item] + '), '
        
        if flag == True:
            query = f'CREATE TABLE IF NOT EXISTS {dbname}.{filename[:-4]} ({query[:-2]}) ENGINE Memory'
        elif flag == False:
            query = f'CREATE OR REPLACE TABLE {dbname}.{filename[:-4]} ({query[:-2]}) ENGINE Memory'
        
        # print(query, end='\n\n')

        client.command(query)
        print(f'The table {dbname}.{filename[:-4]} created.', end='\n')

        print(f'Data is loaded into the table {dbname}.{filename[:-4]} uploaded...', end='\n')
        client.insert_df(f'{dbname}.{filename[:-4]}', df)
        print(f'Data into the table {dbname}.{filename[:-4]} has uploaded.', end='\n\n')

        
if __name__ == '__main__':
    csv2clickhouse(dirpath, database_host, 
                   database_port, database_name, 
                   database_username, database_password, 
                   database_secure, replace_flag)
