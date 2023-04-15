import glob
import pandas as pd
import clickhouse_connect

dirpath = '/Users/pk/Downloads/data/'                           # absolute path to dir with csv-files and credentials
database_name = 'testdb6'                                       # name of database
database_port = '8443'                                          # ClickHouse port
database_secure = True                                          # ClickHouse secure conection setting
action_flag = 'EMPTY'                                           # Action if data exists: APPEND for append or EMPTY for cleaning data before insert 

# read ClickHouse username
with open(dirpath + 'db_user.txt', 'r', encoding='utf-8') as fp:
    database_username = fp.read().rstrip()

# read ClickHouse password
with open(dirpath + 'db_user_password.txt', 'r', encoding='utf-8') as fp:
    database_password = fp.read().rstrip()

# ClickHouse host
with open(dirpath + 'db_hostname.txt', 'r', encoding='utf-8') as fp:
    database_host = fp.read().rstrip()


def csv2clickhouse(localcpath, dbhost, dbport, dbname, dbuser, dbpassword, dbsecure, flag):
    client = clickhouse_connect.get_client(host=dbhost, port=dbport, 
                                           username=dbuser, password=dbpassword, 
                                           secure=dbsecure)
    
    fileslist = [filename.rsplit('/', 1)[1] for filename in glob.glob(localcpath + '*.csv')]

    client.command(f'CREATE DATABASE IF NOT EXISTS {dbname}')

    for filename in fileslist:
        df = pd.read_csv(dirpath + filename)
        columnName = list(df.columns.values)
        columnType = [str(elem) for elem in df.dtypes]
        
        for i in range(len(columnType)):
            if columnType[i] == 'object':
                columnType[i] = 'String'
            # else:
            #     columnType[i] = 'String'
            elif columnType[i] == 'int64':
                columnType[i] = 'UInt64'
            elif columnType[i] == 'int32':
                columnType[i] = 'UInt32'
            elif columnType[i] == 'float64':
                columnType[i] = 'Float64'
            elif columnType[i] == 'float32':
                columnType[i] = 'Float32'    
        
        columnNameTypes = dict(zip(columnName, columnType))
        
        query = ''
        for item in columnNameTypes:
            query += item + ' ' + columnNameTypes[item] + ', '
        query = f'CREATE TABLE IF NOT EXISTS {dbname}.{filename[:-4]} ({query[:-2]}) ENGINE Memory'
        client.command(query)

        if flag == 'EMPTY': # and len(client.command(f'SELECT * FROM {dbname}.{columnName[0]} LIMIT 1')) != 0:
            # client.command(f'DELETE FROM {dbname}.{filename[:-4]} WHERE {columnName[0]} LIKE "%"')
            query = f'ALTER "{filename[:-4]}" UPDATE _row_exists = 0 WHERE {columnName[0]} LIKE "%"'
            print(query)
            client.command(query)

        # for row in df.iterrows():
        #     print(row)

        # for l in df.values.tolist():
        #     # print(l)
        #     s = ', '.join(f'"{str(e)}"' for e in l)
        #     # print(s)
        #     query = 'INSERT INTO OR IGNORE ' + dbname + '.' + filename[:-4] + ' VALUES (' + s + ')' 
        #     print(query)
        #     # client.command(f'INSERT INTO {dbname}.{filename[:-4]} VALUES ({s})')
        #     client.query(query)

        client.insert_df(f'{dbname}.{filename[:-4]}', df)

        # print()
        # print(query)
        # print()

        
if __name__ == '__main__':
    csv2clickhouse(dirpath, database_host, 
                   database_port, database_name, 
                   database_username, database_password, 
                   database_secure, action_flag)

    
    
    
