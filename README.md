# csv2clickhouse

This script allows you to create ClickHouse tables based on a CSV file or pandas data frame. It uses pandas to define the data schema before loading CSV files into ClickHouse.

## Usage

Place all your CSV files in one directory with the PY-script, make changes to the TXT files according to your credentials, and specify the relevant data in the Variables section of the PY-script.

## Note

Be careful with the replace_flag - when set to True, the script will recreate tables with the same name if they already exist, so you may lose existing data in your database. To avoid this, but also prevent data duplication, it is recommended to specify a non-existent database name as the database_name. When set to False in the replace_flag, data from your CSV files will be added to existing tables with the same name (of course, the number of columns and their data types must match).

## Compatibility

The compatibility of data types between pandas and ClickHouse may not be complete. If you find any discrepancies, please let us know.
