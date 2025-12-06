# csv2clickhouse

A Python script to automatically create ClickHouse tables from CSV files and load data into them. Uses Pandas to infer data schema before creating tables.

## Installation

Install required dependencies:

```bash
pip install -r requirements.txt
```

Or manually:

```bash
pip install pandas clickhouse-connect python-dotenv
```

## Configuration

1. Copy the example environment file:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` with your ClickHouse credentials:
   ```env
   DB_HOST=your-clickhouse-host.com
   DB_PORT=8443
   DB_NAME=your_database_name
   DB_USER=your_username
   DB_PASSWORD=your_password
   DB_SECURE=True
   REPLACE_FLAG=False
   ```

**REPLACE_FLAG options:**
- `False` - Create tables if they don't exist (safe, default)
- `True` - Replace existing tables (⚠️ will delete existing data)

## Usage

1. Place your CSV files in the same directory as the script
2. Configure `.env` file
3. Run:
   ```bash
   python csv2clickhouse.py
   ```

The script will create tables with names matching CSV filenames and load all data.

## Type Mapping

| Pandas | ClickHouse |
|--------|------------|
| int8/16/32/64 | Int8/16/32/64 |
| uint8/16/32/64 | UInt8/16/32/64 |
| float16/32/64 | Float32/32/64 |
| bool | Boolean |
| datetime64 | DateTime |
| others | String |

All columns are created as Nullable types.

## Note

⚠️ Be careful with `REPLACE_FLAG=True` - it will delete all existing data in tables with the same names!


