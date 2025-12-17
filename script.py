import os
import requests
from dotenv import load_dotenv
from datetime import datetime
import snowflake.connector

load_dotenv("/Users/hari/Stock-Trading-App-Py/.env")

MASSIVE_API_KEY = os.getenv('MASSIVE_API_KEY')

# Snowflake credentials
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT') 
SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

limit = 1000

DS = '2025-12-17'

def upload_to_snowflake(tickers, fieldnames):
    """Upload ticker data to Snowflake"""
    try:
        # Connect to Snowflake
        print(f"Connecting to Snowflake account: {SNOWFLAKE_ACCOUNT}")
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            role=SNOWFLAKE_ROLE,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        
        cursor = conn.cursor()
        
        # Use fully qualified table name
        table_name = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.TICKERS"
        
        # Create table if it doesn't exist
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ticker VARCHAR(50),
            name VARCHAR(500),
            market VARCHAR(50),
            locale VARCHAR(10),
            primary_exchange VARCHAR(50),
            type VARCHAR(50),
            active BOOLEAN,
            currency_name VARCHAR(10),
            cik VARCHAR(50),
            composite_figi VARCHAR(50),
            share_class_figi VARCHAR(50),
            last_updated_utc TIMESTAMP_NTZ,
            ds VARCHAR(50)
        )
        """
        cursor.execute(create_table_sql)
        print(f"Table {table_name} created or already exists")
        
        # Add ds column if it doesn't exist (for existing tables)
        try:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS ds VARCHAR(50)")
            print("Column DS added or already exists")
        except Exception as e:
            print(f"Note: Could not add DS column (may already exist): {e}")
        
        # Clear existing data
        cursor.execute(f"TRUNCATE TABLE IF EXISTS {table_name}")
        print(f"Cleared existing data from {table_name}")
        
        # Prepare insert statement - Snowflake connector uses %s placeholders
        # Convert field names to uppercase to match Snowflake's default (unquoted identifiers are uppercase)
        uppercase_fields = [field.upper() for field in fieldnames]
        field_list = ', '.join(uppercase_fields)
        placeholders = ', '.join(['%s'] * len(fieldnames))
        insert_sql = "INSERT INTO " + table_name + " (" + field_list + ") VALUES (" + placeholders + ")"
        
        # Convert tickers to list of tuples
        rows = []
        for t in tickers:
            row = []
            for field in fieldnames:
                value = t.get(field, '')
                # Handle None values
                if value is None:
                    value = None
                # Keep boolean as boolean (Snowflake handles it)
                row.append(value)
            rows.append(tuple(row))
        
        # Insert data in batches
        batch_size = 1000
        total_inserted = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            cursor.executemany(insert_sql, batch)
            total_inserted += len(batch)
            print(f"Inserted {total_inserted}/{len(rows)} rows...")
        
        conn.commit()
        print(f"Successfully uploaded {len(tickers)} tickers to Snowflake")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error uploading to Snowflake: {str(e)}")
        raise

def run_stock_job():
    url = f'https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={limit}&sort=ticker&apiKey={MASSIVE_API_KEY}'
    response = requests.get(url)
    DS = datetime.now().strftime('%Y-%m-%d')  # Fixed: %d instead of %D
    tickers = []

    data = response.json()
    if 'results' in data:
        for ticker in data['results']:
            ticker['ds'] = DS  # Fixed: use 'ds' as string key
            tickers.append(ticker)

    while 'next_url' in data:
        print("requesting next page", data['next_url'])
        response = requests.get(data['next_url'] + f'&apiKey={MASSIVE_API_KEY}')
        data = response.json()
        if 'results' in data:
            for ticker in data['results']:
                ticker['ds'] = DS  # Fixed: use 'ds' as string key and add inside loop
                tickers.append(ticker)
        else:
            print(f"Warning: No 'results' key in response. Response: {data}")
            break
        
    example_ticker = {'ticker': 'HIT', 
                    'name': 'Health In Tech, Inc. Class A Common Stock', 
                    'market': 'stocks', 
                    'locale': 'us', 
                    'primary_exchange': 'XNAS', 
                    'type': 'CS', 'active': True, 
                    'currency_name': 'usd', 
                    'cik': '0002019505', 
                    'composite_figi': 'BBG01PK1D0N8', 
                    'share_class_figi': 'BBG01PK1D1P4', 
                    'last_updated_utc': '2025-12-04T07:06:01.330256426Z',
                    'ds': '2025-12-17'}

    fieldnames = list(example_ticker.keys())
    
    # Upload to Snowflake
    upload_to_snowflake(tickers, fieldnames)
    
if __name__ == '__main__':
    run_stock_job()