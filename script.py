import os
import requests
import csv
from dotenv import load_dotenv

load_dotenv()

MASSIVE_API_KEY = os.getenv('MASSIVE_API_KEY')

limit = 1000

url = f'https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={limit}&sort=ticker&apiKey={MASSIVE_API_KEY}'
response = requests.get(url)
tickers = []

data = response.json()
if 'results' in data:
    for ticker in data['results']:
        tickers.append(ticker)

while 'next_url' in data:
    print("requesting next page", data['next_url'])
    response = requests.get(data['next_url'] + f'&apiKey={MASSIVE_API_KEY}')
    data = response.json()
    if 'results' in data:
        for ticker in data['results']:
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
                  'last_updated_utc': '2025-12-04T07:06:01.330256426Z'}

csv_path = 'tickers.csv'
fieldnames = list(example_ticker.keys())
        
with open(csv_path, mode='w', newline='', encoding='UTF-8') as f:
    writer = csv.DictWriter(f, fieldnames, restval='')
    writer.writeheader()
    for t in tickers:
        row = {key: t.get(key, '') for key in fieldnames}
        writer.writerow(row)
print(f'wrote {len(tickers)} to {csv_path}')