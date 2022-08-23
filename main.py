# Built-in packages
from datetime import datetime
import hashlib
import json

# External Packages
from google.cloud import bigquery
from google.oauth2 import service_account
import requests


API_KEY = '[ YOUR API KEY ]'
SA_CREDENTIALS_FILE = 'credentials.json' 
url_endpoint = 'https://newsapi.org/v2/top-headlines?apiKey={key}&country={country}&pageSize=10'


def extract(api_key, country_code):
    """
    Mengambil data mentah dari API, lalu dirubah menjadi dictionary.

    Args:
        apikey (_type_): API Keynya
        country (_type_): Country code
    """
    endpoint = url_endpoint.format(key=api_key, country=country_code)
    response = requests.get(endpoint)
    
    return response.json() # Ini langsung jadi dictionary
    
def transform(raw_data):
    """Merubah `raw_data` menjadi bentuk yang siap untuk diload di
    staging table.

    Args:
        raw_data: Data mentah artikel dalam bentuk Dictionary.
    """
    
    transformed_data = []
    
    for article in raw_data:
        transformed_data.append(
            {
                'super_key': hashlib.md5(str(article).encode()).hexdigest(),
                'raw_article': json.dumps(article),
                'date': article['publishedAt'][:10],   # "publishedAt": "2022-08-19T10:32:49Z"
                'input_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        )
    
    print('Berhasil transform data!')
    
    return transformed_data
    

def load(transformed_data, table_id):
    """Memasukkan data yang sudah di transform ke table database -> BigQuery.
    """
    credential = service_account.Credentials.from_service_account_file(
            SA_CREDENTIALS_FILE,
        )

    client = bigquery.Client(
        credentials=credential,
        project=credential.project_id,
    )
    
    client.insert_rows_json(table_id, transformed_data)
    
    print("Berhasil")


if __name__ == "__main__":
    raw_data = extract(API_KEY, 'id')['articles']
    transformed_data = transform(raw_data)
    
    table_id = 'stg.test_raw'
    load(transformed_data, table_id)
    