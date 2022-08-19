# Built-in packages
from datetime import datetime
import hashlib

# External Packages
import requests


API_KEY = '[ YOUR API KEY ]' 
PHI = 3.14
url_endpoint = 'https://newsapi.org/v2/top-headlines?apiKey={key}&country={country}&pageSize=100'


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
                'raw_article': (article),
                'date': datetime.strptime(article['publishedAt'], '%Y-%m-%dT%H:%M:%SZ'),   # "publishedAt": "2022-08-19T10:32:49Z"
                'input_time': datetime.now()
            }
        )
        
    return transformed_data
    

def load():
    """Memasukkan data yang sudah di transform ke table database.
        *** WILL BE UPDATED ***
    """
    pass


if __name__ == "__main__":
    raw_data = extract(API_KEY, 'sg')['articles']
    transformed_data = transform(raw_data)
    