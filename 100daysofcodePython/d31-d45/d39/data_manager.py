import requests
import os
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
from pprint import pprint

load_dotenv('100daysofcodePython/d31-d45/d39/keys.env')

class DataManager:
    #This class is responsible for talking to the Google Sheet.
    def __init__(self):
        self._user = os.getenv('SHEETY_USERNAME')
        self._password = os.getenv('SHEETY_PASSWORD')
        self._authorization = HTTPBasicAuth(self._user, self._password)
        self.cities = []
        self.sheety_url = os.getenv('SHEETY_URL_ENDPOINT')
        self.sheety_data = [
            {'city': 'Paris', 'iataCode': '', 'lowestPrice': 54, 'id': 2
            },
            {'city': 'Frankfurt', 'iataCode': '', 'lowestPrice': 42, 'id': 3
            },
            {'city': 'Tokyo', 'iataCode': '', 'lowestPrice': 485, 'id': 4
            },
            {'city': 'Hong Kong', 'iataCode': '', 'lowestPrice': 551, 'id': 5
            },
            {'city': 'Istanbul', 'iataCode': '', 'lowestPrice': 95, 'id': 6
            },
            {'city': 'Kuala Lumpur', 'iataCode': '', 'lowestPrice': 414, 'id': 7
            },
            {'city': 'New York', 'iataCode': '', 'lowestPrice': 240, 'id': 8
            },
            {'city': 'San Francisco', 'iataCode': '', 'lowestPrice': 260, 'id': 9
            },
            {'city': 'Dublin', 'iataCode': '', 'lowestPrice': 378, 'id': 10
            }
        ]
        
    def get_destination_data(self):
        res = requests.get(url=self.sheety_url, auth=self._authorization)
        data = res.json()
        pprint(self.sheety_data)
        self.sheety_data = data['prices']
        return self.sheety_data
    
    def update_destination_codes(self):
        for city in self.sheety_data:
            new_data = {
                'price': {
                    'iataCode': city['iataCode']
                }
            }
            res = requests.put(url=f'{self.sheety_url}/{city['id']}', json=new_data, auth=self._authorization)
            pprint(res.json())
    


