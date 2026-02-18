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
        self.sheety_price_url = os.getenv('SHEETY_PRICE_URL_ENDPOINT')
        self.sheety_users_url = os.getenv('SHEETY_USERS_URL_ENDPOINT')
        self.sheety_data = []
        self.emails = []
        
    def get_destination_data(self):
        res = requests.get(url=self.sheety_price_url, auth=self._authorization)
        data = res.json()
        self.sheety_data:list = data['prices']
        return self.sheety_data
    
    def update_destination_codes(self):
        for city in self.sheety_data:
            new_data = {
                'price': {
                    'iataCode': city['iataCode']
                }
            }
            res = requests.put(url=f'{self.sheety_price_url}/{city['id']}', json=new_data, auth=self._authorization)
    
    def get_customer_emails(self):
        res = requests.get(url=self.sheety_users_url, auth=self._authorization)
        data = res.json()
        data = data['users']
        self.emails = [item['whatIsYourEmail?'] for item in data]
        return self.emails
        
