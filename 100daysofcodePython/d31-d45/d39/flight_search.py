import requests
from data_manager import DataManager
from dotenv import load_dotenv
import os
from requests.auth import HTTPBasicAuth
from pprint import pprint
from datetime import datetime, timedelta

load_dotenv('100daysofcodePython/d31-d45/d39/keys.env')

AMADEUS_SERVER = 'https://test.api.amadeus.com/v1'
FLIGHT_ENDPOINT = "https://test.api.amadeus.com/v2/shopping/flight-offers"

class FlightSearch:
    #This class is responsible for talking to the Flight Search API.
    def __init__(self):
        self.base_url = 'https://test.api.amadeus.com/v1'
        self._api_key = os.getenv('AMADEUS_API_KEY')
        self._api_secret = os.getenv('AMADEUS_API_SECRET')
        self._token = self._get_new_token()
    
    def _get_new_token(self):
        header = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        body = {
            'grant_type': 'client_credentials',
            'client_id': self._api_key,
            'client_secret': self._api_secret
        }
        token_endpoint = f'{AMADEUS_SERVER}/security/oauth2/token'
        res = requests.post(url=token_endpoint, headers=header, data=body)
        return res.json()['access_token']
                   
    def get_IATA_code(self, city: str):
        city_search_endpoint = f'{AMADEUS_SERVER}/reference-data/locations/cities'
        header = {
            'Authorization': f'Bearer {self._token}'            
        }
        params = {
            'keyword': city.capitalize(),
            'max': '2',
            'include': 'AIRPORTS'
        }
        res = requests.get(url=city_search_endpoint, params=params, headers=header)
        try:
            data:list = res.json()['data']
            code = data[0]['iataCode']
        except KeyError as error:
            code = 'Not Found'
            print(f'KeyError: {error} for city: {city.capitalize()}')
        except IndexError as error:
            code = 'N/A'
            print(f'IndexError: {error} for city: {city.capitalize()}')
        return code

    def check_flights(self, origin_city_code, destination_city_code, from_time, to_time, is_direct=True):
        headers = {"Authorization": f"Bearer {self._token}"}
        query = {
            "originLocationCode": origin_city_code,
            "destinationLocationCode": destination_city_code,
            "departureDate": from_time.strftime("%Y-%m-%d"),
            "returnDate": to_time.strftime("%Y-%m-%d"),
            "adults": 1,
            "nonStop": str(is_direct).lower(),
            "currencyCode": "GBP",
            "max": "10",
        }

        response = requests.get(
            url=FLIGHT_ENDPOINT,
            headers=headers,
            params=query,
        )

        if response.status_code != 200:
            print(f"check_flights() response code: {response.status_code}")
            print("There was a problem with the flight search.\n"
                  "For details on status codes, check the API documentation:\n"
                  "https://developers.amadeus.com/self-service/category/flights/api-doc/flight-offers-search/api"
                  "-reference")
            print("Response body:", response.text)
            return None

        return response.json()
