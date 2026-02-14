import requests
from datetime import datetime
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import os

load_dotenv('100daysofcodePython/d31-d45/d38/keys.env')

BASE_URL = os.getenv('BASE_URL')
APP_ID=os.getenv('APP_ID')
API_KEY=os.getenv('API_KEY')
SHEETY_URL=os.getenv('SHEETY_URL')
USERNAME=os.getenv('USERNAME')
PASSWORD=os.getenv('PASSWORD')


# Log Exercise
exercise_endpoint = f'{BASE_URL}/v1/nutrition/natural/exercise'

# exercise_desc = str(input('Tell me which exercises you did:' ))
exercise_desc = 'I ran 2k today!'
exercise_desc2 = 'Ran 3k and cycled for 20 minutes!'

exercise_endpoint_json = {
    'query': exercise_desc2
}

headers = {
    'x-app-id': APP_ID,
    'x-app-key': API_KEY,
    'Content-Type': 'application/json' 
}

exercise_endpoint_res = requests.post(url=exercise_endpoint, json=exercise_endpoint_json, headers=headers)
# print(exercise_endpoint_res.text)
print(exercise_endpoint_res.json())
exercise_endpoint_res_json = exercise_endpoint_res.json()


# exercise_endpoint_res_json:dict = {'exercises': [{'tag_id': 50, 'user_input': 'I ran 2k today!', 'duration_min': 30, 'met': 9.8, 'nf_calories': 360, 'photo': {'highres': 'https://placeholder.not-a-real-url.com/exercise/50_highres.jpg', 'thumb': 'https://placeholder.not-a-real-url.com/exercise/50_thumb.jpg', 'is_user_uploaded': False}, 'compendium_code': 12050, 'name': 'running', 'description': None, 'benefits': None}]}
# exercise_endpoint_res_json2 = {'exercises': [{'tag_id': 50, 'user_input': 'Ran 3k and cycled for 20 minutes', 'duration_min': 20, 'met': 9.8, 'nf_calories': 240, 'photo': {'highres': 'https://placeholder.not-a-real-url.com/exercise/50_highres.jpg', 'thumb': 'https://placeholder.not-a-real-url.com/exercise/50_thumb.jpg', 'is_user_uploaded': False}, 'compendium_code': 12050, 'name': 'running', 'description': None, 'benefits': None}]}


today = datetime.now()
today_date = today.strftime('%d/%m/%Y')
today_time = today.strftime('%H:%M:%S')

exercises:list= exercise_endpoint_res_json['exercises']

basic = HTTPBasicAuth(USERNAME, PASSWORD)

for exercise in exercises:
    exercise:dict
    exercise_name = exercise['name'].title()
    exercise_duration = str(exercise['duration_min']).title()
    exercise_calories = str(exercise['nf_calories']).title()

    sheety_json = {
        'workout': {
            'date': today_date,
            'time': today_time,
            'exercise': exercise_name,
            'duration': exercise_duration,
            'calories': exercise_calories
        }
    }
    
    sheety_res = requests.post(url=SHEETY_URL, json=sheety_json, auth=basic)
    print(sheety_res.json())
    # print(sheety_res.text)

