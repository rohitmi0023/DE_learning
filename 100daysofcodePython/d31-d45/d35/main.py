import requests
from twilio.rest import Client

THRESHOLD_TEMPERATURE = 35.0

# https://api.openweathermap.org/data/2.5/forecast?lat=<LAT>8&lon=<LONG>&appid=c99fb367d5f0b2c123e8cf615b8a6256

api_key = ''
account_sid = ''
auth_token = ''

params = {
    'lat': <LAT>,
    'lon': <LONG>,
    'appid': api_key,
    'cnt': 8,
    'units': 'metric'
}

res = requests.get(url=f'https://api.openweathermap.org/data/2.5/forecast', params=params)
res.raise_for_status()
data = res.json()

# is temperature exceeding threshold degrees Celsius tomorrow
def is_temp_above_threshold(data: dict) -> bool:
    try:
        all_days = data['list']
        for day in all_days:
            max_temp = day['main']['temp_max']
            if max_temp > THRESHOLD_TEMPERATURE:
                return True
        return False
    except KeyError as e:
        print(e)
        return None

if is_temp_above_threshold(data):
    # twilio
    client = Client(account_sid, auth_token)

    message = client.messages.create(
    from_='whatsapp:<TWILIO_NUMBER>',
    body=f'Hot weather🥵 ahead exeeding {THRESHOLD_TEMPERATURE} Celsius',
    # content_sid='HXb5b62575e6e4ff6129ad7c8efe1f983e',
    # content_variables='{"1":"12/1","2":"3pm"}',
    to='whatsapp:<OWN_NUMBER>
    )

    print(message.status)