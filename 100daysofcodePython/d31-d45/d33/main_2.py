import requests
import datetime as dt

MY_LAT = 0.855438
MY_LNG = 0.006165

parameters = {
    'lat': MY_LAT,
    'lng': MY_LNG,
    'formatted': 0
}

def is_iss_overhead():
    res_iss = requests.get(url="http://api.open-notify.org/iss-now.json")
    res_iss.raise_for_status()
    data_iss = res_iss.json()

    iss_latitude = float(data_iss['iss_position']['latitude'])
    iss_longitude = float(data_iss['iss_position']['longitude'])
    print(iss_latitude, iss_longitude)
    if MY_LAT-5 <= iss_latitude <= MY_LAT+5 and MY_LNG-5 <= iss_longitude <= MY_LNG+5:
        return True

def is_night():
    res = requests.get('https://api.sunrise-sunset.org/json', params=parameters)
    res.raise_for_status()
    data = res.json()
    sunrise = data['results']['sunrise'].split('T')[1].split(':')[0]
    sunset = data['results']['sunset'].split('T')[1].split(':')[0]
    time_now = dt.datetime.now().hour
    
    if time_now >= sunset and time_now <= sunrise:
        return True
    
if is_iss_overhead() and is_night():
    print('True')
