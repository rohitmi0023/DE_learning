#This file will need to use the DataManager,FlightSearch, FlightData, NotificationManager classes to achieve the program requirements.
from dotenv import load_dotenv
from data_manager import DataManager
from flight_search import FlightSearch
from flight_data import find_cheapest_flight
import time
from datetime import datetime, timedelta

ORIGIN_CITY_IATA = "LON"

load_dotenv(dotenv_path='100daysofcodePython/d31-d45/d39/keys.env')

data_manager = DataManager()
sheet_data:list = data_manager.get_destination_data()

# display cities list identified in sheet
cities = [item['city'] for item in data_manager.sheety_data]
print('Cities to search for: ', cities)

# get emails list
emails = data_manager.get_customer_emails()
print('Emails to send: ', emails)

flight_search = FlightSearch()
for row in sheet_data:
    if row["iataCode"] == "":
        row["iataCode"] = flight_search.get_IATA_code(row["city"])
        # slowing down requests to avoid rate limit
        time.sleep(1)
        
# overwrite price sheet data variable with new codes
data_manager.sheety_data = sheet_data
# update price sheet data in Sheet
data_manager.update_destination_codes()

#----------------- Get Flight details
# print(sheet_data)
# print(data_manager.sheety_data)
tomorrow = datetime.now() + timedelta(days=1)
six_month_from_today = datetime.now() + timedelta(days=(6 * 30))

for record in sheet_data:
    print(f"Getting flights for {record['city']}...")
    flights:dict = flight_search.check_flights(
        ORIGIN_CITY_IATA,
        record["iataCode"],
        from_time=tomorrow,
        to_time=six_month_from_today
    )
    cheapest_flight = find_cheapest_flight(flights)
    print(f"{record['city']}: £{cheapest_flight.price}")
    # Slowing down requests to avoid rate limit
    time.sleep(2)
    
    
    # ========Search for indirect flight if N/A ===========
    if cheapest_flight.price == 'N/A':
        print(f'No direct flight to {record['city']}. Looking for indirect flights....')
        stopover_flights = flight_search.check_flights(
            ORIGIN_CITY_IATA,
            record['iataCode'],
            from_time=tomorrow,
            to_time=six_month_from_today,
            is_direct=False
        )
        print(stopover_flights)
        cheapest_flight = find_cheapest_flight(stopover_flights)
        print(f'Cheapest indirect flight price is: £{cheapest_flight.price}')
    
