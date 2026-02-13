## STEP 1: Use https://www.alphavantage.co
# When STOCK price increase/decreases by 5% between yesterday and the day before yesterday then print("Get News").
import requests

class TrackStockChange:
    def __init__(self, stock_name, API_KEY):
        self.parameters = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': stock_name,
            'apikey': API_KEY
        }
    
    def get_data(self):
        res = requests.get(url='https://www.alphavantage.co/query', params=self.parameters)
        res.raise_for_status()
        return res.json()

    
    def get_change_percent(self, data:dict) -> float:
        try:
            daily:dict = data['Time Series (Daily)'] | {}
        except KeyError as e:
            print(e)
            daily = {}
        stock_price = []
        for index, (key, value) in enumerate(daily.items(), start=1):
            if index > 2:
                break
            if index == 1:
                stock_price.append(value['4. close'])
            if index == 2:
                stock_price.append(value['1. open'])
        try:
            abs_diff = float(stock_price[0]) - float(stock_price[1])
            original_price = float(stock_price[1])
            change_percent = round((abs_diff/original_price)*100,2)
        except IndexError:
            print('Unable to find prices')
            change_percent = 0
        return change_percent
    

players = {'batter': 'virat', 'captain': 'rohit', 'keeper': 'dhoni' }

players_list = [value for (key, value) in players.items()]
print(players_list)