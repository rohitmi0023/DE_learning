import requests
import datetime as dt
from track_stock import TrackStockChange
from news import News, news_data
from twilio.rest import Client

API_KEY = 'BX8FJIGOUDS792TT'
STOCK = "TSLA"
COMPANY_NAME = "Tesla Inc"
NEWS_COMPANY_NAME = 'Tesla'
account_sid = ''
auth_token = ''

NEWS_API_KEY = 'c08fb490107a451f999092ac123824e8'
MAJOR_CHANGE_PERCENT = 2.00
MAJOR_CHANGE_HAPPENED = False

## STEP 1: Use https://www.alphavantage.co
# When STOCK price increase/decreases by 5% between yesterday and the day before yesterday then print("Get News").

track_stock = TrackStockChange(STOCK, API_KEY)
data = track_stock.get_data()
change_percent = track_stock.get_change_percent(data=data)
change_percent = 3

if change_percent >= MAJOR_CHANGE_PERCENT:
    print('Get News')
    MAJOR_CHANGE_HAPPENED = True

## STEP 2: Use https://newsapi.org
# Instead of printing ("Get News"), actually get the first 3 news pieces for the COMPANY_NAME. 
formatted_articles = []
if MAJOR_CHANGE_HAPPENED:
    # news = News(COMPANY_NAME, NEWS_API_KEY)
    # news_data = news.get_data()
    news_data = news_data
    # print(news_data)
    three_articles = news_data[:3]
    formatted_articles = [f'Headline: {article['title']}. \nBrief: {article['desc']} ' for article in three_articles]

## STEP 3: Use https://www.twilio.com
# Send a seperate message with the percentage change and each article's title and description to your phone number. 

    client = Client(account_sid, auth_token)
    body_description = ''
    for item in formatted_articles:
        body_description += item
    body = f'{STOCK}: 🔺{change_percent}\n{body_description}'
    message = client.messages.create(
        from_='whatsapp:<TWILIO_NUMER>',
        body=f'J',
        to='whatsapp:<OWN_NUMBER>'
    )


#Optional: Format the SMS message like this: 
"""
TSLA: 🔺2%
Headline: Were Hedge Funds Right About Piling Into Tesla Inc. (TSLA)?. 
Brief: We at Insider Monkey have gone over 821 13F filings that hedge funds and prominent investors are required to file by the SEC The 13F filings show the funds' and investors' portfolio positions as of March 31st, near the height of the coronavirus market crash.
or
"TSLA: 🔻5%
Headline: Were Hedge Funds Right About Piling Into Tesla Inc. (TSLA)?. 
Brief: We at Insider Monkey have gone over 821 13F filings that hedge funds and prominent investors are required to file by the SEC The 13F filings show the funds' and investors' portfolio positions as of March 31st, near the height of the coronavirus market crash.
"""

