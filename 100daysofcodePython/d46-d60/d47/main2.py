from bs4 import BeautifulSoup
import requests
from dotenv import load_dotenv
import os
import smtplib

# AMAZON_PRODUCT_URL = 'https://appbrewery.github.io/instant_pot/'
AMAZON_PRODUCT_URL = 'https://dl.flipkart.com/s/3TctKTNNNN'
TARGETED_PRODUCT_PRICE = 10000.00

load_dotenv(dotenv_path='100daysofcodePython/d46-d60/d47/keys.env')

SMTP_ADDRESS = os.getenv('SMTP_ADDRESS')
EMAIL_ADDRESS = os.getenv('EMAIL_ADDRESS')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')

header = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8", 
    "Accept-Encoding": "gzip, deflate, br, zstd", 
    "Accept-Language": "en-GB,en;q=0.8", 
    "Host": "httpbin.org", 
    "Priority": "u=0, i", 
    "Sec-Ch-Ua": "\"Not(A:Brand\";v=\"8\", \"Chromium\";v=\"144\", \"Brave\";v=\"144\"", 
    "Sec-Ch-Ua-Mobile": "?0", 
    "Sec-Ch-Ua-Platform": "\"macOS\"", 
    "Sec-Fetch-Dest": "document", 
    "Sec-Fetch-Mode": "navigate", 
    "Sec-Fetch-Site": "none", 
    "Sec-Fetch-User": "?1", 
    "Sec-Gpc": "1", 
    "Upgrade-Insecure-Requests": "1", 
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36", 
    "X-Amzn-Trace-Id": "Root=1-699b727b-791bf6c60c4e48ce6e531366"
}

def get_product_price():
    res = requests.get(url=AMAZON_PRODUCT_URL, headers=header)
    text = res.text

    # soup = BeautifulSoup(text, 'html.parser')
    soup = BeautifulSoup(text, 'lxml')
    print(text)

    whole_number_price = soup.find(name='span', class_='a-price-whole')
    print(whole_number_price)
    whole_number_price = whole_number_price.get_text(strip=True).split('.')[0]

    fraction_number_price = soup.find(name='span', class_='a-price-fraction')
    fraction_number_price = fraction_number_price.get_text(strip=True)

    price = whole_number_price + '.' + fraction_number_price
    price = float(price)
    return price

product_price = get_product_price()
# product_price = 99.99

if product_price < TARGETED_PRODUCT_PRICE:
    message = f'Price has dropped to {product_price}'
    with smtplib.SMTP(SMTP_ADDRESS, port=587) as connection:
        connection.starttls()
        connection.login(user=EMAIL_ADDRESS, password=EMAIL_PASSWORD)
        connection.sendmail(
            from_addr=EMAIL_ADDRESS,
            to_addrs=EMAIL_ADDRESS,
            msg=message
        )
        
