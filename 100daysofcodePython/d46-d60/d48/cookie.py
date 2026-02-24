from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException, ElementClickInterceptedException
from datetime import datetime, timedelta

SECONDS_TO_CHECK = 10
TOTAL_SECONDS_TO_RUN = 60

service = Service(executable_path=ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)

webpage = 'https://ozh.github.io/cookieclicker/'

driver.get(webpage)

language_text = ''
# make sure page loads complete before selecting any element
while not language_text:
    try:
        language = driver.find_element(By.CSS_SELECTOR, value='#langSelect-EN')
        language_text = language.text
        language.click()
    except:
        print('Page Loading...')
        time.sleep(1)
        
        
start_time = datetime.now()
print(f'Start time is: {start_time}')
end_time = timedelta(seconds=TOTAL_SECONDS_TO_RUN) + start_time 

def purchase_resources():
    print('Executing function: ', purchase_resources.__name__)
    print('==========================')
    
    # Get Number of available coins
    coins_header = driver.find_element(By.CSS_SELECTOR, value='#cookies').text
    coins_list = coins_header.split(' ')
    coins = int(coins_list[0].replace(',', ''))
    print('Available Coins at this time: ', coins)
    
    # Get all the price items
    prices = []
    while len(prices) == 0:
        try:
            all_prices = driver.find_elements(By.CSS_SELECTOR, value='.price')
            for item in all_prices:
                try:
                    txt = item.text.strip().replace(',', '')
                    prices.append(int(txt))
                except ValueError:
                    # If no price shown or non-numeric, use 0 as placeholder
                    prices.append(0)
        except StaleElementReferenceException:
            print('Stale element encountered while getting all the resources — will retry later')
            continue
    print('Available prices at this time', prices)

    # Iterate from most expensive to cheapest using product index alignment
    to_be_purchased = True
    while to_be_purchased:
        for idx in range(len(prices) - 1, -1, -1):
            price = prices[idx]
            if price > 0 and coins >= price:
                product_id = f'product{idx}'
                print(f'Trying to purchase {product_id} for {price} coins')
                try:
                    resource = driver.find_element(By.ID, product_id)
                    resource.click()
                    print(f'Purchased {product_id}')
                    to_be_purchased = False
                except StaleElementReferenceException:
                    # Element went stale; skip this one and continue
                    print('Stale element encountered while purchasing — will retry later')
                    continue
                except Exception as e:
                    print('Unexpected error while attempting purchase:', repr(e))
                break
    time.sleep(1)

second_diff = (datetime.now() - start_time).seconds

while datetime.now() < end_time:
    
    # check if 5 seconds have passed
    new_second_diff = (datetime.now() - start_time).seconds
    if second_diff != new_second_diff:
        print(f'Current time is: {datetime.now()}')
        print('Seconds difference is: ', new_second_diff)
        second_diff = new_second_diff
    if (int(second_diff) % SECONDS_TO_CHECK == 0) and (second_diff != 0):
        purchase_resources()
        
    try:
        button = driver.find_element(By.CSS_SELECTOR, value='#bigCookie')
        button.click()
    except StaleElementReferenceException:
        continue

time.sleep(5)

driver.quit()

