from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
from selenium.webdriver.common.by import By

log_path = '100daysofcodePython/d46-d60/d48/chromedriver.log'
webpage = 'https://www.irctc.co.in/nget/train-search'
# Create a single driver instance
service = Service(executable_path=ChromeDriverManager().install())
service.creation_args = ['--verbose']

with open(file=log_path, mode='w') as f:
    service.log_output = f
    print('Service started')
    driver = webdriver.Chrome(service=service)
    driver.maximize_window()
    driver.get(webpage)
    time.sleep(10)
    element = driver.find_element(By.XPATH, value='//*[@id="pr_id_1_list"]/li[4]/span/strong')
    print(element.text)


time.sleep(5)
  # Keep browser open for 10 seconds

# driver.close() # closes a particular tab
driver.quit() # close the entire program