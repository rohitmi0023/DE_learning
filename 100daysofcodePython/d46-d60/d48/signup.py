from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time


service = Service(executable_path=ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)

webpage = 'https://secure-retreat-92358.herokuapp.com/'

driver.get(webpage)


first_name = driver.find_element(By.NAME, value = 'fName')
first_name.send_keys('random')

last_name = driver.find_element(By.NAME, value = 'lName')
last_name.send_keys('random')

email = driver.find_element(By.NAME, value='email')
email.send_keys('random@gmail.com')

submit = driver.find_element(By.CSS_SELECTOR, value='form button')
submit.click()

time.sleep(5)

driver.quit()

