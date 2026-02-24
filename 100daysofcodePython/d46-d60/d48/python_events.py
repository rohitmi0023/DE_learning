from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By

webpage = 'https://www.python.org/'

service = Service(executable_path=ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)

driver.maximize_window()
driver.get(webpage)

events_dict = {}
# goal: {0: {'time': '2026-02-27', 'name': 'MLOps Open Source Sprint – PyLadies Amsterdam'}}

menu = driver.find_elements(By.CSS_SELECTOR , value='.event-widget .menu li')
for index, item in enumerate(menu):
    item_details = item.text.split('\n')
    events_dict[index] = {
        'time': item_details[0],
        'name': item_details[1]
    }

print(events_dict)

# time.sleep(5)
driver.quit()
