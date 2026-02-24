from selenium import webdriver
from selenium.webdriver.chrome.service import Service

from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time
from selenium.webdriver.common.keys import Keys

service = Service(executable_path=ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)

# driver.maximize_window()

webpage = 'https://en.wikipedia.org/wiki/Main_Page'
driver.get(webpage)

# counts = driver.find_element(By.ID, value='articlecount')
article_stats = driver.find_elements(By.CSS_SELECTOR, value='#articlecount a')
article_count = article_stats[1].text

# article_stats[1].click()

all_portals = driver.find_element(By.LINK_TEXT, value='Content portals')
# all_portals.click()

search = driver.find_element(By.NAME, value='search')
search.send_keys('python')
search.send_keys(Keys.ENTER)

time.sleep(5)

driver.quit()
