from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import os
from pathlib import Path
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from datetime import datetime

ACCOUNT_EMAIL = 'rohit@test.com'
ACCOUNT_PASSWORD = 'Rohit@0023!'

# user_data_dir = os.path.join(os.getcwd(), 'chrome_profile')
user_data_dir = os.path.join(Path(__file__).resolve().parent, 'chrome_profile')
# os.makedirs(user_data_dir, exist_ok=True)
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument(f"--user-data-dir={user_data_dir}")
# chrome_options.add_experimental_option("detach", True)

service = Service(executable_path=ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)

webpage = 'https://appbrewery.github.io/gym/'

driver.get(webpage)
# driver.implicitly_wait(4)
wait = WebDriverWait(driver, 2)

#================= login feature
# click on login button to go to login page
login_btn = wait.until(ec.element_to_be_clickable((By.ID, 'login-button')))
# login_btn = driver.find_element(By.ID, 'login-button')
login_btn.click()


# fill in email and password
email_input = wait.until(ec.presence_of_element_located((By.ID, 'email-input')))
email_input.clear()
email_input.send_keys(ACCOUNT_EMAIL)
password_input = driver.find_element(By.ID, 'password-input')
password_input.send_keys(ACCOUNT_PASSWORD)
submit_btn = driver.find_element(By.ID, 'submit-button')
submit_btn.click()


# confirming that we are logged in
wait.until(ec.presence_of_element_located((By.CSS_SELECTOR, '#schedule-page h1')))


# =================== Book Upcoming Tuesday class at 6 PM
# find all class cards
already_booked_count = 0
booked_count = 0
waitlist_count = 0

class_cards = driver.find_elements(By.CSS_SELECTOR, "div[id^='class-card-']")
for item in class_cards:
    # Get the day title from the parent day group
    day_group = item.find_element(By.XPATH, "./ancestor::div[contains(@id, 'day-group-')]")
    day_title = day_group.find_element(By.TAG_NAME, 'h2').text
    
    if 'Tue' in day_title or 'Thu' in day_title:
        time_text = item.find_element(By.CSS_SELECTOR, "p[id^='class-time-']").text
        if '6:00 PM' in time_text:
            class_name = item.find_element(By.CSS_SELECTOR, "h3[id^='class-name-']").text
            button = item.find_element(By.CSS_SELECTOR, "button[id^='book-button-']")
            if button.text == 'Booked':
                already_booked_count += 1
                print(f'✅ Already booked: {class_name} on ({day_title})')
            elif button.text == 'Waitlisted':
                print(f'✅ Already on waitlist: {class_name} on ({day_title})')
                already_booked_count += 1
            elif button.text == 'Join Waitlist':
                button.click()
                print(f'✅ Joined waitlist for: {class_name} on ({day_title})')
                waitlist_count += 1
                waitlists_joined += 1
                time.sleep(0.5)
            elif button.text == 'Book Class':
                button.click()
                booked_count += 1
                print(f"✅ Booked: {class_name} on ({day_title}")
                time.sleep(0.5)

# Print summary
print("\n--- BOOKING SUMMARY ---")
print(f"Classes booked: {booked_count}")
print(f"Waitlists joined: {waitlist_count}")
print(f"Already booked/waitlisted: {already_booked_count}")
print(f"Total Tuesday 6pm classes processed: {booked_count + waitlist_count + already_booked_count}")


time.sleep(15)

driver.quit()
# //*[@id="day-group-tue,-mar-3"]  /html/body/div/main/div/div[7]