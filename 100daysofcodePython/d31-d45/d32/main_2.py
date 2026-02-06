##################### Extra Hard Starting Project ######################

# 1. Update the birthdays.csv

# 2. Check if today matches a birthday in the birthdays.csv

# 3. If step 2 is true, pick a random letter from letter templates and replace the [NAME] with the person's actual name from birthdays.csv

# 4. Send the letter generated in step 3 to that person's email address.

import datetime as dt
import pandas as pd
import random
import smtplib

birthday_file_path = '100daysofcodePython/d31-d45/d32/birthdays.csv'
my_email = 'abc@gmail.com'
password = 'dllkdadsagenccxn'
to_email = ''

today = dt.datetime.now()
data = pd.read_csv(filepath_or_buffer=birthday_file_path)
text = ''

for (index, Series) in data.iterrows():
    if today.day == Series.day and today.month == Series.month:
        random_number = random.randint(1,3)
        letter_path = f'100daysofcodePython/d31-d45/d32/letter_templates/letter_{random_number}.txt'
        with open(letter_path, mode='r') as f:
            text = f.read()
            text = text.replace('[NAME]', Series['name'])
        to_email = Series['email']
                
        with smtplib.SMTP('smtp.gmail.com') as connection:
            connection.starttls()
            connection.login(user=my_email, password=password)
            connection.sendmail(
                from_addr=my_email,
                to_addrs=to_email,
                msg=text
            )
