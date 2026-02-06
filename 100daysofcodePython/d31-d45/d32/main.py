import smtplib
import datetime as dt
import random

quotes_path = '100daysofcodePython/d31-d45/d32/quotes.txt'
my_email = 'abc@gmail.com'
password = 'pwd123'

"""return n number of quotes. Returns list."""
def get_quotes(n):
    try:
        with open(quotes_path, mode='r') as f:
            quotes = f.readlines()
            result_quotes = []
            for _ in range(n):
                random_index = random.randint(0, len(quotes) - 1)
                result_quotes.append(quotes.pop(random_index))
            return result_quotes
    except FileNotFoundError:
        print('No File Found!')
        

if dt.datetime.weekday == 4:
    with smtplib.SMTP('smtp.gmail.com') as connection:
        connection.starttls()
        connection.login(user=my_email, password=password)
        msg = ''
        for item in get_quotes(2):    
            msg += item
        connection.sendmail(
            from_addr=my_email,
            to_addrs='xyz@iiit-bh.ac.in',
            msg=msg
            # msg='Hello'
        )


print(get_quotes(1))