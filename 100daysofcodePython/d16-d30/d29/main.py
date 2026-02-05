from tkinter import *
from tkinter import messagebox
import pyperclip
import json

image_path = '100daysofcodePython/d16-d30/d29/logo.png'
file_path = '100daysofcodePython/d16-d30/d29/file.txt'
json_file_path = '100daysofcodePython/d16-d30/d29/json_file.json'

# ---------------------------- PASSWORD GENERATOR ------------------------------- #
import random

def generate_pwd():
    letters = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']
    numbers = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
    symbols = ['!', '#', '$', '%', '&', '(', ')', '*', '+']

    nr_letters = random.randint(8, 10)
    nr_symbols = random.randint(2, 4)
    nr_numbers = random.randint(2, 4)

    password_list = [random.choice(letters) for _ in range(nr_letters)]
    password_list += [random.choice(symbols) for _ in range(nr_symbols)]
    password_list += [random.choice(numbers) for _ in range(nr_numbers)]

    random.shuffle(password_list)

    password = ''.join(password_list)
    password_entry.delete(first=0, last=END)
    password_entry.insert(0,string=password)
    pyperclip.copy(password_entry.get())

# ---------------------------- SAVE PASSWORD ------------------------------- #
def save_pwd():
    website_detail = website_entry.get()
    email_detail = email_entry.get()
    password_detail = password_entry.get()
    new_data = {
        website_detail: {
            'email': email_detail,
            'password': password_detail
        }
    }
    if len(website_detail)==0 or len(password_detail)==0:
        messagebox.showinfo(title='Info', message='Please don\'t leave any fields empty!!')
        
    else:
        is_ok = messagebox.askokcancel(title=website_detail, message=f'Details: \nEmail:{email_detail} \nPassword: {password_detail}')

        if is_ok:
            try:
                with open(file=json_file_path, mode='r') as f:
                    # loading existing data into dict
                    data = json.load(f)
            except FileNotFoundError:
                with open(file=json_file_path, mode='w') as f:
                    # writing new dictionary data to json file
                    json.dump(obj=new_data, fp=f, indent=4)
            else:
                with open(file=json_file_path, mode='w') as f:
                    # updating existing dictionary data with new data
                    data.update(new_data)
                    # writing new dictionary data to json file
                    json.dump(obj=data, fp=f, indent=4)
            finally:
                website_entry.delete(0, END)
                password_entry.delete(0, END)


# ---------------------------- LOOKUP WEBSITES ------------------------------- #    
def find_website():
    text = website_entry.get()
    try:
        with open(file=json_file_path, mode='r') as f:
            data = json.load(fp=f)
    except FileNotFoundError:
        messagebox.showinfo(title='Info', message=f'No Data File Found')
    else:
        if text in data: # searches for text in data keys
            email = data[text]['email']
            pwd = data[text]['password']
            messagebox.showinfo(title=text, message=f'Email: {email}\n Password: {pwd}')
        else:
            messagebox.showinfo(title='Info', message=f'No details for "{text}" exist')
        

# ---------------------------- UI SETUP ------------------------------- #

window = Tk()
window.title('Password Manager')
window.configure(bg='white', padx=60, pady=60)
window.tk_setPalette(background='white')


canvas = Canvas(width=200, height=200)
logo_image = PhotoImage(file=image_path)

canvas.create_image(100, 100, image=logo_image)
canvas.grid(column=1, row=0)

website_label = Label(text='Website: ')
website_label.grid(row=1, column=0)

website_entry = Entry(width=20)
website_entry.grid(row=1,column=1)
website_entry.focus()

website_search_button = Button(text='Search', width=12, command=find_website)
website_search_button.grid(row=1, column=2)

email_label = Label(text='Email/Username: ')
email_label.grid(row=2, column=0)

email_entry = Entry(width=35)
email_entry.insert(0, 'rohitracer0023@gmail.com')
email_entry.grid(row=2,column=1, columnspan=2)


password_label = Label(text='Password: ')
password_label.grid(row=3, column=0)

password_entry = Entry(width=19)
password_entry.grid(row=3,column=1)


generate_pwd_button = Button(text='Generate Password', width=11, command=generate_pwd)
generate_pwd_button.grid(row=3, column=2)


add_button = Button(text='Add', width=33, command=save_pwd)
add_button.grid(row=4, column=1, columnspan=2)

window.mainloop()
