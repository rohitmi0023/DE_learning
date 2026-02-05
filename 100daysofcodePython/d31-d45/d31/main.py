from tkinter import Tk, Canvas, PhotoImage, Button
import pandas as pd
import random

BACKGROUND_COLOR = "#B1DDC6"
card_front_path = '100daysofcodePython/d31-d45/d31/images/card_front.png'
card_back_path = '100daysofcodePython/d31-d45/d31/images/card_back.png'
known_img_path = '100daysofcodePython/d31-d45/d31/images/right.png'
unknown_img_path = '100daysofcodePython/d31-d45/d31/images/wrong.png'
french_words_csv_path = '100daysofcodePython/d31-d45/d31/data/french_words.csv'
words_to_learn_path = '100daysofcodePython/d31-d45/d31/data/words_to_learn.csv'


# -----------------------SAVE PROGRESS-------------------------------#
def save_progress():
    global list, word
    list.remove(word)
    df = pd.DataFrame(list)
    df.to_csv(path_or_buf=words_to_learn_path, index=False)
    next_card() 
    


# ----------------------GENERATE NEW WORD---------------------------#
try:
    data = pd.read_csv(filepath_or_buffer=words_to_learn_path)
except FileNotFoundError:
    original_data = pd.read_csv(filepath_or_buffer=french_words_csv_path)
    list = original_data.to_dict(orient='records')
else:
    list = data.to_dict(orient='records')

word = {}
french_word = ''
english_word = ''

def next_card():
    global word, french_word, english_word, flip_timer
    window.after_cancel(flip_timer)
    word = random.choice(list)
    french_word = word['French']
    english_word = word['English']
    canvas.itemconfig(canvas_image, image=card_front_image)
    canvas.itemconfig(card_title, text='French', fill='black')
    canvas.itemconfig(card_word, text=french_word, fill='black')
    flip_timer = window.after(3000, display_back, english_word)
    

    
# ----------------------DISPLAY BACK CARD---------------------------#
def display_back(back_word):
    canvas.itemconfig(canvas_image, image=card_back_image)
    canvas.itemconfig(card_title, text='English', fill='white')
    canvas.itemconfig(card_word, text=back_word, fill='white')



# ----------------------UI SETUP---------------------------#
window = Tk()

window.title('Flashy')
window.configure(bg=BACKGROUND_COLOR, padx=50, pady=50, highlightthickness=0)
window.tk_setPalette(background=BACKGROUND_COLOR)

flip_timer = window.after(3000, display_back, english_word)

canvas = Canvas(width=800, height=526, bg=BACKGROUND_COLOR, highlightthickness=0)
card_front_image = PhotoImage(file=card_front_path)
card_back_image = PhotoImage(file=card_back_path)

# image on the centre of canvas
canvas_image = canvas.create_image(400, 263, image=card_front_image)
card_title = canvas.create_text(400, 150, text='', font=('Ariel', 40, 'italic'), fill='black')
card_word = canvas.create_text(400, 263, text='', font=('Ariel', 60, 'bold'), fill='black')
canvas.grid(column=0, row=0, columnspan=2)

unknown_image = PhotoImage(file=unknown_img_path)
unknown_button = Button(image=unknown_image, highlightthickness=0,command=next_card)
unknown_button.grid(column=0, row=1)

known_image = PhotoImage(file=known_img_path)
known_button = Button(image=known_image, highlightthickness=0,command=save_progress)
known_button.grid(column=1, row=1)


next_card()

window.mainloop()