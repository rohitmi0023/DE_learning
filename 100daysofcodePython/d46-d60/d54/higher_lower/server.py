from flask import Flask
import random

app = Flask(__name__)

random_num = random.randint(0,9)

@app.route('/')
def home_page():
    content = '<h1> Guess a number between 0 and 9 </h1>'\
    '<img src="https://media1.giphy.com/media/v1.Y2lkPTc5MGI3NjExbGJyeGlvYnBhb3ZrdHYycnZoNG4yNXJxeTJlZjlhN3duYjJkbHZsdyZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/MFsqcBSoOKPbjtmvWz/giphy.gif" alt="Lots of Money Holding Kid">'
    return content

@app.route('/<int:random_num_page>')
def random_num_page(random_num_page):
    color1 = random.randint(0,255)
    color2 = random.randint(0,255)
    color3 = random.randint(0,255)
    if random_num_page > random_num:
        return f'<h1 style="color:rgb({color1},{color2},{color3})"> Too High! </h1>'\
        '<img src="https://media.giphy.com/media/3o6ZtaO9BZHcOjmErm/giphy.gif" alt="Dog Flying High">'
    elif random_num_page < random_num:
        return f'<h1 style="color:rgb({color1},{color2},{color3}> Too Low! </h1>'\
        '<img src="https://media.giphy.com/media/jD4DwBtqPXRXa/giphy.gif" alt="Dog Digging Ground">'
    else:
        return f"<h1 style='color:rgb({color1},{color2},{color3}'>You found me!</h1>" \
               "<img src='https://media.giphy.com/media/4T7e4DmcrP9du/giphy.gif'/>"


if __name__ == '__main__':
    app.run(debug=True)
    
    