from turtle import Turtle, Screen
import turtle
import pandas as pd

image_path = '100daysofcodePython/d16-d30/d25/blank_states_img.gif'
states_csv_path = '100daysofcodePython/d16-d30/d25/50_states.csv'
output_csv_path = '100daysofcodePython/d16-d30/d25/states_to_learn.csv'

data = pd.read_csv(filepath_or_buffer=states_csv_path)
states_list = data.state.to_list()

screen = Screen()
screen.title('U.S. States Quiz')

screen.addshape(image_path)

turtle.shape(image_path)


def get_mouse_click_coor(x, y):
    print(x,y)
    

turtle.onscreenclick(get_mouse_click_coor)

game_is_on = True

correct_guesses = []

while game_is_on:
    # display banner
    answer_state = turtle.textinput(title=f'{len(correct_guesses)}/50 States Correct', prompt='What\'s another State name?')
    if answer_state:
        answer_state = answer_state.title()
    
    #  check answer
    if answer_state == 'Exit':
        game_is_on = False
        
    # elif len(data[data['state'] == answer_state]):
    elif answer_state in states_list:
        t = Turtle()
        t.penup()
        t.hideturtle()

        state_data = data[data['state'] == answer_state]
        x = state_data['x'].item()
        y = state_data['y'].item()
        # dict = data[data['state'] == answer_state].set_index(keys='state').to_dict()
        # x = dict['x'][answer_state]
        # y = dict['y'][answer_state]
        
        t.goto(x=x, y=y)
        t.write(arg=answer_state, font=('Arial', 10, 'normal'))
        correct_guesses.append(answer_state)
        if len(correct_guesses) == 50:
            game_is_on = False

missed_states = []

for state in states_list:
    if state not in correct_guesses:
        missed_states.append(state)
   
dict = {
    'state': missed_states
}

df = pd.DataFrame(dict)
df.to_csv(output_csv_path)