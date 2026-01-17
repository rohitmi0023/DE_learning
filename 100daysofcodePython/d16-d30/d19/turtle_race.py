from turtle import Turtle, Screen
import random

screen = Screen()
screen.setup(width=500, height=400)
user_bet = screen.textinput(title='Make your bet', prompt='Which color turtle will win the race?')


colors = ('red', 'orange', 'yellow', 'green', 'blue', 'purple')
x_co = -240
y_co = -100
turtles = []
for i in range(6):
    sup = Turtle(shape='turtle')
    sup.speed(8)
    sup.color(colors[i])
    sup.penup()
    sup.goto(x=x_co, y=y_co)
    y_co += 40
    turtles.append(sup)

is_race_on = bool(user_bet)
    
while is_race_on:
    for turtle in turtles:
        if turtle.xcor() < 230:
            space = random.randint(0, 10)
            turtle.forward(space)     
        else:
            winner = turtle.pencolor()
            is_race_on = False
            if winner == user_bet:
                print(f'Congrats, you\'ve won! Winner is {winner} color.')
            else:
                print(f'Bad luck, you\'ve lost! Winner is {winner} color.')
            
screen.exitonclick()