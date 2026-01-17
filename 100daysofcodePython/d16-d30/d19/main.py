from turtle import Turtle, Screen

sup = Turtle()

def move_forward():
    sup.forward(50)

def move_backward():
    sup.backward(50)

def turn_left():
    sup.setheading(sup.heading() + 10)
    
def turn_right():
    sup.setheading(sup.heading() - 10)

def clear():
    sup.clear()
    sup.penup()
    sup.home()
    sup.pendown()

screen = Screen()

screen.listen()
screen.onkey(fun=move_forward, key='w')
screen.onkey(fun=move_backward, key='s')
screen.onkey(fun=turn_left, key='a')
screen.onkey(fun=turn_right, key='d')
screen.onkey(fun=clear, key='c')
screen.exitonclick()