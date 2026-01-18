from turtle import Turtle
import random
import turtle

turtle.write(arg='Hello', align='center')


class Food(Turtle):
    def __init__(self):
        super().__init__()
        self.shape('circle')
        self.penup()
        self.shapesize(stretch_wid=0.5, stretch_len=0.5, outline=None)
        self.color('blue')
        self.speed('fastest')
        self.refresh()
    
    def refresh(self):
        self.goto(x=random.randint(-290,290), y=random.randint(-290,290))
        