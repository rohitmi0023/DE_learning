COLORS = ['red','orange','yellow','green','blue','purple']
STARTING_MOVE_DISTANCE = 5
MOVE_INCREMENT = 10

from turtle import Turtle
import random

max_x, min_x = 280, -280
max_y, min_y = 270, -250

class CarManager(Turtle):
    def __init__(self, position=None, shape = "square", undobuffersize = 1000, visible = True):
        super().__init__(shape, undobuffersize, visible)
        self.penup()
        if position is None:
            self.goto(random.randint(min_x, max_x), random.randint(min_y, max_y))
        else:
            self.goto(position)
        self.shapesize(stretch_wid=1, stretch_len=2)
        self.color(random.choice(COLORS))
        self.setheading(180)

    def move(self, level=0):
        self.forward(STARTING_MOVE_DISTANCE + (level-1)*MOVE_INCREMENT)
        