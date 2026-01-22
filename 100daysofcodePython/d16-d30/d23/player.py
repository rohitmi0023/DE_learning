STARTING_POSITION = (0, -280)
MOVE_DISTANCE = 10
FINISH_LINE_Y = 280

from turtle import Turtle

class Player(Turtle):
    def __init__(self, shape = "turtle", undobuffersize = 1000, visible = True):
        super().__init__(shape, undobuffersize, visible)
        self.color('black')
        self.penup()
        self.goto(STARTING_POSITION)
        self.left(90)
        
    def move_up(self):
        self.forward(MOVE_DISTANCE)
        
    def reset(self):
        self.goto(STARTING_POSITION)