FONT = ('Courier', 24, 'normal')

from turtle import Turtle

class Scoreboard(Turtle):
    def __init__(self, shape = "classic", undobuffersize = 1000, visible = True):
        super().__init__(shape, undobuffersize, visible)
        self.hideturtle()
        self.penup()
        self.goto(-230,270)
        self.level = 1
    
    def display(self):
        self.clear()
        self.write(arg=f'Level: {self.level}', align='center',font=FONT)
        
    def game_over(self):
        self.goto(0,0)
        self.write(arg=f'Game Over!', align='center',font=('Courier', 34, 'normal'))