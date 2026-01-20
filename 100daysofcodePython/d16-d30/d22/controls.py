from turtle import Turtle

class Control(Turtle):
    def __init__(self, shape = "classic", undobuffersize = 1000, visible = True):
        super().__init__(shape, undobuffersize, visible)
        self.color('white')
        self.hideturtle()
        self.penup()
        self.goto(x=0, y=-275)
        
    def display_controls(self):
        self.write(arg='Press \'space\' to kick off the ball', align='center', font=('Ariel', 10, 'normal'))
        self.write(arg='Press \'w\', \'a\' to move the left paddle\n', align='center', font=('Ariel', 10, 'normal'))
        self.write(arg='Press \'up\', \'down\' to move the right paddle\n\n', align='center', font=('Ariel', 10, 'normal'))
        