from turtle import Turtle, Screen

class Paddle(Turtle):
    def __init__(self, position , shape = "square", undobuffersize = 1000, visible = True):
        super().__init__(shape, undobuffersize, visible)
        self.color('white')
        self.shapesize(stretch_wid=5,stretch_len=1)
        self.penup()
        self.goto(position)
    
    def move_up(self):
        new_y = self.ycor() + 20
        self.goto(x=self.xcor(), y=new_y)
    
    def move_down(self):
        new_y = self.ycor() - 20
        self.goto(x=self.xcor(), y=new_y)

    def control(self, up, down, screen):
        screen.onkey(fun=self.move_up, key=up)
        screen.onkey(fun=self.move_down, key=down)