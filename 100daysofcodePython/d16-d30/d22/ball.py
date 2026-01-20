from turtle import Turtle

class Ball(Turtle):
    def __init__(self, shape = "circle", undobuffersize = 1000, visible = True):
        super().__init__(shape, undobuffersize, visible)
        self.color('white')
        self.penup()
        self.shapesize(stretch_wid=1, stretch_len=1)
        self.ball_in_play = False
        self.move_up = True
        self.move_down = False
        self.move_left = False 
        self.move_right = True
        self.move_speed = 1
    
    def start_game(self):
        self.ball_in_play = True
        
    def move(self):
        # detect collistion with wall
        if self.ycor() > 280:
            self.move_up = False
            self.move_down = True
        if self.ycor() < -280:
            self.move_up = True 
            self.move_down = False
        
        if self.move_up and self.move_right:
            self.new_x = self.xcor() + self.move_speed
            self.new_y = self.ycor() + self.move_speed
        elif self.move_down and self.move_right:
            self.new_x = self.xcor() + self.move_speed
            self.new_y = self.ycor() - self.move_speed 
        elif self.move_down and self.move_left:
            self.new_x = self.xcor() - self.move_speed
            self.new_y = self.ycor() - self.move_speed
        elif self.move_up and self.move_left:
            self.new_x = self.xcor() - self.move_speed
            self.new_y = self.ycor() + self.move_speed
        self.goto(x=self.new_x, y=self.new_y)
        
    def refresh(self):
        self.goto(0,0)
        self.ball_in_play = False
        self.move_up = True
        self.move_down = False
        self.move_left = not self.move_left 
        self.move_right = not self.move_right