from turtle import Turtle

class Scoreboard(Turtle):
    def __init__(self, shape = "classic", undobuffersize = 1000, visible = True):
        super().__init__(shape, undobuffersize, visible)
        self.hideturtle()
        self.color('white')
        self.penup()
        self.l_score = 0
        self.r_score = 0
        self.display_score()
        
    def score_update(self, ball_object):
        if ball_object.xcor() > 0:
            self.l_score += 1
        else:
            self.r_score += 1
            
    def display_score(self):
        self.clear()
        self.goto(-100,225)
        self.write(arg=self.l_score, align='center', font=('Courier', 80, 'bold'))
        self.goto(100,225)
        self.write(arg=self.r_score, align='center', font=('Courier', 80, 'bold'))