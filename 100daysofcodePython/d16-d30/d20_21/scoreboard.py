from turtle import Turtle

class Scoreboard(Turtle):
    def __init__(self, shape = "classic", undobuffersize = 1000, visible = True):
        super().__init__(shape, undobuffersize, visible)
        
        self.color('white')
        self.hideturtle()
        self.score = 0
        
        self.penup()
        self.goto(x=0, y=270)
        self.update_scoreboard()
     
    def update_scoreboard(self): 
        self.write(arg=f'Score: {self.score}', align='center', font=('Ariel', 24, 'normal'))
    
    def increase_score(self):
        self.score += 1
        self.clear()
        self.update_scoreboard()
        
    def game_over(self):
        self.goto(0,0)
        self.write(arg=f'Game Over', align='center', font=('Ariel', 24, 'normal'))
        
        
        