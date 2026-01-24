from turtle import Turtle

with open(file='100daysofcodePython/d16-d30/d20_21/data.txt', mode='r') as f:
    score =f.read()

class Scoreboard(Turtle):
    def __init__(self, shape = "classic", undobuffersize = 1000, visible = True):
        super().__init__(shape, undobuffersize, visible)
        with open(file='100daysofcodePython/d16-d30/d20_21/data.txt', mode='r') as f:
            self.high_score = int(f.read())
        self.color('white')
        self.hideturtle()
        self.score = 0
        self.penup()
        self.goto(x=0, y=270)
        self.update_scoreboard()
     
    def update_scoreboard(self): 
        self.clear()
        self.write(arg=f'Score: {self.score} High Score: {self.high_score}', align='center', font=('Ariel', 24, 'normal'))
    
    def increase_score(self):
        self.score += 1
        self.update_scoreboard()
        
    def reset(self):
        if self.score > self.high_score:
            self.high_score = self.score
            with open(file='100daysofcodePython/d16-d30/d20_21/data.txt', mode='w') as f:
                f.write(str(self.high_score))
        self.score = 0
        self.update_scoreboard()
    
    # def game_over(self):
    #     self.goto(0,0)
    #     self.write(arg=f'Game Over', align='center', font=('Ariel', 24, 'normal'))
        
        
        