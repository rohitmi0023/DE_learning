# breadown the problem
# generate turtle and move. Refresh once reached top -> Done
# generate 10 cars at range (+-250,+-250) coordinates -> Done. Extra is generate new cars from max_xcor and keep constant number of cars in screen
# detect collision turtle with cars. -> Done
# keep track of scorecard -> Done
# Keep track of levels and Increase after pass -> Done


import time
from turtle import Screen
from player import Player
from car_manager import CarManager
import random
from scoreboard import Scoreboard

REFRESH_SECONDS = 0.1
SCREEN_HEIGHT, SCREEN_WIDTH = 600, 600
FINISH_LINE = 280

screen = Screen()
screen.setup(width=SCREEN_WIDTH, height=SCREEN_HEIGHT)
screen.tracer(0)
screen.listen()

game_is_on = True

p1 = Player()

screen.onkey(fun=p1.move_up , key='Up')
cars = []

score = Scoreboard()

for _ in range(10):
    cars.append(CarManager())
    
while game_is_on:
    time.sleep(REFRESH_SECONDS)
    screen.update()
 
    score.display() 
    
    if len(cars) < 15:
        cars.append(CarManager(position=(290, random.randint(-230, 280))))
        
    if p1.ycor() > FINISH_LINE:
        p1.reset()
        score.level += 1

        
    for car in cars:
        
        if car.distance(p1) < 20:
            score.game_over()
            game_is_on = False
            
        car.move(score.level)
        if car.xcor() < -320:
            car.hideturtle()
            cars.remove(car)

screen.exitonclick()