# breaking down the problem
# 1. create screen
# 2. create and move paddles
# 3. create another paddle
# 4. create the ball and make it move
# 5. detect collision with wall and bounce
# 6. detect collision with paddle
# 7. detect when paddle misses
# 8. create score tracker

from turtle import Turtle, Screen
import turtle
from paddle import Paddle
from ball import Ball
import time
from controls import Control
from scoreboard import Scoreboard

# screen config.
screen = Screen()
screen.bgcolor('black')
screen.setup(width=800, height=600) 
screen.title('Pong Game')


# create paddle on screen
screen.tracer(0)

r_paddle = Paddle(position=(350,0))
l_paddle = Paddle(position=(-350,0))

# move paddle on screen
r_paddle.control(up='Up', down='Down', screen=screen)
l_paddle.control(up='w', down='s', screen=screen)

screen.listen()

# screen.onkey(fun=r_paddle.move_up, key='Up')
# screen.onkey(fun=r_paddle.move_down, key='Down')

# screen.onkey(fun=l_paddle.move_up, key='w')
# screen.onkey(fun=l_paddle.move_down, key='s')


# create the ball and move it
ball = Ball()
control = Control()
score = Scoreboard()

control.display_controls()

game_is_on = True

screen.onkey(fun=ball.start_game, key='space')

while game_is_on:
    screen.update()
    if ball.ball_in_play:
        ball.move()
    
        # detect collision with paddles
        if ball.distance(r_paddle) < 50 and ball.xcor() > 320 or ball.distance(l_paddle) < 50 and ball.xcor() < -320:
            ball.move_left = not ball.move_left
            ball.move_right = not ball.move_right          
            ball.move_speed += 0.5

        # misses paddle and goes to hit side walls
        if ball.xcor() > 370 and ball.distance(r_paddle) >= 50 or ball.xcor() < -370 and ball.distance(l_paddle) >= 50:
            score.score_update(ball)
            ball.refresh()
            score.display_score()
        
    
    
screen.exitonclick()


