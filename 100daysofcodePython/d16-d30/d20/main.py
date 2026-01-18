from turtle import Turtle, Screen 
import time
from snake import Snake

screen = Screen()
screen.setup(width=600, height=600)
screen.bgcolor('black')
screen.title('My snake Game!')
screen.tracer(0)

game_is_on = True

snake = Snake()

screen.listen()
screen.onkey(fun=snake.move_up, key='Up')
screen.onkey(fun=snake.move_down, key='Down')
screen.onkey(fun=snake.move_left, key='Left')
screen.onkey(fun=snake.move_right, key='Right')

for _ in range(100):
    screen.update()
    time.sleep(0.1)
    snake.move()


screen.exitonclick()