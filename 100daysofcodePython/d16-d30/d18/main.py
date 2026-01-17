import turtle as t
from turtle import Screen
import colorgram
import random

# rgb_colors = []
# colors = colorgram.extract('100daysofcodePython/d16-d30/d18/image.jpg', 30)
# for color in colors:
#     r = color.rgb.r
#     g = color.rgb.g
#     b = color.rgb.b
#     new_color = (r,g,b)
#     rgb_colors.append(new_color)

# print(rgb_colors)
color_list = [(202, 164, 110), (240, 245, 241), (236, 239, 243), (149, 75, 50), (222, 201, 136), (53, 93, 123), (170, 154, 41), (138, 31, 20), (134, 163, 184), (197, 92, 73), (47, 121, 86), (73, 43, 35), (145, 178, 149), (14, 98, 70), (232, 176, 165), (160, 142, 158), (54, 45, 50), (101, 75, 77), (183, 205, 171), (36, 60, 74), (19, 86, 89), (82, 148, 129), (147, 17, 19), (27, 68, 102), (12, 70, 64), (107, 127, 153), (176, 192, 208), (168, 99, 102)]

space = 50
size = 20
length = 10
breadth = 10

t.colormode(255)
sup = t.Turtle()
sup.color('purple')
sup.teleport(-200,-100)

for y in range(10):
    for x in range(10):
        sup.speed('fast')
        sup.dot(size, random.choice(color_list))
        sup.penup()
        if x != 9:
            sup.forward(space)
    if y != 9:
        sup.speed('fastest')
        sup.left(90)
        sup.penup()
        sup.forward(space)
        sup.left(90)
        sup.forward(space*9)
        sup.right(180)

screen = Screen()
screen.delay(10000)
screen.exitonclick()