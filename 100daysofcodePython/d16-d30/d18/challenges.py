from turtle import Screen
import random
import turtle as t

# config
t.colormode(255)
sup = t.Turtle()
sup.shape('turtle')
sup.color('purple')
# sup.pensize(15)
# sup.left(50)

color_names = [
    "aliceblue", "aquamarine", "azure", "beige", "bisque",
    "black", "blue", "blueviolet", "brown", "burlywood",
    "cadetblue", "chartreuse", "chocolate", "coral", "cornflowerblue",
    "crimson", "cyan", "darkblue", "darkcyan", "darkgoldenrod",
    "darkgray", "darkgreen", "darkkhaki", "darkmagenta", "darkolivegreen",
    "darkorange", "darkorchid", "darkred", "darksalmon", "darkseagreen"
]

def random_color():
    red = random.randint(0,255)
    green = random.randint(0,255)
    blue = random.randint(0,255)
    return (red, green, blue)

# square
def draw_square():
    for _ in range(4):
        sup.forward(100)
        sup.right(90)

# dashed line
def draw_dashes():
    dash_length = 20
    space_length = 10
    number_of_dash = 15
    for _ in range(number_of_dash):
        sup.pendown()
        sup.forward(dash_length)
        sup.penup()
        sup.forward(space_length)


# triangle to decagon
def draw_shapes():    
    starting_side = 3
    max_sides = 10
    dash_length = 150

    for side in range(starting_side,max_sides+1):
        angle = 360/side
        for _ in range(side):
            sup.color(random.choice(color_names))
            sup.forward(dash_length)
            sup.right(angle)
            
          
# random walk
def random_walk():
    directions = [0,90,180,270]
    sup.speed('fastest')
    for _ in range(100):
        sup.color(random_color())
        sup.forward(30)
        sup.setheading(random.choice(directions))
        
def draw_circles():
    sup.shape('arrow')
    sup.speed('fastest')
    for i in range(0,360,5):
        sup.color(random_color())
        sup.circle(100)
        sup.setheading(i)

# draw_shapes()
# random_walk()
draw_circles()
    
screen = Screen()
screen.exitonclick()