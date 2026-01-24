from turtle import Turtle

STARTING_POSITIONS = [(0,0), (-20,0),(-40,0)]
MOVE_DISTANCE = 20
UP = 90
DOWN = 270
LEFT = 180
RIGHT = 0

class Snake:
    def __init__(self):
        self.segments = []
        self.create_snake()
        self.head = self.segments[0]
        # self.tail = self.segments[-1]
        
    def create_snake(self):
        for position in STARTING_POSITIONS:
            self.add_segment(position)
         
    def add_segment(self, position):
        new_segment = Turtle(shape='square')
        new_segment.color('white')
        new_segment.penup()
        new_segment.goto(position)
        self.segments.append(new_segment)     
    
    def extend(self):
        self.add_segment(self.segments[-1].position())
        
    def reset(self):
        for seg in self.segments:
            seg.goto(-2000,-2000)
        self.segments.clear()
        self.create_snake()
        self.head = self.segments[0]
        
    def move(self):        
        for seg_num in range(len(self.segments)-1, 0, -1):
            self.segments[seg_num].goto(self.segments[seg_num-1].xcor(), self.segments[seg_num-1].ycor())
        self.head.forward(MOVE_DISTANCE)

    def move_up(self):
        if self.head.heading() != DOWN:
            self.head.setheading(UP)
        
    def move_down(self):
        if self.head.heading() != UP: 
            self.head.setheading(DOWN)
        
    def move_left(self):
        if self.head.heading() != RIGHT:     
            self.head.setheading(LEFT)
        
    def move_right(self):
        if self.head.heading() != LEFT:     
            self.head.setheading(RIGHT)
    
    
        