from tkinter import *


window = Tk() 
window.title('GUI Program')
window.minsize(width=500, height=300)

my_label = Label(text='Labelled')
my_label.grid(row=0, column=0)


def button_clicked():
    print('Clicked')
    text = input.get()
    my_label.config(text=text)


button1 = Button(text='Click Me', command=button_clicked)
button1.grid(row=1, column=2)


button2 = Button(text='New Button', command=button_clicked)
button2.grid(row=0, column=1)

input = Entry(width=10)
input.grid(row=2, column=3)



window.mainloop()

