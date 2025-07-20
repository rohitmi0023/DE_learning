class Perso:
    hands = 2

    def __init__(self):
        print("PERSON CONSTRUCTOR")
    h2 = 23
    def hi(self):
        print("hi")

class Human(Person):
    def __init__(self):
        super().__init__()
        super().hi()
        print("HUMAN CONSTRUCTOR")


    def say(self): 
        print("skjhdjf")

    def __str__(self):
        return "THIS IS A HUMAN CLASS"

    def __repr__(self):
        return "HELLO"

obj = Human()
print(obj)
Human.h2 = 22
print(Human.h2)
print(str(obj))
# obj.say()
# obj.hi()

# %%
# lists = [1, 2]
# print(id(lists))
# print(id(lists[0]))
# lists[0] = 11
# print(id(lists))
lang = 'Python'
print(id(lang))
lang = 'Scala'
print(id(lang))

id = 'String'
print(id)
# %%
var = 10
def funct() -> None:
    """Sampple Docstring"""
    global var
    var = 20
    # return var
funct()
print(var)
funct.__doc__
help(funct)
funct.__annotations__

