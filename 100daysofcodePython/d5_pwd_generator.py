#Password Generator Project
# Topics - Loops, random()
import random
letters = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']
numbers = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
symbols = ['!', '#', '$', '%', '&', '(', ')', '*', '+']

print("Welcome to the PyPassword Generator!")
nr_letters = 3 #int(input("How many letters would you like in your password?\n")) 
nr_symbols = 3 #int(input(f"How many symbols would you like?\n"))
nr_numbers = 2 #int(input(f"How many numbers would you like?\n"))

#Eazy Level - Order not randomised:
#e.g. 4 letter, 2 symbol, 2 number = JduE&!91

pwd = ''

for i in range(1, nr_letters+1):
    str = random.choice(letters)
    pwd += str

for i in range(1, nr_symbols+1):
    str = random.choice(symbols)
    pwd += str

for i in range(1,nr_numbers+1):
    str = random.choice(numbers)
    pwd += str

print('Easy Level password', pwd)

"""
#Hard Level - Order of characters randomised:
#e.g. 4 letter, 2 symbol, 2 number = g^2jk8&P
pwd2 = ''

while nr_letters > 0 or nr_symbols > 0 or nr_numbers > 0:
    rand = random.randint(1,3)
    if rand == 1 and nr_letters > 0:
        # for i in range(1, nr_letters+1):
        str = random.choice(letters)
        pwd2 += str
        nr_letters -= 1
    if rand == 2 and nr_symbols > 0:
        # for i in range(1, nr_symbols+1):
        str = random.choice(symbols)
        pwd2 += str
        nr_symbols -= 1
    if rand == 3 and nr_numbers > 0:
        # for i in range(1,nr_numbers+1):
        str = random.choice(numbers)
        pwd2 += str
        nr_numbers -= 1

print('Hard level password', pwd2)
"""

# Editor's solution - add characters in a list -> Shuffle list -> make string

pwd3 = ''
pwd3list = []

for i in range(1, nr_letters+1):
    str = random.choice(letters)
    pwd3list.append(str)

for i in range(1, nr_symbols+1):
    str = random.choice(symbols)
    pwd3list.append(str)

for i in range(1,nr_numbers+1):
    str = random.choice(numbers)
    pwd3list.append(str)

random.shuffle(pwd3list)

for char in pwd3list:
    pwd3 += char
print('Editors Hard Level password', pwd3)
