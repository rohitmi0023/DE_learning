# %%
# Map
numbers = [1,2,3]
def squared(x):
    return x**2
mapped = map(squared, numbers)
listed = list(mapped)

# %%
# lambda
numbers3 = [1,1,2]
mapped2 = map(lambda x: {x:x**2}, numbers)
listed2 = list(mapped2)

mapped3 = map(lambda x: [x,x**2], numbers3)
listed3 = dict(mapped3)

# %%
# FlatMap

from itertools import chain

words = ["hello world", "python flatmap"]
split_words = list(map(lambda x: x.split(), words))
flat_words = list(chain.from_iterable(map(lambda x: x.split(), words)))

#%%
# Questions #1. output of list(map(lambda x: x * 2, [[1], [2]]))  # Hint: Not [2, 4]! 
ans1 = list(map(lambda x: x*2, [[1],[2]]))
# output-> [[1,1], [2,2]]
# x*2 duplicates the elements inside the sublists (list concatenation), not the sublists themselves
# [x] * 2 would duplicate the sublists themselves resulting in output [[[1],[1]], [[2],[2]]]

# %%
# Question #2. How would you use flatMap to convert ["a,b", "c,d"] to ["a", "b", "c", "d"]?
q2 = ["a,b", "c,d"]
from itertools import chain
ans2 = list(chain.from_iterable(map(lambda x: x.split(','), q2)))

# %%
# Question #3. In PySpark, when would you prefer map over flatMap for JSON data?
# map is to be prefered over flatMap when we want to preserve the structure




# %% Keyword Arguments

# Function Definitions -> collects arbitrary keyword arguments into a dictionary(**kwargs)
# Function Calls -> unpacks a dictionary into keyword arguments

# Common Use Cases
# 1. Configuring Functions
def setup_db(**config)-> int:
    host = config.get("host","localhost")
    print(f"host is {host}")

setup_db(host='aws.com', port=5432)

# 2. Wrapping Functions
def log_call(func, **kwargs):
    print(f"calling {func.__name__} with {kwargs}")

log_call(greet, name='Rohit',age=30)

# %%
sets = set(dic)
tuples = tuple(dic)
tuples2 = tuple(dic.values())

# %%
li = [13,3, 'dfd',['dgg']]
li2 = ['dg']
indexed = li.index(13)
added = li+li2

print(list({'name':'rohit'}.items()))
list()

def fib(stop):
    current_fib, next_fib = 0,1
    for _ in range(0,stop):
        fib_number = current_fib
        current_fib, next_fib = next_fib, current_fib+next_fib
        yield fib_number

fib(10)
list(fib(10))
[*fib(10)]

# list of square of first 10 integer numbers
[x**2 for x in range(1,11)]


li[:]
li[1:4:2]

digits = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
first_three = digits[slice(0, 3)]
last_three = digits[slice(-3,None)]
every_other = digits[slice(None, None, 2)]
digits[slice(7,11)]
everyy_other = every_other
id(everyy_other)
id(every_other)== id(everyy_other)

# %%
# shallow copy

alphabets = ['a','b','c']
copy_alph = alphabets[:]
alphabets.append('d')
id(alphabets) == id(copy_alph)
id(alphabets[0]) == id(copy_alph[0])
copyy_alph = alphabets.copy()
id(copyy_alph) == id(alphabets)
id_alphabets = id(alphabets) # ...6096
id_copyy_alph = id(copyy_alph) # ... 8848
id1 = id(alphabets[0]) # ...5264
id11 = id(copyy_alph[0]) # ...5264
alphabets[0] = 'A'
id1 = id(alphabets[0]) # ... 3728
id11 = id(copyy_alph[0]) # ... 5264

# %%
# Deep Copy

from copy import deepcopy
matrix = [[1,2,3],[4,5,6],[7,8,9]]
matrix_copy = deepcopy(matrix)
id(matrix) == id(matrix_copy)
id(matrix[0]) == id(matrix_copy[0])

alphs = ['z','y','x']
alphs_copy = deepcopy(alphs)
id(alphs) == id(alphs_copy) 
id(alphs[0]) == id(alphs_copy[0]) # because element is immutable

# %%
# Updating Values

numbers = [1, 2, 0, 0, 0, 0, 4, 5, 6, 7]
# print(id(numbers))
numbers[2:6] = [3]
numbers[2:1] = [3]
# print(id(numbers))

# %%
a = [10];
print(id(a))
a[0] = 2
print(id(a))

# %%
# Append
pets = ["cat", "dog"]
pets.append('fish')
# pets[len(pets):] = ['fish']

# Extend
fruits = ["apple", "pear", "peach"]
# fruits.extend(['orange','mango'])
fruits[len(fruits):] = ['orange','mango']
fruits

# Insert
letters = ['A', 'B', 'D']
# letters.insert(2, 'C')
letters[2:2] = ['C']

# %%
# Removing
# list.remove(item)
sample = [12, 11, 10, 42, 14, 12, 42]
sample.remove(12)

# pop([index])
returned = sample.pop(2)

sample.clear()

# %%
from sys import getsizeof

numbers = []
numbers_bytes = []
for value in range(100):
    print(getsizeof(numbers))
    numbers.append(value)
    numbers_bytes.append(getsizeof(numbers))


# %% Sets
lists = [1,2,2]
sets = set(lists)
sets2 = {1,2,3,3}

# %%
# Median Calculation

def median(lists):
    length = len(lists)
    middle_index = len(lists)//2
    sorted_lists = sorted(lists)
    if middle_index%2 == 1:
        return sorted_lists[middle_index]
    else:
        lower, upper = middle_index-1, middle_index + 1
        return sum(sorted_lists[lower:upper])/2

lists1 = [3,42,535,5]
median(lists1)

https://autodesk.wd1.myworkdayjobs.com/en-US/Ext/details/Data-Engineer_25WD88957-1?q=xuu

# %%

dicts = {"hello": 'World'}
dicts['hello']
dicts['new'] = 'Universe'
# print(dicts)
for x, y in dicts.items():
    print(x, y)


# %%

# Apply Functions

def apply_function(func, *args, **kwargs):
    """
    Applies a function with given positional and keyword arguments.
    
    Args:
        func: The function to apply.
        *args: Positional arguments for the function.
        **kwargs: Keyword arguments for the function.
    
    Returns:
        The result of the function call.
    """
    print("Applying function:", func.__name__)
    print("Positional arguments:", args)
    print("Keyword arguments:", kwargs)
    return func(*args, **kwargs)


# Example usage
def add(a, b):
    return a + b   

result = apply_function(add, a=5, b=3)
print(result)  # Output: 8


# %%

class CList:

    @property
    def hello(self):
        return "Hello, World!"

c = CList()
print(c.hello)
# %%
# what i am doing wrong here?

def logger(fun):
    def wrapper(*args, **kwargs):
        print(f"Calling function {fun.__name__} with arguments {args} and keyword arguments {kwargs}")
        val =  fun(*args, **kwargs)
        print('the result is:', val)
        return val
    return wrapper

@logger
def add(a, b):
    return a + b

# add(2,3)


# print(add(2, 3))

# %%
# generate a parameterized decorator
def parameterized_decorator(param):
    def decorator(func):
        print(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator

@parameterized_decorator('temp')
def multiply(a, b):
    return a * b

# print(dec)
# implementation of  

# %%
class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    @property
    def age(self):
        return self._age
    
    @age.setter
    def age(self, value):
        if value < 0:
            raise ValueError("Age cannot be negative")
        self._age = value


boy = Person('Rohit',20)
    
# %%
class Vector2:
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __add__(self, other):
        print("DO IT YOURSELF")
        # if isinstance(other, Vector2):
        #     return Vector2(self.x + other.x, self.y + other.y)
        # return NotImplemented

    def __repr__(self):
        return f"Vector2({self.x}, {self.y})"
    

a = Vector2(1, 2)
b = Vector2(3, 4)

c = a + b  # Using the overloaded __add__ method
print(c)  # Output: Vector2(4, 6)
print(type(c))  # Output: <class '__main__.Vector2'>

