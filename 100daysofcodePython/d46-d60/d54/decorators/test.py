def decorator_fn(function):
    def wrapper_fn(*args):
        print('Starting calculating')
        # Do something before actual function
        return function(args[0], args[1])
    return wrapper_fn


def add(a, b):
    return a+b

main_fn = decorator_fn(add)
output = main_fn(1,3)

print(output)

print('==============')

# def decorator_fn(function):
#     def wrapper_fn(a,b):
#         print('Starting calculating')
#         # Do something before actual function
#         return function(a,b)
#     return wrapper_fn

def decorator_fn(function):
    def wrapper_fn(*args):
        a, b = args
        print('Starting calculating')
        # Do something before actual function
        return function(a,b)
    return wrapper_fn
    

def add(a, b):
    return a+b

decorated_fn = decorator_fn(add)
output = decorated_fn(1,4)
print(output)