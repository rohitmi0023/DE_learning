import time
current_time = time.time()
print(current_time) # seconds since Jan 1st, 1970 

# Write your code below 👇

def speed_calc_decorator(function):
    def wrapper_fn():
        before_time = time.time()
        function()
        after_time = time.time()
        diff = after_time - before_time
        print(f'{function.__name__} run speed: {diff}')
    return wrapper_fn
      
@speed_calc_decorator
def fast_function():
    for i in range(1000000):
        i * i
        
@speed_calc_decorator
def slow_function():
    for i in range(10000000):
        i * i
        
fast_function()
slow_function()