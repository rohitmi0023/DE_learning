def decorator_fn(function):
    def wrapper_fn(*args):
        print('Starting calculating')
        return function(args[0], args[1])
    return wrapper_fn


@decorator_fn 
def add(a, b):
    return a+b

output = add(1,3)
print(output)

# Explanation
"""
1. decorator_fn is created in global scope
2. it takes add fn as argument with its defintion
3. deco fn creates wrapper_fn inside its scope
4. deco fn returns wrapper_fn
5. gloabl add fn has now living inside wrapper fn
"""
