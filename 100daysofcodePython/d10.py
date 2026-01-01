#  Calculator project
# Learning - Function with return feature
def input1():
    num1 = float(input('What\'s the first number?: '))
    return num1

def operations():
    print('+')
    print('-')
    print('*')
    print('/')
    operation = input('Pick an operation: ')
    return operation

def input2():
    num2 = float(input('What\'s the next number?: '))
    return num2

def calculator(num1,num2, operation):
    if operation == '+':
        result = num1 + num2
        print(num1, '+', num2, '=', result)
    elif operation == '-':
        result = num1 - num2
        print(num1, '-', num2, '=', result)
    elif operation == '*':
        result = num1*num2
        print(num1, '*', num2, '=', result)
    elif operation == '/':
        if num2 == 0:
            print('Cannot divide by 0!!')
            return None
        result = num1/num2
        print(num1, '/', num2, '=', result)
    else:
        print('Not a valid operation!!')
        return None
    return result


counter = 0
while True:
    if counter == 0:
        result = calculator(input1(),input2(),operations())
        counter += 1
    else:
        command = input(f'Type \'y\' to continue calculating with {result}, or type \'n\' to start a new operation: ')
        if command == 'n':
            result = calculator(input1(),input2(), operations())
        elif command == 'y':
            result = calculator(result, input2(), operations())
    if result is None:
        break