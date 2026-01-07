# DEBUGGING
# 1. Describe the Problem
# 2. Reproduce the Bug
# 3. Play Computer
# 4. Fix the error
# 5. Print Function
# 6. Debugger feature
# 7. Take a break
# 8. Ask a friend
# 9. Run often

# Target is the number up to which we count
def fizz_buzz(target):
    for number in range(1, target + 1):
        if number % 3 == 0 and number % 5 == 0:
            print("FizzBuzz")
        elif number % 3 == 0:
            print("Fizz")
        elif number % 5 == 0:
            print("Buzz")
        else:
            print(number)

fizz_buzz(20)