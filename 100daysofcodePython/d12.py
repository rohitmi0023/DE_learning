# Topic - Scope and Number guessing game
import random
# Prime Number Checker
def is_prime(num):
    if num == 1:
        return True
    half = round(num/2)
    for i in range(2, half+1):
        if num%i == 0:
            return False
    return True
    
# print(is_prime(4))

########### GUESS THE NUMBER GAME
HARD_LEVEL_ATTEMPTS = 5
EASY_LEVEL_ATTEMPTS = 10

def banner():
    print('Welcome to the Number Guessing Game!')

def number_choice():
    print('I am thinking of a number between 1 and 100')
    return random.randint(1,100)

def difficulty_mode():
    game_mode = input('Choose a difficulty. Type \'easy\' or \'hard\': ')
    if game_mode == 'easy':
        return EASY_LEVEL_ATTEMPTS
    
    elif game_mode == 'hard':
        return HARD_LEVEL_ATTEMPTS
    
    else:
        print('Invalid option selected!!')
        return None

def check_anwer(guess_number, random_number):
    if guess_number == random_number:
        print('You guessed the correct number, Congratulations!!')
        return True
    elif not attempts:
        print('You ran out of attempts. Correct Number was', random_number)
        return True
    elif guess_number < random_number:
        print('Too Low.')
        return False
    else:
        print('Too high.')
        return False


banner()

random_number = number_choice()
print('Random Number, ', random_number)

attempts = difficulty_mode()

is_game_over = False

while attempts and not is_game_over:
    print(f'You have {attempts} attempts remaining to guess the number.')
    guess_number = int(input('Make a guess: '))
    is_game_over = check_anwer(guess_number, random_number)
    attempts -= 1
    if attempts == 0:
        print('You ran out of lives. Game Over!!😭')



"""
# FIRST APPROACH
random_number = random.randint(1,100)

print('Welcome to the Number Guessing Game!')
print('I am thinking of a number between 1 and 100')
game_mode = input('Choose a difficulty. Type \'easy\' or \'hard\': ')
attempts = 0
if game_mode == 'easy':
    attempts = 10
elif game_mode == 'hard':
    attempts = 5
else:
    print('Invalid option selected!!')

print(random_number)
while attempts:
    print(f'You have {attempts} attempts remaining to guess the number.')
    guess_number = int(input('Make a guess: '))
    attempts -= 1
    if guess_number == random_number:
        print('You guessed the correct number, Congratulations!!')
        break
    elif not attempts:
        print('You ran out of attempts. Correct Number was', random_number)
        break
    elif guess_number < random_number:
        print('Too Low.')
    else:
        print('Too high.')
    print('Guess again.')
    

"""