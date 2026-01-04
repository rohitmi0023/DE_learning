# Problem Solving Strategy
# 1. Breakdown the problem
# 2. Make a todo list of problems
# 3. Turn the problem into comments
# 4. start solving easier task first - Write Code -> Run Code -> Fix Code -> Write Code -> ... 
# 5. Solve Next task

from game_data import data
import random
import art

# Task: Display Banner 
print(art.logo)

# Task: play the game till its over
is_game_over = False

# Task: keep counter for score
score = 0

# Task: create object selector
def pick_object():
    """picks, displays an object from data. Returns object as dictionary"""
    total_objects = len(data) - 1
    scanned_number = random.randint(0, total_objects)
    obj = data.pop(scanned_number)
    return obj


def display_obj1(obj1):
    obj1_name, obj1_desc, obj1_country, obj1_followers = obj1['name'], obj1['description'], obj1['country'], obj1['follower_count']
    print(f'Compare A: {obj1_name}, a {obj1_desc}, from {obj1_country}')    


def display_obj2(obj2):
    obj2_name, obj2_desc, obj2_country, obj2_followers = obj2['name'], obj2['description'], obj2['country'], obj2['follower_count']
    print(art.vs)
    print(f'Against B: {obj2_name}, a {obj2_desc}, from {obj2_country}')


# Task: ask user for answer
def user_guess():
    choice = input('Who has more followers? Type \'A\' or \'B\': ')
    return choice


# Task: if correct, score += 1 else same score. If invalid choice then return -1
def check_answer(choice, obj1_followers, obj2_followers):
    print(choice, obj1_followers, obj2_followers)
    print(choice)
    if obj1_followers > obj2_followers and choice == 'A':
        return 1
    elif obj2_followers > obj1_followers and choice == 'B':
        return 1
    elif choice != 'A' and choice != 'B':
        return -1
    else:
        return 0


# Task: for next round, make compare 1 to 2nd object and fetch compare 2 data
def next_round(obj2):
    obj1 = obj2
    obj2 = pick_object()
    return obj1, obj2


while not is_game_over:

    if score == 0:
        obj1 = pick_object()
        display_obj1(obj1)

        obj2 = pick_object()
        display_obj2(obj2)

    else:
        obj1, obj2 = next_round(obj2)
        display_obj1(obj1)
        display_obj2(obj2)

    choice = user_guess()

    points = check_answer(choice, obj1['follower_count'], obj2['follower_count'])

    score += points

    if points == -1:
        is_game_over = True
        print('You made an invalid choice! Final score: ', score)
        score = -1
    elif points == 0:
        print('Sorry, that\'s wrong. Final score: ', score)
        is_game_over = True
    else:
        print('You are right! Current score: ', score)    

# Task: if score == length-1 of data, then make game over and congratulate
    if len(data) == 0:
        print('Congratulations, you have won the game!!🏆')
        is_game_over = True


