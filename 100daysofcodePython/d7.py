import random

word_list = ['cat', 'mouse', 'lion']

chosen_word = random.choice(word_list)
print(chosen_word)

print('_'*len(chosen_word))

guessed_list = []

game_over = False

lives = 2

while not game_over and lives:
    guess = input('Guess a letter: ').lower()
    display = ''
    guessed_list.append(guess)
    for char in chosen_word:
        if char in guessed_list:
            display += char
        else:
            display += '_'
    if guess not in chosen_word:
        lives -= 1
        if lives == 0:
            print('You Lose!')
            break
        print('Lives left: ', lives)
    if display == chosen_word:
        game_over = True
    print(display)


