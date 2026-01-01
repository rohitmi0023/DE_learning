# Capstone Project for d1 to d10- Blackjack Project
import random

# print('Do you want to play a game of Blackjack? Type \'y\' or \'n\': ')
def play_game():
    dealer_cards = []
    my_cards = []
    is_game_over = False

    def pick_card():
        """Returns a random card from the given list of cards"""
        cards = [11,1,2,3,4,5,6,7,8,9,10,10,10,10]
        card = random.choice(cards)
        return card

    def check_score(card_list):
        if sum(card_list) == 21 and len(card_list) == 2:
            return 0
            
        if sum(card_list) > 21 and 11 in card_list:
            card_list.remove(11)
            card_list.append(1)

        return sum(card_list)

    def display_final_scores(my_cards, dealer_cards):
        print(f'    Your final hand: {my_cards}, final score: {check_score(my_cards)}')
        print(f'    Computer\'s final hand: {dealer_cards}, final score: {check_score(dealer_cards)}')


    for _ in range(2):
        my_cards.append(pick_card())
        dealer_cards.append(pick_card())

    while not is_game_over:
        my_score = check_score(my_cards)
        dealer_score = check_score(dealer_cards)    
        print(f'    Your cards: {my_cards}, current score: {check_score(my_cards)}')
        print(f'    Computer\'s first card: {dealer_cards[0]}')
        if my_score == 0 or dealer_score == 0 or my_score > 21:
            is_game_over = True
        else:
            choice = input('Type \'y\' to get another card, type \'n\' to pass: ')
            if choice == 'y':
                my_cards.append(pick_card())
            else:
                is_game_over = True


    while dealer_score != 0 and dealer_score < 17:
        dealer_cards.append(pick_card())
        dealer_score = check_score(dealer_cards)


    def compare(my_score, dealer_score):
        if my_score == dealer_score:
            return 'Draw!!🙈'
        elif dealer_score == 0:
            return 'You lose!! Opponent has Blackjack!!'
        elif my_score == 0:
            return 'You win with a Blackjack!!'
        elif my_score > 21:
            return 'You went over 21, you lose!'
        elif dealer_score > 21:
            return 'You win, opponent went over 21'
        elif my_score > dealer_score:
            return 'You win!!'
        else:
            return 'You lose!!'

    display_final_scores(my_cards, dealer_cards)
    result = compare(my_score, dealer_score)
    print(result)


 
while input('Do you want to play another game of blackjack? Type \'y\' or \'n\': ') == 'y':
    print('\n'*20)
    play_game()