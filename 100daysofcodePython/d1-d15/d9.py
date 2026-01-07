# Method 1
bidders = {'names': [], 'amounts': []}


bidders['names'].append('Rohit')
bidders['amounts'].append(100)

bidders['names'].append('Nitish')
bidders['amounts'].append(1000)

max_bidding_amount = max(bidders['amounts'])

max_amount_index  = bidders['amounts'].index(max_bidding_amount)

winner_name = bidders['names'][max_amount_index]
winner_amount = bidders['amounts'][max_amount_index]

print(f'Highest bidder is {winner_name} with a amount of ${winner_amount}')

# does not handle equal amount bids-  gives the first person name only



