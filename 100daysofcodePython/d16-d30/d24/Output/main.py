# breakdown the problem
# variable for invited names
# read starting letter and search for [Name]
# replace [Name] with invited names, store the new letter
# write to a new file

invited_names = []
PLACEHOLDER = '[Name]'

with open(file='./Input/Names/invited_names.txt', mode='r') as f:
    names = f.readlines()
    for name in names:
        # name = name.replace('\n', '')
        invited_names.append(name.strip())    

# with open(file='./Input/Letters/starting_letter.txt', mode='r') as f:
#     letter = f.readlines()
#     for name in invited_names:
#         new_letter = []
#         for line in letter:
#             line = line.replace(PLACEHOLDER,name)
#             new_letter.append(line)
        
#         file_name = f'letter_for_{name}.txt'    
#         with open(file = f'./Output/ReadyToSend/{file_name}', mode='w') as f2:
#             f2.writelines(new_letter)

with open(file='./Input/Letters/starting_letter.txt', mode='r') as f:
    letter = f.read()
    for name in invited_names:
        new_letter = letter.replace(PLACEHOLDER,name)        
        
        file_name = f'letter_for_{name}.txt'    
        with open(file = f'./Output/ReadyToSend/{file_name}', mode='w') as f2:
            f2.writelines(new_letter)