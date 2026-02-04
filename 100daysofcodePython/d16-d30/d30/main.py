# Keyword Method with iterrows()
# {new_key:new_value for (index, row) in df.iterrows()}

import pandas

nato_file_path = '100daysofcodePython/d16-d30/d26/nato_phonetic_alphabet.csv'

data = pandas.read_csv(filepath_or_buffer=nato_file_path)


phonetic_dict = {row.letter: row.code for (index, row) in data.iterrows()}
print(phonetic_dict)

# while True:
#     try:
#         word = input("Enter a word: ").upper()
#         output_list = [phonetic_dict[letter] for letter in word]
#     except KeyError:
#         print('Sorry, only letters in the alphabet please.')
#     else:
#         print(output_list)  
#         break

def generate_phoenetic():
    word = input("Enter a word: ").upper()
    try:
        output_list = [phonetic_dict[letter] for letter in word]
    except KeyError:
        print('Sorry, only letters in the alphabet please.')
        generate_phoenetic()
    else:
        print(output_list)              
        
generate_phoenetic()