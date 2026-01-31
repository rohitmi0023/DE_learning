import pandas as pd
nato_file_path = '100daysofcodePython/d16-d30/d26/nato_phonetic_alphabet.csv'

df = pd.read_csv(filepath_or_buffer=nato_file_path)

data_dict = {row.letter:row.code for (index, row) in df.iterrows()}

# print(data_dict)

word = str(input('Enter a word: '))

output_list = [data_dict[char.upper()] for char in word]

print(output_list)


# Coding Exercise
# sentence = "What is the Airspeed Velocity of an Unladen Swallow?"
# words = sentence.split(' ')
# result = {word:len(word) for word in words}
# print(result)

