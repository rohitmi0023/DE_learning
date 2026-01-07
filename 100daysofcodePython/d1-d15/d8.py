import time
# Postion argument vs keyword argument

def calculate_love_score(name1, name2):
    word1 = 'true'
    word2 = 'love'
    name1 = name1.lower()
    name2 = name2.lower()
    total1 = 0
    total2 = 0
    for char in word1:
        for char2 in name1:
            if char2 == char:
                total1 += 1
        for char2 in name2:
            if char2 == char:
                total1 +=1 
    for char in word2:
        for char2 in name1:
            if char2 == char:
                total2 += 1
        for char2 in name2:
            if char2 == char:
                total2 += 1 
    # print(total1)
    # print(total2)
    score = str(total1)+str(total2)
    print(score)

# calculate_love_score('Angela Yu','Jack Bauer')


alphabet = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u',
            'v', 'w', 'x', 'y', 'z']


# ip = input('Type \'encode\' to encrypt, type \'decode\' to decrypt:\n')

message = 'Cat'.lower()
print('Original message: ',message)

shift_number = 10

def encrypt(message, shift_number):
    print(f'Encrypting message: {message}....')  
    encrypted = ''
    # encrypted message
    for char in message:
        index = alphabet.index(char)
        shifted_index = index + shift_number
        shifted_index %= 26
        new_char = alphabet[shifted_index]
        encrypted += new_char
    # time.sleep(2)
    print(f'Message Encrypted Successfully: {encrypted}')

def decrypt(message, shift_number):
    print(f'Decyrpting message: {message} ....')
    decrypted = ''
    for char in message:
        index = alphabet.index(char)
        shifted_index = index - shift_number 
        shifted_index %= 26
        new_char = alphabet[shifted_index]
        decrypted += new_char
    # time.sleep(2)
    print('Message Decrypted successfully: ', decrypted)


encrypt('cat', shift_number)
decrypt('mkd', shift_number)

