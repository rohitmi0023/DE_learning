# %%
# Convert Python Dictionaries to JSON(Serialization)
import json

sample = {'a': 1, 'b': 2}
json.dumps(sample)

sample2 = {1:True, 2:False}
json.dumps(sample2)

dog_id = 1
dog_name = "Frieda"
dog_registry = {dog_id: {"name": dog_name}}
json.dumps(dog_registry)

toy_conditions = {"chew bone": 7, "ball": 3, "sock": -1}
json.dumps(toy_conditions, sort_keys=True)

# Write JSON file with Python
dog_data = {
  "name": "Frieda",
  "is_dog": True,
  "hobbies": ["eating", "sleeping", "barking",],
  "age": 8,
  "address": {
    "work": None,
    "home": ("Berlin", "Germany",),
  },
  "friends": [
    {
      "name": "Philipp",
      "hobbies": ["eating", "sleeping", "reading",],
    },
    {
      "name": "Mitch",
      "hobbies": ["running", "snacking",],
    },
  ],
}

write_path = 'test.json'
with open(write_path, mode='w', encoding='utf-8') as json_file:
    json.dump(dog_data, json_file) # what to write, where to write

# Reading JSON with Python

# json.loads(): To deserialize a string, bytes, or byte array instances
# json.load(): To deserialize a text file or a binary file
dog_registry = {1: {"name": "Di"}}
dog_json = json.dumps(dog_registry)
dog_json
new_dog_registry = json.loads(dog_json)
dog_registry == new_dog_registry

# Deserialize JSON Data Types
dog_data = {
   "name": "Frieda",
   "is_dog": True,
   "hobbies": ["eating", "sleeping", "barking",],
   "age": 8,
   "address": {
     "work": None,
     "home": ("Berlin", "Germany",),
   },
 }
dog_data_json = json.dumps(dog_data)
dog_data_json

new_dog_data = json.loads(dog_data_json)
new_dog_data


# Open an external JSON File with Python
with open('test.json',mode='r',encoding='utf-8') as read_file:
    data = json.load(read_file) # reading JSON file
    # The Python object that you get from json.load() depends on the top-level data type of your JSON file.
    print(data['name']) 

# Interacting With JSON

# Prettify JSON With Python
dog_friend = {
    "name": "Mitch",
    "age": 6.5,
}
print(json.dumps(dog_friend, indent=4))
# same for dump method

# python -m json.tool dog_friend.json --indent 4 -> to validate a json file 
# pass optional outpile file name to display output to file

# to downsize file
# $ python -m json.tool pretty_frieda.json mini_frieda.json --compact