# %%
import pandas as pd

def get_citystate(item):
    if ' (' in item:
        return item[:item.find(' (')]
    elif '[' in item:
        return item[:item.find('[')]
    else:
        return item


def towns():
    university_towns = []
    with open('./data-sets/university_towns.txt') as file:
        for line in file:
            if '[edit]' in line:
                state = line
            else:
                university_towns.append((state, line))
        df = pd.DataFrame(university_towns, columns=['State', 'Town'])
        df = df.map(get_citystate)
        return df 

arr = towns()