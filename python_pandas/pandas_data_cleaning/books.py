# %%
import pandas as pd
import numpy as np
def read():
    df = pd.read_csv("data-sets/BL-Flickr-Images-Book.csv")
    to_drop = ['Edition Statement', 'Corporate Author', 'Corporate Contributors','Former owner', 'Engraver', 'Contributors', 'Issuance type','Shelfmarks']
    df.drop(columns=to_drop, inplace=True, axis=1)
    df.set_index('Identifier', inplace=True) 
    extr = df['Date of Publication'].str.extract(r'^(\d{4})', expand=False)
    df['Date of Publication'] = pd.to_numeric(extr)
    london = df['Place of Publication'].str.contains('London', case=False, na=False)
    oxford = df['Place of Publication'].str.contains('Oxford', case=False, na=False)
    df['Place of Publication'] = np.where(london,'London', np.where(oxford, 'Oxford', df['Place of Publication'].str.replace('-',' ')))
    return df
books = read()
# %%
