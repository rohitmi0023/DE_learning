# 	Performance Optimization
# Article: https://towardsdatascience.com/optimizing-pandas-code-the-impact-of-operation-sequence-0c5aa159632a/

# %%
import pandas as pd
import timeit

n = 1_000_000
df = pd.DataFrame({
    letter: list(range(n)) for letter in "abcdefghijklmnopqrstuwxyz"
})
# df.head()

# Question- to filter a < 50_000 and b > 3000 and take only five coloumns- 'a', 'b', 'g', 'n', 'x'
take_columns = ['a', 'b', 'g', 'n', 'x']
query = "a < 50_000 and b > 3000"

# # 1st method
subdf = df[take_columns]
subdf = subdf[subdf['a'] < 50_000]
subdf = subdf[subdf['b'] > 3000]

# 2nd Method
subdf = subdf[subdf['a'] < 50_000]
subdf = subdf[subdf['b'] > 3000]
subdf = subdf[take_columns]

# 3rd method
df.filter(take_columns).query(query)

# 4th method 
df.query(query).filter(take_columns)


# %%

import pandas as pd
import timeit

n = 1_000_000
df = pd.DataFrame({
    letter: list(range(n)) for letter in "abcdefghijklmnopqrstuwxyz"
})

n_of_elements = lambda d: d.shape[0]*d.shape[1]
n_of_elements(df)

# Does it matter which row filtering will you apply first, a < 50_000 or b > 3000?
n_of_elements(df[df['a'] < 50_000])
# -> 1250000

n_of_elements(df[df['b'] > 3000])
# -> 24924975


