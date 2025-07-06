# %%
# Map
numbers = [1,2,3]
def squared(x):
    return x**2
mapped = map(squared, numbers)
listed = list(mapped)

# %%
# lambda
numbers3 = [1,1,2]
mapped2 = map(lambda x: {x:x**2}, numbers)
listed2 = list(mapped2)

mapped3 = map(lambda x: [x,x**2], numbers3)
listed3 = dict(mapped3)

# %%
# FlatMap

from itertools import chain

words = ["hello world", "python flatmap"]
split_words = list(map(lambda x: x.split(), words))
flat_words = list(chain.from_iterable(map(lambda x: x.split(), words)))

#%%
# Questions #1. output of list(map(lambda x: x * 2, [[1], [2]]))  # Hint: Not [2, 4]! 
ans1 = list(map(lambda x: x*2, [[1],[2]]))
# output-> [[1,1], [2,2]]
# x*2 duplicates the elements inside the sublists (list concatenation), not the sublists themselves
# [x] * 2 would duplicate the sublists themselves resulting in output [[[1],[1]], [[2],[2]]]

# %%
# Question #2. How would you use flatMap to convert ["a,b", "c,d"] to ["a", "b", "c", "d"]?
q2 = ["a,b", "c,d"]
from itertools import chain
ans2 = list(chain.from_iterable(map(lambda x: x.split(','), q2)))

# %%
# Question #3. In PySpark, when would you prefer map over flatMap for JSON data?
# map is to be prefered over flatMap when we want to preserve the structure
