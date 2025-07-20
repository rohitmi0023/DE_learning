#%%

dic = {'name': 'rohit'}
def unpack(dic):
    print(dic)
    unpacked = set(dic)
    print(unpacked)
    print('bye')

unpack(dic)


# %%
def greet(name):
    print(f"Hello {name}")

greet(**dic)
