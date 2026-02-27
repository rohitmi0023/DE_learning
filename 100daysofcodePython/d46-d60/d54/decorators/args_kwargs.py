class User:
    def __init__(self, name):
        self.name = name
        self.is_logged_in = False
    

def decorator_auth_check(function):
    def wrapper_fn(*args, **kwargs):
        fn_arg0 = args[0]
        is_logged_in = fn_arg0.is_logged_in
        if is_logged_in:
            function(fn_arg0)
        else:
            print('You need to login to create post!')
    return wrapper_fn



@decorator_auth_check      
def create_user_post(user:User):
    print(f'Hurray! A new post is created for user: {user.name}')
    
        
new_user = User('Rohit')
new_user.is_logged_in = True
create_user_post(new_user)



