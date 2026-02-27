from flask import Flask

app = Flask(__name__)

def make_bold(function):
    def wrapper_function():
        return f'<b> {function()} <b>'
    return wrapper_function

@app.route('/')
def hello_world():
    return 'Hi'


@app.route('/bye')
@make_bold
def bye():
    return 'Bye'


@app.route('/username/<name>/<int:number>')
def greet(name, number):
    return f'Hello {name}, age {number}'



if __name__ == '__main__':
    app.run(debug=True)
    
print('Executed', __file__)

# Env variable configuration
# export FLASK_APP=100daysofcodePython/d46-d60/d54/hello.py