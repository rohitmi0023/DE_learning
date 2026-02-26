from flask import Flask

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hi'

if __name__ == '__main__':
    app.run()
    
print('Executed', __file__)

# Env variable configuration
# export FLASK_APP=100daysofcodePython/d46-d60/d54/hello.py