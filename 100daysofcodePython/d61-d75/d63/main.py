from flask import Flask, render_template, request, redirect, url_for
# import sqlite3

# db = sqlite3.connect('100daysofcodePython/d61-d75/d63/books-collection.db')
# cursor = db.cursor()
# cursor.execute("CREATE TABLE books (id INTEGER PRIMARY KEY, title varchar(250) NOT NULL UNIQUE, author varchar(250) NOT NULL, rating FLOAT NOT NULL)")
# cursor.execute("INSERT INTO books VALUES(1, 'Harry Potter', 'J. K. Rowling', '9.3')")
# db.commit()
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import Integer, String, Float

all_books = []

# Initialize the extension
class Base(DeclarativeBase):
    pass
db = SQLAlchemy(model_class=Base)

# configuring the extension
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///books-collection.db'
db.init_app(app)

# Define Models
class Books(db.Model):
    id: Mapped[Integer] = mapped_column(primary_key=True)
    title: Mapped[String] = mapped_column(unique=True, nullable=False)
    author: Mapped[String] = mapped_column(nullable=False)
    rating: Mapped[Float] = mapped_column(nullable=False)

# # create the table
with app.app_context():
    db.create_all()

@app.route('/')
def home():
    books_length = len(all_books)
    return render_template('index.html', books=all_books)


@app.route("/add", methods=['GET', 'POST'])
def add():
    if request.method == 'POST':
        new_book = {key:value for (key, value) in request.form.items()}
        all_books.append(new_book)
        return redirect(url_for('home'))
        
    return render_template('add.html')


if __name__ == "__main__":
    app.run(debug=True)

