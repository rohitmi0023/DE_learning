from bs4 import BeautifulSoup
import requests

website = 'https://web.archive.org/web/20200518073855/https://www.empireonline.com/movies/features/best-movies-2/'
movies_list_file = '/Users/rohitmi/Documents/DE_learning/100daysofcodePython/d31-d45/d45/movies.txt'

res = requests.get(url=website)
html = res.text
soup = BeautifulSoup(html, 'html.parser')

all_movies = soup.find_all(name='h3', class_ = 'title')
movies_list = [item.getText() for item in all_movies[::-1]]

with open(file=movies_list_file, mode='w') as f:
    for movie in movies_list:
        movie += '\n'
        f.write(movie)
