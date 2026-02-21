from bs4 import BeautifulSoup
import requests

# file_path = '100daysofcodePython/d31-d45/d45/website.html'

# with open(file=file_path, mode='r') as f:
#     contents = f.read()

# soup = BeautifulSoup(contents, 'html.parser')

# print(soup.title)
# print(soup.title.name)
# print(soup.title.string)
# # print(soup.prettify())
# print(soup.p)

# # all anchor tags
# anchor_tags = soup.find_all(name='a')
# for tag in anchor_tags:
#     # print(tag.getText())
#     tag.
    

# YCOMBINATOR
y_combinator = 'https://appbrewery.github.io/news.ycombinator.com/'

res = requests.get(url=y_combinator)
text = res.text

soup = BeautifulSoup(text, 'html.parser')

# print(soup.title.string)

article_tag = soup.find(name='a', class_='storylink')
article_text = article_tag.getText()
article_link = article_tag.get('href')
article_upvote = soup.find(name='span', class_='score').getText()

# print(article_text, article_link, article_upvote)

all_news = soup.find_all('a', class_='storylink')
article_texts = [news.getText() for news in all_news]
article_links = [news.get('href') for news in all_news]
article_upvotes = [int(score.getText().split()[0]) for score in soup.find_all('span', class_='score')]

max_upvotes = max(article_upvotes)
idx = article_upvotes.index(max_upvotes)
print(article_texts[idx])
print(article_links[idx])
print(max_upvotes)


