import requests
from bs4 import BeautifulSoup
from datetime import datetime
import spotipy
from spotipy.oauth2 import SpotifyOAuth

# date = input('Which year do you want to travel to? Type the date in this format YYYY-MM-DD:')
user_date = '2024-01-01'
category_url = 'https://en.wikipedia.org/wiki/Category:Lists_of_Billboard_Year-End_Hot_100_singles'

header = {
    'USER-AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36'
}

res = requests.get(url=category_url, headers=header)
category_data = res.text

soup = BeautifulSoup(category_data, 'html.parser')

# extract available starting year and end year
years = soup.find_all(name='div', class_='mw-category-group')
titles = [item.getText() for item in years]
titles = titles[0].split('\n')
start_year = titles[1][-5:]
end_year = titles[len(titles)-1][-5:]

# check if uerar is valid
is_year_valid = False 
year = datetime.strptime(user_date, '%Y-%m-%d').year
# if (year <= int(end_year) and year >= int(start_year)):
if (int(start_year) <= year <= int(end_year)):
    is_year_valid = True

    
def get_wiki_billboard(year):    
    year_url = f'https://en.wikipedia.org/wiki/Billboard_Year-End_Hot_100_singles_of_{year}'
    
    res = requests.get(url=year_url, headers=header)
    text = res.text
    
    soup = BeautifulSoup(text, 'html.parser')
    
    body_rows = soup.select(selector='table tbody tr')

    songs = []
    for row in body_rows:
        cells = row.find_all(name="td")
        if len(cells) < 3:
            continue
       
        # Get the rank
        rank = cells[0].get_text(strip=True)

        # Get title inside the <a>, no surrounding quotes
        title_tag = cells[1].select_one("a")
        if title_tag:
            title = title_tag.get_text(strip=True)
        else:
            title = cells[1].get_text(strip=True)

        # Get artists spaced and without the \n at the end
        artist_tag = cells[2].select_one("a")
        if artist_tag:
            artist = artist_tag.get_text(strip=True)
        else:
            artist = cells[2].get_text(strip=True)

        song_data = {
            "no.": rank,
            "title": title,
            "artist": artist,
        }
        songs.append(song_data)
    return songs
    
if is_year_valid:
    songs = get_wiki_billboard(year)
    
print(songs)

# Spotify Authentication
sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        scope="playlist-modify-private",
        redirect_uri="http://example.com",
        client_id=YOUR_CLIENT_ID,
        client_secret=YOUR-CLIENT-SECRET,
        show_dialog=True,
        cache_path="token.txt"
    )
)
user_id = sp.current_user()["id"]
print(user_id)

# Searching Spotify for songs by title
song_uris = []
year = user_date.split("-")[0]
for song in songs:
    result = sp.search(q=f"track:{song['title']} year:{year}", type="track")
    print(result)
    try:
        uri = result["tracks"]["items"][0]["uri"]
        song_uris.append(uri)
    except IndexError:
        print(f"{song} doesn't exist in Spotify. Skipped.")

# Creating a new private playlist in Spotify
playlist = sp.user_playlist_create(user=user_id, name=f"{user_date} Billboard 100", public=False)
print(playlist)

# Adding songs found into the new playlist
sp.playlist_add_items(playlist_id=playlist["id"], items=song_uris)