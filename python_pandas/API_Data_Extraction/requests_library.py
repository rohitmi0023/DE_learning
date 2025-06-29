# Article: https://realpython.com/python-requests/#the-response
# Other Article to Consider: https://requests.readthedocs.io/en/latest/user/quickstart/#make-a-request

# %%
# GET request

import requests

requests.get("https://api.github.com")

# %%
# The Response-> powerful object

response = requests.get("https://api.github.com")
print(response)
print(response.status_code)
print(response.__bool__)
print(response.raise_for_status)
try:
    response = requests.get("https://api.githubb.com")
    response.raise_for_status()
except requests.exceptions.HTTPError as http_err:
    print(f"HTTP error occured: {http_err}")
except Exception as err:
    print(f'Some other error occured {response.status_code}')
else:
    print('Success')

# %%
# Content

import json

response = requests.get("https://api.github.com")
print(response.content)
response.encoding = 'utf-8'
op = response.text
json.loads(op)
response_dict = response.json()
print(response_dict)
# Headers
response.headers['content-type'] #keys are case-insensitive
headers = response.headers
print(headers)

# %%
# Query String Parameters

response = requests.get('https://api.github.com/search/repositories',
params={"q":"language:python","sort":"stars", "order":"desc"}
)
json_response = response.json()
# print(json_response)
popular_repo = json_response['items']
for repo in popular_repo[:3]:
    print(f"Name: {repo['name']}")
    print(f"Description: {repo['description']}")
    print(f"Stars: {repo['stargazers_count']}")
    # print()

# %%
# Request Headers

response = requests.get("https://api.github.com/search/repositories",
params={"q": '"real python"'},
headers={"Accept": "application/vnd.github.text-match+json"}
)
json_response = response.json()
first_repository = json_response["items"][0]
print(first_repository["text_matches"][0]["matches"])

# %%
# Other HTTP Methods

requests.get('url')
requests.post('url',data={'key':'value'})
requests.put('url',data={'key':'value'})
requests.delete('url')
requests.head('url')
requests.patch('url',data={'key':'value'})
requests.options('url')

# %%
# The Message Body


requests.post('https://httpbin.org/post', data={'key':'value'})
requests.post("https://httpbin.org/post", data=[('key','value')])
response = requests.post("https://httpbin.org/post", json={'key':'value'})
# print(response)
json_response = response.json()
print(json_response)
json_response['data']
json_response['headers']

# %%
# Request Inspection (things before request sent to Server)

response = requests.post("https://httpbin.org/post", json={"key":"value"})
print(response.json())
response.request.headers['Content-Type']
response.request.url
response.request.body

# %%
# Authentication
import requests
response = requests.get('https://httpbin.org/basic-auth/user/passwd',auth=('user','passwd'))
response = requests.get('https://httpbin.org/basic-auth/user/passwd',auth=requests.auth.HTTPBasicAuth('user','passwd'))
response.request.headers

# %%
# Performance -> # Session Class/Object

from custom_token_auth import TokenAuth

requests.get('https://api.github.com',timeout=(2,5))

token = 'abc'

with requests.Session() as session: # helps in persistent connection
    session.auth = TokenAuth(token)
    response = session.get("https://api.github.com/user")

print(response.headers)
print(response.json())

# %%
# Retries

github_adapter = requests.adapters.HTTPAdapter(max_retries=2)
session = requests.Session()
session.mount("https://api.github.com", github_adapter)
try:
    session = session.get('https://api.github.com/')
except requests.exceptions.RetryError as err:
    print(f"Error: {err}")
finally:
    session.close()

