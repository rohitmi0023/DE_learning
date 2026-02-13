import requests
from datetime import datetime

USERNAME = 'rohitmi0023'
TOKEN = 'oafhohoafdfs'

WEB_URL = 'https://pixe.la/v1/users/rohitmi0023/graphs/graph1.html'

pixela_endpoint = 'https://pixe.la/v1/users'

user_json = {
    'token': TOKEN,
    'username': USERNAME,
    'agreeTermsOfService': 'yes',
    'notMinor': 'yes'
}

#---------- User Creation
# res = requests.post(url=pixela_endpoint, json=user_json)
# print(res.text)


#----------- Graph Creation
# 'https://pixe.la/v1/users/rohitmi0023/graphs'
graph_endpoint = f'{pixela_endpoint}/{USERNAME}/graphs'

graph_json = {
    'id': 'graph1',
    'name': 'Cycling Graph',
    'unit': 'Km',
    'type': 'float',
    'color': 'ajisai'
}

headers = {
    'X-USER-TOKEN': TOKEN
}

# graph_res = requests.post(url=graph_endpoint, json=graph_json, headers=headers)
# print(graph_res.text)


# ------------- Post Value to Graph: Pixel 
# 'https://pixe.la/v1/users/rohitmi0023/graphs/graph1'
pixel_creation_endpoint = f'{graph_endpoint}/{graph_json['id']}'

today = datetime.now()
today = today.strftime('%Y%m%d')

value = str(input('How many kms to log?'))

pixel_creation_json = {
    'date': today,
    'quantity': value
}

# pixel_creation_res = requests.post(url=pixel_creation_endpoint, json=pixel_creation_json, headers=headers)
# print(pixel_creation_res.text)

# --------------- Updating Logged Data
# 'https://pixe.la/v1/users/rohitmi0023/graphs/graph1/20260214'
update_put_endpoint = f'{pixel_creation_endpoint}/{today}'

delete_json = {
    'quantity': '0.1',    
}

# update_put_endpoint_res = requests.put(url=update_put_endpoint, json=delete_json, headers=headers)
# print(update_put_endpoint_res.text)


# ----------------- Deleting Logged Data
delete_endpoint = update_put_endpoint

delete_endpoint_res = requests.delete(url=delete_endpoint, headers=headers)
print(delete_endpoint_res.text)