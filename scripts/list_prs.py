import os
import requests
token = os.environ.get('GITHUB_TOKEN')
url = 'https://api.github.com/repos/loxjbve/--/pulls'
headers = {'Authorization': f'token {token}'} if token else {}
r = requests.get(url, headers=headers)
for pr in r.json():
    print(f"#{pr['number']}: {pr['title']} ({pr['head']['ref']} -> {pr['base']['ref']})")
