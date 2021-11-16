import requests
from bs4 import BeautifulSoup
import subprocess

# beautifulsoup bs4
URL = "http://200.152.38.155/CNPJ/"
r = requests.get(URL)
# print(dir(r))
texto = BeautifulSoup(r.text, 'html')
texto = texto.find_all("a")
urls = []
for i, linha in enumerate(texto):
    url = linha.get('href')
    if "ESTABELE" in url or "EMPRE" in url:
        urls.append(url)

for i, url in enumerate(urls):
    print(f'Processando arquivo {i+1} de {len(urls)} => {url}')
    p = subprocess.run(['curl', f'{URL}{url}', '--output', url])
    print(p.returncode)
