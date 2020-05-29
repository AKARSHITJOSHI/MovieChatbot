from bs4 import BeautifulSoup
import requests,url
r = requests.get(url) # where url is the above url    
bs = BeautifulSoup(r.text)
for movie in bs.findAll('td','title'):
    title = movie.find('a').contents[0]
    genres = movie.find('span','genre').findAll('a')
    genres = [g.contents[0] for g in genres]
    runtime = movie.find('span','runtime').contents[0]
    year = movie.find('span','year_type').contents[0]
    print(title)