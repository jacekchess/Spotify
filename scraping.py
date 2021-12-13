# %%
import csv
import requests
import os
import gzip
import shutil
from urllib.request import urlopen
from bs4 import BeautifulSoup

# %%
url="https://kworb.net/spotify/artists.html"
html=urlopen(url)
soup=BeautifulSoup(html, "html.parser")    


# %%
ids = ""
i=0
for item in soup.find_all('td', attrs={'class' : 'text'}):
    if i <=10:
        link = item.div.a['href'].split("/")[1].split(".")[0]
        # print(link)
        ids += link + "%"
    i += 1
print(ids[:-1])


# for item in soup.find_all('h2'):
#     string=item.get_text()
#     split_string=string.split(", ")
#     country=split_string[-1]
#     if country in countries:
#         print(string)


# %%
# for item in soup.find_all('h2'):
#     string=item.get_text()
#     split_string=string.split(', ')
#     country=split_string[-1]
#     if country in countries:
#         table=item.nextSibling.nextSibling.nextSibling.nextSibling
#         #print(table.get('class', []))
#         #print(type(table))
#         listing_url=table.contents[3].contents[1].contents[5].contents[0]['href']
#         filename=f'airbnb_gz\{string}.csv.gz'

#         with open(filename, "wb") as f:
#             r = requests.get(listing_url)
#             f.write(r.content)
