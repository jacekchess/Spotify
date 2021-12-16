# %%
from urllib.request import urlopen
from bs4 import BeautifulSoup

# %%
url="https://kworb.net/spotify/artists.html"
html=urlopen(url)
soup=BeautifulSoup(html, "html.parser")    


# %%
ids = ""
#links = ""
i=1
for item in soup.find_all('td', attrs={'class' : 'text'}):
    id = item.div.a['href'].split("/")[1].split(".")[0]
    ids += id + ","
    if i == 5000:
        ids = ids[:-1]
        break
    if i % 40 == 0:
        ids = ids[:-1] + "\n"
    i += 1
    # albums_link = "https://api.spotify.com/v1/artists/" + id + "/albums"
    # links += albums_link + "\n"
# links=links[:-1]

# %%
with open("artists_ids.txt", "w") as output:
    output.write(ids)
# with open("albums_links.txt", "w") as output:
#     output.write(links)

