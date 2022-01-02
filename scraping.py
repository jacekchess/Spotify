# %%
# wymagane pakiety
from urllib.request import urlopen
from bs4 import BeautifulSoup

# %%
# strona internetowa zawierająca ranking artystów wraz z ich id
url="https://kworb.net/spotify/artists.html"
html=urlopen(url)
soup=BeautifulSoup(html, "html.parser")    


# %%
ids = ""
i=1
for item in soup.find_all('td', attrs={'class' : 'text'}):
    # ekstrakcja id artysty z odwołania do spotify 
    id = item.div.a['href'].split("/")[1].split(".")[0]
    ids += id + ","
    # usunięcie separatora z końcowej linii pliku
    if i == 5000:
        ids = ids[:-1]
        break
    # podział na linie po 40 id każda 
    # na potrzeby dalszego przetwarzania - jeden request do API może zawierać ograniczoną liczbę id
    if i % 40 == 0:
        ids = ids[:-1] + "\n"
    i += 1

# %%
# zapis id artystów do pliku tekstowego
with open("artists_ids.txt", "w") as output:
    output.write(ids)