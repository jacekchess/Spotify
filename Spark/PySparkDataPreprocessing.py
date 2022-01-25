#!/usr/bin/env python
# coding: utf-8

# ## Przetwarzanie danych w PySpark

# In[1]:


# potrzebne importy
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, explode_outer, countDistinct, size, input_file_name, when, substring, length, expr


# In[2]:


# inicjowanie sesji Spark
spark = (
    SparkSession.builder
    .appName("HDFS")
    .getOrCreate()
)


# ### Artyści

# In[3]:


# wczytanie danych dla artystów
artists = (
    spark.read
    .json('hdfs://localhost:8020/user/wisniewskij/spotify/artists/*', multiLine=True)
)


# In[4]:


artists.printSchema()


# In[5]:


# selekcja potrzebnych artybutów
artists_df = (
    artists
    .select(explode_outer('artists').alias('artists'))
    .select('artists.id',
            'artists.name',
            'artists.followers.total', 
            'artists.genres',
            'artists.popularity')
    .withColumnRenamed('total', 'followers')
)


# In[6]:


artists_df.printSchema()


# In[7]:


artists_df.show()


# In[8]:


# sprawdzenie duplikatów
artists_df_duplicated = artists_df.groupBy("id", "name").count().filter("count > 1")
artists_df_duplicated.show(truncate=False)
print('Liczba rekordów dla zduplikowanych artystów: ', artists_df_duplicated.groupBy().sum().collect()[0][0])


# In[9]:


# wyfiltrowanie rekordów dla zduplikowanych artystów
id_duplicated = [row.id for row in artists_df_duplicated.select("id").collect()]
artists_df.filter(artists_df.id.isin(id_duplicated)).orderBy('name').show(21)


# In[10]:


# widok tymczasowy na potrzeby dalszego przetwarzania
artists_df.createGlobalTempView("artists")


# In[11]:


# zdeduplikowana ramka danych
artists_df_deduplicated = (
    spark.sql("SELECT id, name, max(followers) as followers, popularity, genres FROM global_temp.artists GROUP BY id, name, popularity, genres")
)
artists_df_deduplicated.filter(artists_df.id.isin(id_duplicated)).orderBy('name').show() # sprawdzenie duplikatów
print('Liczba unikalnych artystów: ', artists_df_deduplicated.count())


# In[12]:


# spłaszczenie kolumny zawierającej gatunki, w których tworzy artysta
artists_df_deduplicated_genres = artists_df_deduplicated.withColumn('genres', explode_outer('genres'))


# In[13]:


artists_df_deduplicated_genres.count()


# In[14]:


# liczba artystów bez gatunków
artists_df_deduplicated_genres.filter(col('genres').isNull()).select('id').distinct().count() 


# ### Albums

# In[15]:


# wczytanie danych dla albumów
albums = (
    spark.read
    .json('hdfs://localhost:8020/user/wisniewskij/spotify/albums/*', multiLine=True)
)


# In[16]:


albums.printSchema()


# In[17]:


# liczba plików z pustą listą albumów (artystów bez albumów)
albums.select(size('items').alias('size')).groupBy('size').count().filter(col('size') == 0).show()


# In[18]:


# id artystów bez albumów
albums.filter(size('items')==0).select(split('href', '/')[5].alias('artist_id'), 'items').show(truncate=False)


# In[19]:


# selekcja potrzebnych artybutów
albums_df = (
    albums
    .select(split('href', '/')[5].alias('artist_id'), 'items')
    .withColumn('albums', explode_outer('items').alias('albums'))
    .select('artist_id',
            col('albums.album_type').alias('album_type'),
            'albums.id',
            'albums.name',
            'albums.release_date', 
            'albums.total_tracks')
    .filter(col('album_type').isin(['album', 'single']))
)


# In[20]:


albums_df.printSchema()


# In[21]:


albums_df.show()


# In[22]:


albums_df.count()


# In[23]:


# liczba unikalnych albumów
albums_df.select('id').distinct().count()


# In[24]:


# sprawdzenie braków (artystów bez dopasowanych albumów/albumów nie pasujących do artystów)
artists_albums_outer_join_null = (
    artists_df_deduplicated_genres
    .join(albums_df.withColumnRenamed('id', 'album_id').withColumnRenamed('name', 'album_name'), 
          artists_df_deduplicated.id == albums_df.artist_id, 'outer')
    .filter(col('id').isNull() | col('artist_id').isNull())
)


# In[25]:


# artyści bez dopasowanych albumów
artists_albums_outer_join_null.select('id', 'name').distinct().show(30,truncate=False)


# In[26]:


# albumy niedopasowane do artystów
artists_albums_outer_join_null.select('artist_id').distinct().show(truncate=False)


# In[27]:


# Morissette - 2x id - 62WbvkXqQGvXQvw74GU3kQ / 2hz61ryzrN6bUBZjOQnKbd
# MC 3L - 2x id - 4BbsSamQy6XSByO4O4Nymt / 6ZjisQlDoiiHbr8yN9J1Sc
# Ashin Chen - 2x id - 6H93wOohK6r1MwGh41Z4Nb / 7CP9fSApQUk9nKQx0rPSda
# pozostałe artist_id (11) - drugie działające id dla zduplikowanych artystów
# 2zHzn2oA1QJOB8SPIpoiYD - Dadju, 3FgenUy0FsiWi5tatx73Ha - Sehinsah, 56r1N9JpztKsmNNk2FtpDU / 1LT6utaOOPuP58rAQTCrWl - Vassy, 
# 6ZjisQlDoiiHbr8yN9J1Sc - MC 3L, 1XsVSBeFDiptyuBlX2FgFs - Rochak Kohli, 5AVUF0rTgAIMhdP5mtnbN7 - Marca MP,
# 2iAtipcqILmxM2vctkuJCd - Bibic, 13KOtjLXdUPwIDuGsORa5i - Yuridope, 6QqEc36uUltQc78hnqXOgx - Jul, 
# 46mGxneffUCmDhMU1m6zYu - Tungevaag, 4CwqmHBrsCzKuBqlQKfYxZ - Russ Millions
# ==> artists + albums łączone przy pomocy inner/left join (zduplikowani artyści pomijani; po zmianie id 3 artystów bez albumów)


# In[28]:


# zmiana id dla 3 artystów (opisane powyżej)
albums_df = (
    albums_df
    .withColumn("artist_id", when(albums_df.artist_id == "2hz61ryzrN6bUBZjOQnKbd","62WbvkXqQGvXQvw74GU3kQ") \
                .when(albums_df.artist_id == "6ZjisQlDoiiHbr8yN9J1Sc","4BbsSamQy6XSByO4O4Nymt") \
                .when(albums_df.artist_id == "7CP9fSApQUk9nKQx0rPSda","6H93wOohK6r1MwGh41Z4Nb") \
                .otherwise(albums_df.artist_id)
               )
)


# ### Tracks

# In[29]:


# wczytanie danych dla utworów
tracks = (
    spark.read
    .json('hdfs://localhost:8020/user/wisniewskij/spotify/tracks/*', multiLine=True)
)


# In[30]:


# dodanie kolumny z nazwą pliku (celem uzyskania id artysty)
tracks = tracks.withColumn("filename", input_file_name())


# In[31]:


tracks.printSchema()


# In[32]:


# liczba plików z pustą listą utworów
tracks.select(size('tracks').alias('size')).groupBy('size').count().filter(col('size') == 0).show()


# In[33]:


# selekcja potrzebnych artybutów
tracks_df = (
    tracks
    .select(split('filename', '/')[7].alias('filename'), 'tracks')
    .withColumn('tracks', explode('tracks'))
    .select(col('tracks.album.id').alias('album_id'),
            'tracks.duration_ms',
            'tracks.explicit',
            'tracks.id',
            'tracks.name',
            'tracks.popularity',
            'filename'
           )
    .withColumn("filename",expr("substring(filename, 1, length(filename)-5)")) # usunięcie ".json"
    .withColumnRenamed("filename", 'artist_id')
)


# In[34]:


tracks_df.show()


# In[35]:


tracks_df.count()


# In[36]:


# liczba unikalnych utworów
tracks_df.select('id').distinct().count()


# In[37]:


# zmiana id 3 artystów (tych samych dla których modyfikacja albumów)
tracks_df = (
    tracks_df
    .withColumn("artist_id", when(tracks_df.artist_id == "2hz61ryzrN6bUBZjOQnKbd","62WbvkXqQGvXQvw74GU3kQ") \
                .when(tracks_df.artist_id == "6ZjisQlDoiiHbr8yN9J1Sc","4BbsSamQy6XSByO4O4Nymt") \
                .when(tracks_df.artist_id == "7CP9fSApQUk9nKQx0rPSda","6H93wOohK6r1MwGh41Z4Nb") \
                .otherwise(tracks_df.artist_id)
               )
)


# ### Finalne dane

# In[38]:


data = (
    artists_df_deduplicated_genres
    .withColumnRenamed('id', 'artist_id')
    .withColumnRenamed('name', 'artist_name')
    .withColumnRenamed('popularity', 'artist_popularity')
    .join(albums_df.withColumnRenamed('id', 'album_id').withColumnRenamed('name', 'album_name'), 'artist_id', 'left')
    .join(tracks_df
          .withColumnRenamed('id', 'track_id')
          .withColumnRenamed('name', 'track_name')
          .withColumnRenamed('popularity', 'track_popularity'), ['artist_id', 'album_id'],  'left')
)


# In[39]:


data.count()


# In[40]:


data.show()


# In[41]:


# uporządkowanie kolumn
data = data.select('artist_id', 'artist_name', 'followers', 'artist_popularity', 'genres', 'album_id', 'album_type', 
                   'album_name', 'release_date', 'total_tracks', 'track_id', 'track_name', 'explicit',
                   'track_popularity', 'duration_ms')


# In[42]:


data.write.option('header', 'true').csv('hdfs://localhost:8020/user/wisniewskij/spotify/data')

