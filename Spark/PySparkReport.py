#!/usr/bin/env python
# coding: utf-8

# # Report

# ## Wczytanie danych

# In[1]:

# potrzebne importy
import findspark
findspark.init()

from pyspark.sql import SparkSession
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import happybase


# In[2]:

# inicjowanie sesji Spark
spark = (
    SparkSession.builder
    .appName("HDFS")
    .getOrCreate()
)


# In[3]:

# wczytanie danych z HDFS
data = (
    spark.read
    .option('header', 'true')
    .option('inferschema', 'true')
    .csv('hdfs://localhost:8020/user/wisniewskij/spotify/data/*')
)

# In[4]:
# inicjowanie połączenia z HBase
# connection = happybase.Connection('127.0.0.1', port = 9090)
# connection.open()


# In[5]:
# utworzenie tabeli w HBase
# connection.create_table(
#     'spotify_views',
#     {'genres' : dict(),
#      'albums' : dict(),
#      'tracks' : dict()
#     }
# )

# ## Application

# In[6]:

def generate_genres(artist_id):
    
    # inicjowanie połączenia z HBase
    connection = happybase.Connection('127.0.0.1', port = 9090)
    connection.open()
    data_hbase = connection.table('spotify_views')
    
    # wczytanie danych dla danego artysty z HBase 
    hbase_genres = None
    for key, content in data_hbase.scan(filter = str("SingleColumnValueFilter ('genres','artist_id',=,'regexstring:^" + artist_id + "$')")):
        row = []
        cols = []
        for column, value in content.items():
            if b'genres:' in column:
                row.append(value)
                cols.append(column)
        

        if (hbase_genres is None) and (len(cols) > 0):
            hbase_genres = pd.DataFrame(columns = cols)

        if len(row) > 0:
            hbase_genres = hbase_genres.append(pd.DataFrame([row], columns = cols), ignore_index=True)

            
    # sytuacja gdy widok dla danego artysty istnieje już w HBase
    if hbase_genres is not None:
        data_genres_final = hbase_genres.dropna(subset = [b'genres:artist_id'])
        data_genres_final = data_genres_final.iloc[: , 1:] # usunięcie kolumny z id artysty 
        data_genres_final.columns = ['artist_name', 'artist_popularity',
               'common_genres', 'followers', 'place']

        for column in data_genres_final.columns:
            data_genres_final[column] = data_genres_final[column].str.decode('utf-8') # wczytanie polskich znaków
            
        data_genres_final = data_genres_final[['place', 'artist_name', 'followers', 'artist_popularity',
       'common_genres']]
        data_genres_final['place'] = data_genres_final['place'].astype('int64') # konwersja typu
        data_genres_final.sort_values(by=['place'], inplace = True) # sortowanie wg rankingu
        
        
    # sytuacja gdy widok dla danego artysty nie istnieje jeszcze w HBase - utworzenie widoku z danych z HDFS
    else:
        # lista gatunków w których tworzy dany artysta
        genres = data.filter(data.artist_id == artist_id).dropDuplicates(['genres']).select(data.genres).collect()

        genres_array = []
        for genre in np.concatenate(np.array(genres)):
            genres_array.append(genre)

        # dane dla artystów tworzących w tych samych gatunkach    
        data_common_genres = (
            data
            .filter(data.genres.isin(genres_array))
            .groupBy('artist_id').agg(F.collect_set('genres').alias('common_genres'))
            .dropDuplicates(['artist_id'])
        )
        # dodanie kolumny z rankingiem wg followers
        data_genres = (
            data
            .filter(data.genres.isin(genres_array))
            .dropDuplicates(['artist_id'])
            .withColumn('place', F.row_number().over(Window.orderBy(data.followers.desc())))
            .join(data_common_genres, 'artist_id', 'left')
        )
        # miejsce danego artysty w rankingu
        artist_place = np.array(
            data_genres
            .filter(data_genres.artist_id == artist_id)
            .select('place')
            .orderBy(data_genres.followers.desc())
            .collect()
        )[0][0]
        # sytuacja w której dany artysta jest w pierwszej 12 rankingu
        if artist_place < 13:
            data_genres_final = (
                data_genres
                .select('place', 'artist_name', 'followers', 'artist_popularity', F.concat_ws(", ",F.col('common_genres')).alias('common_genres'))
                .orderBy(data_genres.followers.desc())
                .limit(12).toPandas().reset_index(drop = True)
            )
        # sytuacja gdy jest poza 12 (dodanie wierszy z danym artystą i jego sąsiadami)
        else:
            data_genres_1 = (
                data_genres
                .select('place', 'artist_name', 'followers', 'artist_popularity', F.concat_ws(", ",F.col('common_genres')).alias('common_genres'))
                .orderBy(data_genres.followers.desc())
                .limit(10).toPandas()
            )
            data_genres_2 = (
                data_genres
                .select('place', 'artist_name', 'followers', 'artist_popularity', F.concat_ws(", ",F.col('common_genres')).alias('common_genres'))
                .filter((data_genres.place > int(artist_place - 2)) & (data_genres.place < int(artist_place + 2)) )
                .orderBy(data_genres.followers.desc())
                .toPandas()
            )
            data_genres_final = data_genres_1.append(data_genres_2).reset_index(drop = True)
            
        # zapis widoku do HBase
        counter = 0
        for key, content in data_hbase.scan():
            counter += 1

        for i in range(data_genres_final.shape[0]):
            data_hbase.put(str(counter + i), 
                              {'genres:artist_id': artist_id,
                               'genres:place': str(data_genres_final.loc[i, "place"]),
                               'genres:artist_name': str(data_genres_final.loc[i, "artist_name"]),
                               'genres:followers': str(data_genres_final.loc[i, "followers"]),
                               'genres:artist_popularity': str(data_genres_final.loc[i, "artist_popularity"]),
                               'genres:common_genres': str(data_genres_final.loc[i, "common_genres"])
                              })
            
    # dodanie wiersza z "..." w sytuacji gdy artysta jest poza top 12 w gatunkach w których tworzy
    if data_genres_final.shape[0] == 13:
        data_genres_1 = data_genres_final.head(10)
        data_genres_2 = data_genres_final.tail(3)

        data_genres_final = data_genres_1.append(pd.DataFrame([['...', '...', '...', '...', '...']], columns = data_genres_1.columns), ignore_index=True).append(data_genres_2).reset_index(drop = True)

    return data_genres_final


# In[7]:

def generate_albums(artist_id):
    
    # inicjowanie połączenia z HBase
    connection = happybase.Connection('127.0.0.1', port = 9090)
    connection.open()
    data_hbase = connection.table('spotify_views')
    
    # wczytanie danych dla danego artysty z HBase  
    hbase_albums = None
    for key, content in data_hbase.scan(filter = str("SingleColumnValueFilter ('albums','artist_id',=,'regexstring:^" + artist_id + "$')")):
        row = []
        cols = []
        for column, value in content.items():
            if b'albums:' in column:
                row.append(value)
                cols.append(column)
        

        if (hbase_albums is None) and (len(cols) > 0):
            hbase_albums = pd.DataFrame(columns = cols)

        if len(row) > 0:
            hbase_albums = hbase_albums.append(pd.DataFrame([row], columns = cols), ignore_index=True)

            
    # sytuacja gdy widok dla danego artysty istnieje już w HBase
    if hbase_albums is not None:
        data_albums = hbase_albums.dropna(subset = [b'albums:artist_id'])
        data_albums = data_albums.drop(data_albums.columns[[2]], axis=1) 
        data_albums.columns = ['album_name', 'album_type', 'release_date', 'total_tracks']

        for column in data_albums.columns:
            data_albums[column] = data_albums[column].str.decode('utf-8') # wczytanie polskich znaków

        data_albums = data_albums[['album_name', 'release_date', 'total_tracks', 'album_type']]
        data_albums.sort_values(by=['release_date'], inplace = True, ascending = False)
        
    # sytuacja gdy widok dla danego artysty nie istnieje jeszcze w HBase - utworzenie widoku z danych z HDFS
    else:
        data_albums = (
            data
            .filter(data.artist_id == artist_id)
            .dropDuplicates(['album_id'])
            .dropDuplicates(['album_name'])
            .select(data.album_name, data.release_date, data.total_tracks, data.album_type)
            .orderBy(data.release_date.desc())
            .toPandas()
        )
            
        # zapis widoku do HBase
        counter = 0
        for key, content in data_hbase.scan():
            counter += 1
            
        for i in range(data_albums.shape[0]):
            data_hbase.put(str(counter + i), 
                              {'albums:artist_id': artist_id,
                               'albums:album_name': str(data_albums.loc[i, "album_name"]),
                               'albums:release_date': str(data_albums.loc[i, "release_date"]),
                               'albums:total_tracks': str(data_albums.loc[i, "total_tracks"]),
                               'albums:album_type': str(data_albums.loc[i, "album_type"]),
                              })
            
    return data_albums


# In[8]:

def generate_tracks(artist_id):
    
    # inicjowanie połączenia z HBase
    connection = happybase.Connection('127.0.0.1', port = 9090)
    connection.open()
    data_hbase = connection.table('spotify_views')
    
    # wczytanie danych dla danego artysty z HBase  
    hbase_tracks = None
    for key, content in data_hbase.scan(filter = str("SingleColumnValueFilter ('tracks','artist_id',=,'regexstring:^" + artist_id + "$')")):
        row = []
        cols = []
        for column, value in content.items():
            if b'tracks:' in column:
                row.append(value)
                cols.append(column)
        

        if (hbase_tracks is None) and (len(cols) > 0):
            hbase_tracks = pd.DataFrame(columns = cols)

        if len(row) > 0:
            hbase_tracks = hbase_tracks.append(pd.DataFrame([row], columns = cols), ignore_index=True)

            
    # sytuacja gdy widok dla danego artysty istnieje już w HBase
    if hbase_tracks is not None:
        data_tracks = hbase_tracks.dropna(subset = [b'tracks:artist_id'])
        data_tracks = data_tracks.drop(data_tracks.columns[[1]], axis=1)
        data_tracks.columns = ['album_name', 'duration', 'explicit', 'track_name', 'track_popularity']

        for column in data_tracks.columns:
            data_tracks[column] = data_tracks[column].str.decode('utf-8') # wczytanie polskich znaków

        data_tracks = data_tracks[['track_name', 'track_popularity', 'duration', 'explicit', 'album_name']]
        data_tracks.sort_values(by=['track_popularity'], inplace = True, ascending = False)
        
    # sytuacja gdy widok dla danego artysty nie istnieje jeszcze w HBase - utworzenie widoku z danych z HDFS
    else:
        data_tracks = (
            data
            .filter(data.artist_id == artist_id)
            .dropDuplicates(['track_id'])
            .filter(data.track_name != 'null')
            .withColumn('duration', F.from_unixtime(F.col("duration_ms").cast("string")/1000, format="mm:ss"))
            .select("track_name", "track_popularity", "duration", "explicit", "album_name")
            .orderBy(data.track_popularity.desc())
            .toPandas()
        )
            
        # zapis widoku do HBase
        counter = 0
        for key, content in data_hbase.scan():
            counter += 1
            
        for i in range(data_tracks.shape[0]):
            data_hbase.put(str(counter + i), 
                              {'tracks:artist_id': artist_id,
                               'tracks:track_name': str(data_tracks.loc[i, "track_name"]),
                               'tracks:track_popularity': str(data_tracks.loc[i, "track_popularity"]),
                               'tracks:duration': str(data_tracks.loc[i, "duration"]),
                               'tracks:explicit': str(data_tracks.loc[i, "explicit"]),
                               'tracks:album_name': str(data_tracks.loc[i, "album_name"])
                              })
            
    return data_tracks

