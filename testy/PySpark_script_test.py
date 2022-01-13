#!/usr/bin/env python
# coding: utf-8

# ## PySpark script

# In[1]:


import findspark
findspark.init()


# In[2]:


from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("HDFS")
    .getOrCreate()
)


# In[3]:


from pyspark.sql.functions import col, split, explode, explode_outer, countDistinct, size, input_file_name, when, substring, length, expr


# ### Artists

# In[4]:


artists = (
    spark.read
    .json('hdfs://localhost:8020/user/wisniewskij/spotify/artists/*', multiLine=True)
)


# In[5]:


# artists.printSchema()


# In[6]:


artists_df = (
    artists
    .select(explode_outer('artists').alias('artists'))
    .select('artists.id',
            'artists.name',
            'artists.followers.total', 
            'artists.genres',
            'artists.popularity')
    .withColumnRenamed('total', 'followers')
    #.withColumn('genres', explode('genres'))
)


# In[7]:


# artists_df.printSchema()


# In[8]:


#artists_df.show()


# In[9]:


# sprawdzenie duplikatów
artists_df_duplicated = artists_df.groupBy("id", "name").count().filter("count > 1")
artists_df_duplicated.show(truncate=False)
artists_df_duplicated.groupBy().sum().collect()[0][0]


# In[10]:


id_duplicated = [row.id for row in artists_df_duplicated.select("id").collect()]
artists_df.filter(artists_df.id.isin(id_duplicated)).orderBy('name').show(21)


# In[11]:


artists_df.createGlobalTempView("artists") ### odkomentować przy nowej sesji / restarcie kernela


# In[12]:


artists_df_deduplicated = ( # finalna ramka dla artystów
    spark.sql("SELECT id, name, max(followers) as followers, popularity, genres FROM global_temp.artists GROUP BY id, name, popularity, genres")
)
artists_df_deduplicated.filter(artists_df.id.isin(id_duplicated)).orderBy('name').show()
artists_df_deduplicated.count()


# In[13]:


artists_df_deduplicated_genres = artists_df_deduplicated.withColumn('genres', explode_outer('genres'))

import numpy
import happybase

connection = happybase.Connection('127.0.0.1', port = 9090)
connection.open()


# In[50]:


# connection.create_table(
#     'artists_test',
#     {'artist' : dict()
#     }
# )


# In[61]:


catalog = ''.join("""
{        
"table":{"namespace":"default", "name":"artists_test"},
"rowkey":"key",        
"columns":{          
"col0":{"cf":"rowkey", "col":"key", "type":"string"},          
"col1":{"cf":"artist", "col":"artist_id", "type":"string"},          
"col2":{"cf":"artist", "col":"artist_name", "type":"string"},          
"col3":{"cf":"artist", "col":"followers", "type":"bigint"},          
"col4":{"cf":"artist", "col":"artist_popularity", "type":"tinyint"},          
"col5":{"cf":"artist", "col":"genres", "type":"string"}
}        
}      
""".split())


# In[64]:


#import os

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11 --repositories http://repo.hortonworks.com/content/groups/public/'


# In[69]:


artists_df_deduplicated_genres.write.options(catalog=catalog).format("org.apache.spark.sql.execution.datasources.hbase").save()

