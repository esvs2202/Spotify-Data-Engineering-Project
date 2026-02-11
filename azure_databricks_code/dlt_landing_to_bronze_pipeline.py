from pyspark import pipelines as dp 

# bronze tables
@dp.table 
def dim_user():
    df = spark.readStream.format("cloudFiles")\
              .option("cloudFiles.format", "parquet")\
              .option("cloudFiles.schemaLocation", "abfss://bronze@spotifydeprojectstorage.dfs.core.windows.net/dim_user/schema/")\
              .option("cloudFiles.schemaEvolutionMode", "addNewColumns")\
            .option("pathGlobFilter", "*.parquet")\
              .load("abfss://landing@spotifydeprojectstorage.dfs.core.windows.net/DimUser/")
    return df 

@dp.table 
def dim_artist():
    df = spark.readStream.format("cloudFiles")\
              .option("cloudFiles.format", "parquet")\
              .option("cloudFiles.schemaLocation", "abfss://bronze@spotifydeprojectstorage.dfs.core.windows.net/dim_artist/schema/")\
              .option("cloudFiles.schemaEvolutionMode", "addNewColumns")\
            .option("pathGlobFilter", "*.parquet")\
              .load("abfss://landing@spotifydeprojectstorage.dfs.core.windows.net/DimArtist/")
    return df 

@dp.table 
def dim_track():
    df = spark.readStream.format("cloudFiles")\
              .option("cloudFiles.format", "parquet")\
              .option("cloudFiles.schemaLocation", "abfss://bronze@spotifydeprojectstorage.dfs.core.windows.net/dim_track/schema/")\
              .option("cloudFiles.schemaEvolutionMode", "addNewColumns")\
            .option("pathGlobFilter", "*.parquet")\
              .load("abfss://landing@spotifydeprojectstorage.dfs.core.windows.net/DimTrack/")
    return df 

@dp.table 
def dim_date():
    df = spark.readStream.format("cloudFiles")\
              .option("cloudFiles.format", "parquet")\
              .option("cloudFiles.schemaLocation", "abfss://bronze@spotifydeprojectstorage.dfs.core.windows.net/dim_date/schema/")\
              .option("cloudFiles.schemaEvolutionMode", "addNewColumns")\
            .option("pathGlobFilter", "*.parquet")\
              .load("abfss://landing@spotifydeprojectstorage.dfs.core.windows.net/DimDate/")
    return df 
  
@dp.table 
def fact_stream():
    df = spark.readStream.format("cloudFiles")\
              .option("cloudFiles.format", "parquet")\
              .option("cloudFiles.schemaLocation", "abfss://bronze@spotifydeprojectstorage.dfs.core.windows.net/fact_stream/schema/")\
              .option("cloudFiles.schemaEvolutionMode", "addNewColumns")\
            .option("pathGlobFilter", "*.parquet")\
              .load("abfss://landing@spotifydeprojectstorage.dfs.core.windows.net/FactStream/")
    return df 