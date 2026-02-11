from pyspark import pipelines as dp

# create gold tables for each one of the data entities

# dim_user
@dp.table 
def stg_dim_user():
    df = spark.readStream.table("`spotify-catalog`.silver.dim_user")
    return df 

dp.create_streaming_table(name = "dim_user")

dp.create_auto_cdc_flow(
    target = "dim_user",
    source = "stg_dim_user",
    keys = ["user_id"],
    sequence_by = "updated_at",
    except_column_list = ["__START_AT","__END_AT"],
    ignore_null_updates = False,
    stored_as_scd_type = 2,
    name = None,
    once = False 
)

#dim_artist
@dp.table 
def stg_dim_artist():
    df = spark.readStream.table("`spotify-catalog`.silver.dim_artist")
    return df 

dp.create_streaming_table(name = "dim_artist")

dp.create_auto_cdc_flow(
    target = "dim_artist",
    source = "stg_dim_artist",
    keys = ["artist_id"],
    sequence_by = "updated_at",
    except_column_list = ["__START_AT","__END_AT"],
    ignore_null_updates = False,
    stored_as_scd_type = 2,
    name = None,
    once = False 
)

# dim_track
@dp.table 
def stg_dim_track():
    df = spark.readStream.table("`spotify-catalog`.silver.dim_track")
    return df 

dp.create_streaming_table(name = "dim_track")

dp.create_auto_cdc_flow(
    target = "dim_track",
    source = "stg_dim_track",
    keys = ["track_id"],
    sequence_by = "updated_at" ,
    except_column_list = ["__START_AT","__END_AT"],
    ignore_null_updates = False,
    stored_as_scd_type = 2,
    name = None,
    once = False
)

# dim_date
@dp.table 
def stg_dim_date():
    df = spark.readStream.table("`spotify-catalog`.silver.dim_date")
    return df 

dp.create_streaming_table(name = "dim_date")

dp.create_auto_cdc_flow(
    target = "dim_date",
    source = "stg_dim_date",
    keys = ["date_key"],
    sequence_by = "date",
    except_column_list = ["__START_AT","__END_AT"],
    ignore_null_updates = False,
    stored_as_scd_type = 2,
    name = None,
    once = False
)

# fact_stream
@dp.table 
def stg_fact_stream():
    df = spark.readStream.table("`spotify-catalog`.silver.fact_stream")
    return df 

dp.create_streaming_table(name = "fact_stream")

dp.create_auto_cdc_flow(
    target = "fact_stream",
    source = "stg_fact_stream",
    keys = ["stream_id"],
    sequence_by = "stream_timestamp",
    except_column_list = ["__START_AT","__END_AT"],
    ignore_null_updates = False,
    stored_as_scd_type = 2,
    name = None,
    once = False
)