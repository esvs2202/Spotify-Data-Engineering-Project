from pyspark import pipelines as dp 
from pyspark.sql.functions import col

# creating Silver tables for each data entity which accomodate the clean data

# for dim_user
@dp.table 
def stg_dim_user():
    df = spark.readStream.table("`spotify-catalog`.bronze.dim_user")
    df = df.drop("_rescued_data").dropDuplicates(["user_id"])
    return df

dp.create_streaming_table(name = "dim_user")

dp.create_auto_cdc_flow(
    target = "dim_user",
    source = "stg_dim_user",
    keys = ["user_id"],
    sequence_by = "updated_at",
    ignore_null_updates = False,
    stored_as_scd_type = 2,
    track_history_except_column_list = None,
    name = None,
    once = False 
)

# for dim_artist
@dp.table 
def stg_dim_artist():
    df = spark.readStream.table("`spotify-catalog`.bronze.dim_artist")
    df = df.drop("_rescued_data").dropDuplicates(["artist_id"])
    return df

dp.create_streaming_table(name = "dim_artist")

dp.create_auto_cdc_flow(
    target = "dim_artist",
    source = "stg_dim_artist",
    keys = ["artist_id"],
    sequence_by = "updated_at",
    ignore_null_updates = False,
    stored_as_scd_type = 2,
    track_history_except_column_list = None,
    name = None,
    once = False 
)

# dim_track
@dp.table 
def stg_dim_track():
    df = spark.readStream.table("`spotify-catalog`.bronze.dim_track")
    df = df.drop("_rescued_data").dropDuplicates(["track_id"])
    return df

dp.create_streaming_table(name = "dim_track")

dp.create_auto_cdc_flow(
    target = "dim_track",
    source = "stg_dim_track",
    keys = ["track_id"],
    sequence_by = "updated_at",
    ignore_null_updates = False,
    stored_as_scd_type = 2,
    track_history_except_column_list = None,
    name = None,
    once = False 
)

# dim_date
@dp.table 
def stg_dim_date():
    df = spark.readStream.table("`spotify-catalog`.bronze.dim_date")
    df = df.drop("_rescued_data").dropDuplicates(["date_key"])
    return df

dp.create_streaming_table(name = "dim_date")

dp.create_auto_cdc_flow(
    target = "dim_date",
    source = "stg_dim_date",
    keys = ["date_key"],
    sequence_by = "date",
    ignore_null_updates = False,
    stored_as_scd_type = 2,
    track_history_except_column_list = None,
    name = None,
    once = False 
)

# fact_stream
@dp.table 
@dp.expect_or_drop("valid_stream", "listen_duration > 0 AND device_type IS NOT NULL AND user_id IS NOT NULL AND track_id IS NOT NULL")
def stg_fact_stream():
    df = spark.readStream.table("`spotify-catalog`.bronze.fact_stream")
    df = df.drop("_rescued_data").dropDuplicates(["stream_id"])
    return df

dp.create_streaming_table(name = "fact_stream")

dp.create_auto_cdc_flow(
    target = "fact_stream",
    source = "stg_fact_stream",
    keys = ["stream_id"],
    sequence_by = "stream_timestamp",
    ignore_null_updates = False,
    stored_as_scd_type = 2,
    track_history_except_column_list = None,
    name = None,
    once = False 
)
