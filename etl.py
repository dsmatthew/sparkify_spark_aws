import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


config = configparser.ConfigParser()
config.read('dl.cfg')

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']
os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Load and process the raw song data. Load JSON files and extract 'songs' and 'artists' tables.
    Store both tables via spark as parquet files.

    Parameters
    ----------
    spark : spark instance
        
    input_data : str
        Path to input data (json files)
    output_data : str
        Path to output data (parquet files) - could be local or S3 bucket.


    Return
    ------
    songs_table : PySpark dataframe
        Content of the dimensions "songs"
    artists_table : PySpark dataframe
        Content of the dimensions "artists"
    '''
    # get filepath to song data file
    song_data = input_data + '/song_data/*/*/*/*.json'
    
    # Predefined schema
    songSchema = StructType([
        StructField('num_songs',            IntegerType())
        , StructField('artist_id',          StringType())
        , StructField('artist_latitude',    DoubleType())
        , StructField('artist_longitude',   DoubleType())
        , StructField('artist_location',    StringType())
        , StructField('artist_name',        StringType())
        , StructField('song_id',            StringType())
        , StructField('title',              StringType())
        , StructField('duration',           DoubleType())
        , StructField('year',               IntegerType())
    ])
    
    # read song data file
    df = spark.read.json(song_data, schema=songSchema)
    if 's3' in input_data.lower():
        print('[SUCCESS] reading song data from AWS S3 bucket.')
    else:
        print('[SUCCESS] reading song data from local file system.')

    
    # extract columns to create songs table
    # cols for table 'songs': song_id, title, artist_id, year, duration --> only distinct values
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    #songs_table.write.format('parquet').mode('overwrite').partitionBy('year', 'artist_id').saveAsTable(output_data + '/songs') # storing on a DBMS (e.g. Hadoop/Hive)
    songs_table.write.parquet(output_data + '/songs', mode='overwrite', partitionBy=['year', 'artist_id'])
    #songs_table.write.format('parquet').mode('append').partitionBy('year', 'artist_id').saveAsTable('songs')

    # extract columns to create artists table
    artists_table = df.select('artist_id', col('artist_name').alias('name'), col('artist_location').alias('location')
                              , col('artist_latitude').alias('latitude'), col('artist_longitude').alias('longitude')).dropDuplicates()
    
    # write artists table to parquet files
    #artists_table.write.format('parquet').mode('overwrite').saveAsTable('artists') # storing on a DBMS (e.g. Hadoop/Hive)
    artists_table.write.parquet(output_data + '/artists', mode='overwrite')
    
    return songs_table, artists_table


def process_log_data(spark, input_data, output_data):
    '''
    Load and process the raw log data. Load JSON files and extract 'users', 'time' and 'songplays' tables.
    Store both tables via spark as parquet files.

    Parameters
    ----------
    spark : spark instance
        
    input_data : str
        Path to input data (json files)
    output_data : str
        Path to output data (parquet files) - could be local or S3 bucket.


    Return
    ------
    users_table : PySpark dataframe
        Content of the dimensions "users"
    time_table : PySpark dataframe
        Content of the dimensions "time"
    songplays_table: PySpark dataframe
        Content of the fact table "songplays"
    '''
    # get filepath to song data file
    song_data = input_data + '/song_data/*/*/*/*.json'
    #log_data = input_data + '/log-data/*.json'
    log_data = input_data + '/log_data/*/*/*.json'
    
    # Predefined schema
    logSchema = StructType([
        StructField('artist',           StringType())
        , StructField('auth',           StringType())
        , StructField('firstName',      StringType())
        , StructField('gender',         StringType())
        , StructField('itemInSession',  IntegerType())
        , StructField('lastName',       StringType())
        , StructField('length',         DoubleType())
        , StructField('level',          StringType())
        , StructField('location',       StringType())
        , StructField('method',         StringType())
        , StructField('page',           StringType())
        , StructField('registration',   StringType())
        , StructField('sessionId',      IntegerType())
        , StructField('song',           StringType())
        , StructField('status',         IntegerType())
        , StructField('ts',             IntegerType())
        , StructField('userAgent',      IntegerType())
        , StructField('userId',         IntegerType())
    ])
    
    # read song data file
    df = spark.read.json(log_data)
    if 's3' in input_data.lower():
        print('[SUCCESS] reading song data from AWS S3 bucket.')
    else:
        print('[SUCCESS] reading song data from local file system.')
    
    # filter by actions for song plays
    print('Dataframe unprocessd: ', str((df.count(), len(df.columns))))
    df = df.filter(lower(col('page')) == 'nextsong')
    print('Dataframe processed:  ', str((df.count(), len(df.columns))))
    
    
    # extract columns to create songs table
    # cols for table 'users': user_id, first_name, last_name, gender, level --> distinct by 'user_id'
    users_table = df.select(col('userId').alias('user_id'), col('firstName').alias('first_name'), col('lastName').alias('last_name')
                           , 'gender', 'level').dropDuplicates()
    
    
    # write users table to parquet files
    #users_table.write.format('parquet').mode('overwrite').saveAsTable('users') # storing on a DBMS (e.g. Hadoop/Hive)
    users_table.write.parquet(output_data + '/users', mode='overwrite')

    # create timestamp column from original timestamp column
    # cols for table 'time': start_time, hour, day, week, month, year, weekday
    #get_timestamp = 
    time_table = df.select('ts').dropDuplicates() # get distinct timestamp
    
    # create datetime column from original timestamp column
    time_table = time_table.withColumn('start_time', to_timestamp(col('ts') / 1000))
    #get_datetime = udf()
    #df = 
    
    # extract columns to create time table
    time_table = time_table.select('start_time', hour(col('start_time')).alias('hour'), dayofweek(col('start_time')).alias('day')
                                   , weekofyear(col('start_time')).alias('week'), month(col('start_time')).alias('month')
                                   , year(col('start_time')).alias('year'), weekofyear(col('start_time')).alias('weekday'))
    
    # write time table to parquet files partitioned by year and month
    #time_table.write.format('parquet').mode('overwrite').saveAsTable('time') # storing on a DBMS (e.g. Hadoop/Hive)
    time_table.write.parquet(output_data + '/time', mode='overwrite', partitionBy=['year', 'month']) # partition to spread the amount of data

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)
    #song_df = spark.read.load(output_data + '/songs', format='parquet')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, [df.artist == song_df.artist_name
                                       , df.song == song_df.title], how='inner')
    songplays_table = songplays_table.withColumn('start_time', to_timestamp(col('ts') / 1000)) # prepare timestamp
    songplays_table = songplays_table.join(time_table.select('start_time', 'month')
                                           , on=['start_time'], how='inner')
    songplays_table = songplays_table.withColumn('songplay_id', monotonically_increasing_id()) # generate an artificial ID

    # merge table parts & select columns
    # cols for 'songplays': songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    songplays_table = songplays_table.select('songplay_id', 'start_time', col('userId').alias('user_id'), 'level', 'song_id', 'artist_id'
                             , col('sessionId').alias('session_id'), 'location', col('userAgent').alias('user_agent'), 'year', 'month')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + '/songplays', mode='overwrite', partitionBy=['year', 'month'])
    if 's3' in output_data.lower():
        print('[SUCCESS] writing songplays data to AWS S3 bucket.')
    else:
        print('[SUCCESS] writing songplays data to local file system.')
    
    
    return users_table, time_table, songplays_table

def main():
    spark = create_spark_session()
    
    # Configuration for AWS S3 processing
    input_data = 's3a://udacity-dend/'
    output_data = 's3://dend-ms-project-4/data_out'
    
    # Configuration for local processing (development purpose)
    #input_data = 'data'
    #output_data = 'data/output'
    
    # process song_data
    var_return1, var_return2 = process_song_data(spark, input_data, output_data)
    print('nrows of:')
    print('\tsongs: ', var_return1.count())
    print('\tartists: ', var_return2.count())

    # process log_data
    var_return3, var_return4, var_return5 = process_log_data(spark, input_data, output_data)
    print('nrows of:')
    print('\tusers: ', var_return3.count())
    print('\ttime: ', var_return4.count())
    print('\tsongsplays: ', var_return5.count())


if __name__ == "__main__":
    main()
