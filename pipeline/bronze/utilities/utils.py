from pyspark.sql import functions as F

def metadata(df):
    return (
            df        
            .selectExpr("*"
            ,"_metadata.file_path as file_path"
            ,"_metadata.file_modification_time as file_modification_time")
            .withColumn("inserted_date", F.current_timestamp())
            .withColumn("updated_date", F.current_timestamp())
        )

def bronze_ingest(spark, path_name: str, format: str = "csv"):
    # Declare the path of the Volumes
    catalog = 'db_bike'
    schema_db = '00_landing'
    volume = 'volume_bike'
    base_path = f'/Volumes/{catalog}/{schema_db}/{volume}/'
    
    file_path = f'{base_path}/data/{path_name}'
    schema_location = f'{base_path}/autoloader-metadata/{path_name}'

    # Read the data from the File Storage/Volumes
    df = (spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", format)
        .option("cloudFiles.schemaLocation", f'{schema_location}')
        .option("header", True)
        .option("sep",",")
        .option("inferSchema", "true")
        .option("encoding", 'ISO-8859-1')
        .load(file_path)
    )

    # Add metadata for the Tables
    return metadata(df)

