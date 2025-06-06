from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col


spark = SparkSession.builder \
    .appName("Anime ETL") \
    .config("spark.jars", "C:/path/to/mysql-connector-java-8.0.28.jar") \
    .getOrCreate()

try:
    
    csv_path = "C:/Users/rahel/Downloads/archive/top_anime_dataset.csv"
    print(f"Loading CSV from: {csv_path}")
    df = spark.read.option("header", True).csv(csv_path)
    
    
    df = df.dropna(subset=["anime_id", "name", "score", "members"])
    df = df.filter(col("members") > 1000)

    
    print("\nCreating DimAnime...")
    dim_anime = df.select(
        col("anime_id").cast("bigint"),
        "name", 
        "type",
        col("episodes").cast("int"),
        col("score").cast("float"),
        col("members").cast("int")
    ).distinct()
    
    
    print("\nCreating DimGenre...")
    dim_genre = df.select(
        explode(split(col("genres"), ",")).alias("genre_name")
    ).distinct()
    
    
    print("\nCreating DimStudio...")
    dim_studio = df.select(
        explode(split(col("studios"), ",")).alias("studio_name")
    ).distinct()

   
    print("\nCreating FactAnimeStats...")
    fact_anime = df.select(
        col("anime_id").cast("bigint"),
        "score",
        "members",
        explode(split(col("genres"), ",")).alias("genre_name"),
        explode(split(col("studios"), ",")).alias("studio_name")
    )

    
    jdbc_url = "jdbc:mysql://localhost:3306/anime_warehouse"
    properties = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    print("\nWriting DimAnime...")
    dim_anime.write.jdbc(url=j)