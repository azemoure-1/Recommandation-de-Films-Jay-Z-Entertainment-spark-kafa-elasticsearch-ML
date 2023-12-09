import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import year, to_date, when
from pyspark.sql.functions import array, lit, when, col, expr, regexp_extract
from elasticsearch import Elasticsearch

# Set Spark configurations
spark_conf = SparkConf() \
    .setAppName("KafkaSparkIntegration_movie_data") \
    .set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0") \
    .set("spark.executor.memory", "2g") \
    .set("spark.executor.cores", "2") \
    .set("spark.cores.max", "2")

# Create Spark session
spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

# Define Kafka consumer configuration
kafka_topic = "movie_data"

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", kafka_topic) \
    .load()

# Define the schema for the incoming JSON messages
json_schema = StructType([
    StructField("Action", StringType()),
    StructField("Adventure", StringType()),
    StructField("Animation", StringType()),
    StructField("Children", StringType()),
    StructField("Comedy", StringType()),
    StructField("Crime", StringType()),
    StructField("Documentary", StringType()),
    StructField("Drama", StringType()),
    StructField("Fantasy", StringType()),
    StructField("Film-Noir", StringType()),
    StructField("Horror", StringType()),
    StructField("IMDb_URL", StringType()),
    StructField("Musical", StringType()),
    StructField("Mystery", StringType()),
    StructField("Romance", StringType()),
    StructField("Sci-Fi", StringType()),
    StructField("Thriller", StringType()),
    StructField("War", StringType()),
    StructField("Western", StringType()),
    StructField("movieId", IntegerType()),
    StructField("movie_title", StringType()),
    StructField("release_date", StringType()),
    StructField("unknown", StringType())
])

# Parse the JSON messages from the 'value' column using the defined schema
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", json_schema).alias("data")) \
    .select("data.*")


# Assuming 'genres' is defined as a list of genre names
genres = ["Action", "Adventure", "Animation", "Children", "Comedy", "Crime", "Documentary", "Drama",
          "Fantasy", "Film-Noir", "Horror", "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"]

# Assuming 'parsed_df' is the DataFrame with parsed movie data
parsed_df = parsed_df.withColumn("genre_count", sum(parsed_df[genre] for genre in genres))

# Filter out records with missing release dates
parsed_df = parsed_df.withColumn("release_date", to_date("release_date", "dd-MMM-yyyy"))

# Recalculate the release year
parsed_df = parsed_df.withColumn("release_year", year(parsed_df["release_date"]))


# Replace values with column name if 1, else with space :
parsed_df = parsed_df.withColumn("genre", array(*[when(col(column) == 1, lit(column)).otherwise(lit('')) for column in genres]))

# Remove empty strings from the "genre" list :
parsed_df = parsed_df.withColumn("genre", expr("filter(genre, element -> element != '')"))

parsed_df = parsed_df.withColumn("movie_title", regexp_extract(parsed_df["movie_title"], "^(.*?)\s\(\d{4}\)$", 1))


# Assuming parsed_df_movies is your DataFrame for movie data
parsed_df = parsed_df \
    .filter(col("IMDb_URL").isNotNull() & col("movieId").isNotNull() & col("movie_title").isNotNull() & col("release_date").isNotNull() & col("genre_count").isNotNull() & col("release_year").isNotNull() & col("genre").isNotNull())


# Drop individual genre columns
parsed_df = parsed_df.drop(*genres)
parsed_df = parsed_df.drop("unknown")


# Elasticsearch server URL
es_server = "http://localhost:9200"

# Elasticsearch mapping
mapping = {
    "mappings": {
        "properties": {
            "IMDb_URL": {"type": "keyword"},
            "movieId": {"type": "integer"},
            "movie_title": {"type": "text"},
            "release_date": {"type": "date"},
            "genre_count": {"type": "integer"},
            "release_year": {"type": "integer"},
            "genre": {"type": "keyword"}
        }
    }
}


# Create Elasticsearch index with explicit mapping if it does not exist
es = Elasticsearch([{'host': es_server.split(":")[1].replace("//", ''), 'port': int(es_server.split(":")[2]), 'scheme': 'http'}])

if not es.indices.exists(index="movie_data"):
    es.indices.create(index="movie_data", body=mapping)

# Write data to Elasticsearch
es_resource = "movie_data"



# Write data to Elasticsearch
query = parsed_df.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .outputMode("append") \
    .option("es.resource", "user_ratings") \
    .option("es.nodes", es_server.split("//")[1]) \
    .option("es.port", es_server.split(":")[2]) \
    .option("es.nodes.wan.only", "false") \
    .option("es.index.auto.create", "false") \
    .option("failOnDataLoss", "false") \
    .option("checkpointLocation", "./checkpointLocation/movie_data") \
    .start()

# Await termination of the query
query.awaitTermination()