
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import from_json, col, when, from_unixtime, date_format
from pyspark.sql.types import StructType, StringType, IntegerType
from elasticsearch import Elasticsearch

# Set Spark configurations
spark_conf = SparkConf() \
    .setAppName("KafkaSparkIntegration_user_ratings") \
    .set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0") \
    .set("spark.executor.memory", "2g") \
    .set("spark.executor.cores", "2") \
    .set("spark.cores.max", "2")

# Create Spark session
spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

# Define Kafka consumer configuration for user_ratings_data
kafka_topic_ratings = "user_ratings_data"

df_ratings = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", kafka_topic_ratings) \
    .load()

# Define the schema for the incoming JSON messages in user_ratings_data
json_schema_ratings = StructType() \
    .add("user_id", IntegerType()) \
    .add("movieId", IntegerType()) \
    .add("rating", IntegerType()) \
    .add("timestamp", IntegerType())

# Parse the JSON messages from the 'value' column using the defined schema
parsed_df_ratings = df_ratings.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", json_schema_ratings).alias("data")) \
    .select("data.*")

# Filter out records with missing user_id or movieId
parsed_df_ratings = parsed_df_ratings \
    .filter(col("user_id").isNotNull() & col("movieId").isNotNull() & col("rating").isNotNull() & col("timestamp").isNotNull())

# Change the type of "timestamp" from int to date
parsed_df_ratings = parsed_df_ratings.withColumn(
    "timestamp",
    date_format(from_unixtime(col("timestamp").cast("double")), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
)

# Define the explicit mapping for Elasticsearch index
mapping = {
    "mappings": {
        "properties": {
            "user_id": {"type": "integer"},
            "movieId": {"type": "integer"},
            "rating": {"type": "integer"},
            "timestamp": {"type": "date", "format": "strict_date_optional_time||epoch_millis"}
        }
    }
}

# Elasticsearch server URL
es_server = "http://localhost:9200"

# Create Elasticsearch index with explicit mapping
es = Elasticsearch([{'host': es_server.split(":")[1].replace("//", ''), 'port': int(es_server.split(":")[2]), 'scheme': 'http'}])


if not es.indices.exists(index="user_ratings"):
    es.indices.create(index="user_ratings", body=mapping)

# Write data to Elasticsearch
query = parsed_df_ratings.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .outputMode("append") \
    .option("es.resource", "user_ratings") \
    .option("es.nodes", es_server.split("//")[1]) \
    .option("es.port", es_server.split(":")[2]) \
    .option("es.nodes.wan.only", "false") \
    .option("es.index.auto.create", "false") \
    .option("failOnDataLoss", "false") \
    .option("checkpointLocation", "./checkpointLocation/") \
    .start()

# Await termination of the query
query.awaitTermination()