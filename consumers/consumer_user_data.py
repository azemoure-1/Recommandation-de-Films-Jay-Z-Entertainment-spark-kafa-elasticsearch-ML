
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StringType, IntegerType
from elasticsearch import Elasticsearch

# Set Spark configurations
spark_conf = SparkConf() \
    .setAppName("KafkaSparkIntegration_UserData") \
    .set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0") \
    .set("spark.executor.memory", "2g") \
    .set("spark.executor.cores", "2") \
    .set("spark.cores.max", "2")

# Create Spark session
spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

# Define Kafka consumer configuration for user_data
kafka_topic_user_data = "user_data"

# Read streaming data from Kafka
df_user_data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", kafka_topic_user_data) \
    .load()

# Define the schema for the incoming JSON messages in user_data
json_schema_user_data = StructType() \
    .add("user_id", IntegerType()) \
    .add("age", IntegerType()) \
    .add("gender", StringType()) \
    .add("occupation", StringType()) \
    .add("zip_code", StringType())

# Parse the JSON messages from the 'value' column using the defined schema
parsed_df_user_data = df_user_data.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", json_schema_user_data).alias("data")) \
    .select("data.*")

# Add age_group column based on age
parsed_df_user_data = parsed_df_user_data.withColumn("age_group",
                                                     when(col("age").between(0, 25), "Young")
                                                     .when(col("age").between(26, 60), "Adult")
                                                     .otherwise("Senior"))

# Filter out records with missing values
parsed_df_user_data = parsed_df_user_data \
    .filter(col("user_id").isNotNull() & col("age").isNotNull() & col("gender").isNotNull() & col("occupation").isNotNull() & col("zip_code").isNotNull())

mapping = {
    "mappings": {
        "properties": {
            "user_id": {"type": "integer"},
            "age": {"type": "integer"},
            "gender": {"type": "keyword"},
            "occupation": {"type": "keyword"},
            "zip_code": {"type": "keyword"},
            "age_group": {"type": "keyword"}
        }
    }
}

# Elasticsearch server URL
es_server = "http://localhost:9200"

# Create Elasticsearch index with explicit mapping if it does not exist
es = Elasticsearch([{'host': es_server.split(":")[1].replace("//", ''), 'port': int(es_server.split(":")[2]), 'scheme': 'http'}])

if not es.indices.exists(index="user_data"):
    es.indices.create(index="user_data", body=mapping)


# Write data to Elasticsearch
query = parsed_df_user_data.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .outputMode("append") \
    .option("es.resource", "user_data") \
    .option("es.nodes", es_server.split("//")[1]) \
    .option("es.port", es_server.split(":")[2]) \
    .option("es.nodes.wan.only", "false") \
    .option("es.index.auto.create", "false") \
    .option("failOnDataLoss", "false") \
    .option("checkpointLocation", "./checkpointLocation/user_data") \
    .start()

# Await termination of the query
query.awaitTermination()
