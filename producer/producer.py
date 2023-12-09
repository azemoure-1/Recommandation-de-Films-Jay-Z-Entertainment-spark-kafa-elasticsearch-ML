import requests
from confluent_kafka import Producer
import json
import time

# Kafka configuration
kafka_config = {
    "bootstrap.servers": "localhost:9092",
    "batch.num.messages": 1000,
    "delivery.report.only.error": True
}

producer = Producer(kafka_config)

def fetch_and_produce(api_endpoint, topic):
    try:
        url = f"http://127.0.0.1:5000/{api_endpoint}"
        response = requests.get(url)
        if response.status_code == 200:
            data = json.dumps(response.json())
            producer.produce(topic, key=api_endpoint, value=data)
            producer.flush()
            print(response.json())
        else:
            print(f"Failed to fetch data from {url}. Status Code: {response.status_code}")
    except Exception as e:
        print(f"Error: {e}")

# Start IDs
current_user_id = 1
current_movie_id = 1

# Define Kafka topics for each API endpoint
user_topic = "user_data"
user_ratings_topic = "user_ratings_data"
movie_topic = "movie_data"
genres_topic = "genres_data"

while True:
    fetch_and_produce(f"user/{current_user_id}", user_topic)
    fetch_and_produce(f"user_ratings/{current_user_id}", user_ratings_topic)
    fetch_and_produce(f"movie/{current_movie_id}", movie_topic)

    current_user_id += 1
    current_movie_id += 1
    time.sleep(2)
