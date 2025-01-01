from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
import json
import os

load_dotenv()

kafka_broker = os.getenv("KAFKA_BROKER")
confluent_api_key = os.getenv("CONFLUENT_STAGE_KEY")
confluent_api_secret = os.getenv("CONFLUENT_STAGE_SECRET")
sport_radar_key = os.getenv("SPORTS_RADAR_KEY")

# Kafka consumer configuration for Confluent Cloud
consumer = Consumer({
    'bootstrap.servers': kafka_broker,  # Replace with your Confluent Cloud broker
    'group.id': 'soccer-data-consumer-group',
    'auto.offset.reset': 'earliest',  # Start from the beginning if no offset is committed
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': confluent_api_key,  # Replace with your Confluent API key
    'sasl.password': confluent_api_secret,  # Replace with your Confluent API secret
})

topic = "football_global_competitions"
consumer.subscribe([topic])

def process_message(message):
    try:
        data = json.loads(message.value().decode("utf-8"))
        # Process the data (e.g., send to Flink, store in DB, etc.)
        print(f"Processing data: {data}")
    except Exception as e:
        print(f"Failed to process message: {e}")

if __name__ == "__main__":
    try:
        while True:
            msg = consumer.poll(1.0)  # Poll for messages
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"End of partition reached {msg.topic()} {msg.partition()}")
                else:
                    print(f"Error: {msg.error()}")
            else:
                process_message(msg)
    except KeyboardInterrupt:
        print("Consumer shutdown")
    finally:
        consumer.close()
