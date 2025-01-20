from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
import json
import os

load_dotenv()

# Kafka consumer configuration for Confluent Cloud
consumer = Consumer({
    'bootstrap.servers': os.getenv("KAFKA_BROKER"),
    'group.id': 'soccer-data-consumer-group',
    'auto.offset.reset': 'earliest',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("CONFLUENT_STAGE_KEY"),
    'sasl.password': os.getenv("CONFLUENT_STAGE_SECRET"),
    'retry.backoff.ms': 500,
    'session.timeout.ms': 120000,
    'max.poll.interval.ms': 300000,
    'request.timeout.ms': 60000,
})




topic = "processed_football_global_competitions"
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
