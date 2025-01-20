import pytest
import time
from confluent_kafka import Producer, Consumer, KafkaError
import json

TOPIC = "football_global_competitions"

@pytest.fixture(scope="module")
def kafka_producer():
    producer = Producer({
        'bootstrap.servers': 'localhost:9092',  # Use localhost for external connections
        'security.protocol': 'PLAINTEXT',       # Ensure the protocol matches the Kafka setup
        'client.id': 'test-producer',
    })
    yield producer
    producer.flush()

@pytest.fixture(scope="module")
def kafka_consumer():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',  # Use localhost for external connections
        'group.id': 'test-consumer-group',
        'auto.offset.reset': 'earliest',
        'security.protocol': 'PLAINTEXT',
    })
    consumer.subscribe([TOPIC])  # Subscribe to the correct topic
    yield consumer
    consumer.close()


def produce_test_messages(producer, topic, messages):
    """Helper function to produce test messages."""
    for key, value in messages.items():
        producer.produce(
            topic=topic,
            key=str(key),
            value=json.dumps(value),
            callback=lambda err, msg: print(f"Produced: {msg.value()}" if not err else f"Error: {err}")
        )
    producer.flush()  # Ensure all messages are sent
    print(f"Finished producing messages to topic {topic}")


def test_kafka_integration(kafka_producer, kafka_consumer):
    """Integration test for Kafka producer and consumer."""
    print("Starting Kafka integration test...")

    # Test data to produce
    test_messages = {
        "sr:competition:1": {"id": "sr:competition:1", "name": "UEFA Euro", "gender": "men"},
        "sr:competition:2": {"id": "sr:competition:2", "name": "FIFA World Cup", "gender": "men"},
    }

    # Produce messages to Kafka
    produce_test_messages(kafka_producer, TOPIC, test_messages)

    # Poll for messages from Kafka
    print("Polling messages...")
    consumed_messages = {}
    timeout = time.time() + 10  # 10 seconds timeout
    while len(consumed_messages) < len(test_messages) and time.time() < timeout:
        msg = kafka_consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaError(msg.error())
        else:
            key = msg.key().decode('utf-8')
            value = json.loads(msg.value().decode('utf-8'))
            consumed_messages[key] = value

    print("Consumed messages:", consumed_messages)
    # Assertions
    assert consumed_messages == test_messages  # Verify all messages were consumed
