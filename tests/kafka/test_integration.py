from testcontainers.kafka import KafkaContainer
from confluent_kafka import Producer, Consumer
import pytest
import json

@pytest.fixture(scope="module")
def kafka_container():
    with KafkaContainer() as kafka:
        kafka.start()
        yield kafka.get_bootstrap_server()

def test_producer_consumer_integration(kafka_container):
    # Kafka configuration
    topic = "test_topic"
    producer_config = {
        'bootstrap.servers': kafka_container,
    }
    consumer_config = {
        'bootstrap.servers': kafka_container,
        'group.id': 'test_group',
        'auto.offset.reset': 'earliest',
    }

    # Create a producer
    producer = Producer(producer_config)
    producer.produce(topic, key="1", value=json.dumps({"id": "1", "name": "Premier League"}))
    producer.flush()

    # Create a consumer
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])

    msg = consumer.poll(10)
    assert msg is not None
    assert json.loads(msg.value()) == {"id": "1", "name": "Premier League"}
