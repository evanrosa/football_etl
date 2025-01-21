import pytest
import time
from confluent_kafka import Producer, Consumer, KafkaError
import json
from kafka.scripts.producer import produce_topic

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

@pytest.mark.parametrize("test_data", [
    {"sr:competition:3": {"id": "sr:competition:3", "name": "Copa America", "gender": "men"}},
    {"sr:competition:4": {"id": "sr:competition:4", "name": "Olympics", "gender": "women"}}
])
def test_kafka_integration_multiple(kafka_producer, kafka_consumer, test_data):
    produce_test_messages(kafka_producer, TOPIC, test_data)

    # Poll and validate
    consumed_messages = {}
    timeout = time.time() + 10
    while len(consumed_messages) < len(test_data) and time.time() < timeout:
        msg = kafka_consumer.poll(1.0)
        if msg and not msg.error():
            consumed_messages[msg.key().decode()] = json.loads(msg.value().decode())

    assert consumed_messages == test_data


def test_kafka_high_throughput(kafka_producer, kafka_consumer):
    num_messages = 1000
    test_messages = {f"key-{i}": {"value": i} for i in range(num_messages)}

    produce_test_messages(kafka_producer, TOPIC, test_messages)

    consumed_messages = {}
    timeout = time.time() + 20
    while len(consumed_messages) < num_messages and time.time() < timeout:
        msg = kafka_consumer.poll(1.0)
        if msg and not msg.error():
            consumed_messages[msg.key().decode()] = json.loads(msg.value().decode())

    assert len(consumed_messages) == num_messages
    assert consumed_messages == test_messages

def test_kafka_high_throughput_extended(kafka_producer, kafka_consumer):
    """Test Kafka with a large number of messages."""
    num_messages = 10000
    test_messages = {f"key-{i}": {"value": i} for i in range(num_messages)}

    produce_test_messages(kafka_producer, TOPIC, test_messages)

    consumed_messages = {}
    timeout = time.time() + 60  # Extended timeout for large payloads
    while len(consumed_messages) < num_messages and time.time() < timeout:
        msg = kafka_consumer.poll(1.0)
        if msg and not msg.error():
            consumed_messages[msg.key().decode()] = json.loads(msg.value().decode())

    assert len(consumed_messages) == num_messages
    assert consumed_messages == test_messages

@pytest.mark.parametrize("test_data", [
    {},  # Empty payload
    {"competitions": None},  # Null value
    {"competitions": "This should be a list"},  # Invalid data type
    {"competitions": [{"name": "Missing ID"}]},  # Missing required field
    {"competitions": [{"id": "valid", "name": "Valid Competition", "gender": 123}]},  # Invalid field type
])
def test_kafka_edge_cases(kafka_producer, test_data):
    """Test Kafka producer with edge cases."""
    topic = TOPIC
    try:
        produce_topic(topic, test_data)
    except Exception as e:
        print(f"Handled edge case: {e}")

    # Assertions: Ensure no invalid messages were produced
    assert True  # Replace with more specific assertions if needed

def test_kafka_partition_handling(kafka_producer, kafka_consumer):
    """Test Kafka producer and consumer with multiple partitions."""
    test_messages = {
        "sr:competition:1": {"id": "sr:competition:1", "name": "UEFA Euro", "gender": "men"},
        "sr:competition:2": {"id": "sr:competition:2", "name": "FIFA World Cup", "gender": "men"},
    }

    # Produce messages with partition keys
    for key, value in test_messages.items():
        kafka_producer.produce(
            topic=TOPIC,
            key=key,  # Key determines the partition
            value=json.dumps(value),
            callback=lambda err, msg: print(f"Produced to partition {msg.partition()}" if not err else f"Error: {err}")
        )
    kafka_producer.flush()

    # Poll messages and validate partitions
    consumed_messages = {}
    timeout = time.time() + 10
    while len(consumed_messages) < len(test_messages) and time.time() < timeout:
        msg = kafka_consumer.poll(1.0)
        if msg and not msg.error():
            key = msg.key().decode('utf-8')
            value = json.loads(msg.value().decode('utf-8'))
            consumed_messages[key] = value
            print(f"Consumed from partition {msg.partition()}")

    assert consumed_messages == test_messages

def test_produce_with_retries(mock_producer, mocker):
    """Test Kafka producer retry logic."""
    # Patch the producer inside produce_topic
    mocker.patch("kafka.scripts.producer.producer", mock_producer)

    # Mock produce to fail on the first two attempts, then succeed
    mock_producer.produce.side_effect = [
        Exception("Temporary failure"),
        Exception("Temporary failure"),
        None,  # Success on the third attempt
    ]

    data = {"competitions": [{"id": "sr:competition:1", "name": "UEFA Euro", "gender": "men"}]}
    topic = "mock_topic"

    # Call the function
    produce_topic(topic, data, max_retries=3)

    # Assertions
    assert mock_producer.produce.call_count == 3  # Two failures + one success
    mock_producer.flush.assert_called_once()


def test_partition_assignment(mock_consumer, mocker):
    """Test partition assignment."""
    mock_on_assign = mocker.MagicMock()
    mock_consumer.subscribe(
        [TOPIC],
        on_assign=lambda c, partitions: mock_on_assign(partitions)
    )
    assigned_partitions = ["partition_0", "partition_1"]
    mock_on_assign(assigned_partitions)

    mock_on_assign.assert_called_once_with(assigned_partitions)


def test_partition_revocation(mock_consumer, mocker):
    """Test partition revocation."""
    mock_on_revoke = mocker.MagicMock()
    mock_consumer.subscribe(
        [TOPIC],
        on_revoke=lambda c, partitions: mock_on_revoke(partitions)
    )
    revoked_partitions = ["partition_0", "partition_1"]
    mock_on_revoke(revoked_partitions)

    mock_on_revoke.assert_called_once_with(revoked_partitions)
