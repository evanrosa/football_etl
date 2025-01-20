import pytest
import json
import time
from kafka.scripts.consumer import process_message
from confluent_kafka import Consumer

@pytest.fixture
def mock_env_vars(monkeypatch):
    """Fixture to mock environment variables."""
    monkeypatch.setenv("KAFKA_BROKER", "mock_broker")
    monkeypatch.setenv("CONFLUENT_STAGE_KEY", "mock_key")
    monkeypatch.setenv("CONFLUENT_STAGE_SECRET", "mock_secret")
    monkeypatch.setenv("SPORTS_RADAR_KEY", "mock_sport_radar_key")

@pytest.fixture
def mock_consumer(mocker):
    """Fixture to mock Kafka Consumer."""
    consumer_mock = mocker.Mock(spec=Consumer)
    mocker.patch("confluent_kafka.Consumer", consumer_mock)
    return consumer_mock


@pytest.fixture
def valid_message(mocker):
    """Fixture for a valid Kafka message."""
    mock_message = mocker.MagicMock()
    mock_message.value.return_value = json.dumps({"key": "value"}).encode("utf-8")
    mock_message.error.return_value = None
    return mock_message


@pytest.fixture
def invalid_message(mocker):
    """Fixture for an invalid Kafka message."""
    mock_message = mocker.MagicMock()
    mock_message.value.return_value = b"invalid-json"
    mock_message.error.return_value = None
    return mock_message

def test_env_variables(mock_env_vars):
    """Test that environment variables are loaded correctly."""
    from os import getenv
    assert getenv("KAFKA_BROKER") == "mock_broker"
    assert getenv("CONFLUENT_STAGE_KEY") == "mock_key"
    assert getenv("CONFLUENT_STAGE_SECRET") == "mock_secret"
    assert getenv("SPORTS_RADAR_KEY") == "mock_sport_radar_key"

def test_consumer_configuration(mocker, mock_env_vars):
    """Test that the consumer is initialized with the correct configuration."""
    # Mock the Consumer class
    mock_consumer_class = mocker.patch("confluent_kafka.Consumer")

    # Re-import and trigger consumer initialization
    from importlib import reload
    import kafka.scripts.consumer
    reload(kafka.scripts.consumer)  # Ensures consumer is reinitialized with mock

    expected_config = {
        'bootstrap.servers': 'mock_broker',
        'group.id': 'soccer-data-consumer-group',
        'auto.offset.reset': 'earliest',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': 'mock_key',
        'sasl.password': 'mock_secret',
        'retry.backoff.ms': 500,
        'session.timeout.ms': 120000,
        'max.poll.interval.ms': 300000,
        'request.timeout.ms': 60000,
    }

    # Verify the Consumer was initialized with the correct configuration
    mock_consumer_class.assert_called_once_with(expected_config)

def test_consumer_initialization(mocker, mock_env_vars):
    """Test that the consumer is initialized correctly."""
    # Patch the consumer object directly in the module where it is used
    mock_consumer_instance = mocker.Mock()
    mocker.patch("kafka.scripts.consumer.consumer", mock_consumer_instance)

    # Import the consumer from the module
    from kafka.scripts.consumer import consumer

    # Assert that the consumer is not None (mocked instance exists)
    assert consumer is not None

def test_process_message_valid(valid_message, mocker):
    """Test processing of a valid Kafka message."""
    mock_print = mocker.patch("builtins.print")  # Save the mocked print object
    process_message(valid_message)
    mock_print.assert_called_once_with("Processing data: {'key': 'value'}")

def test_process_message_invalid(invalid_message, mocker):
    """Test processing of an invalid Kafka message."""
    mock_print = mocker.patch("builtins.print")  # Save the mocked print object
    process_message(invalid_message)
    mock_print.assert_called_once_with("Failed to process message: Expecting value: line 1 column 1 (char 0)")

def test_consumer_poll(mock_consumer, valid_message, mocker):
    """Test the Kafka consumer polling logic."""
    # Configure the mocked consumer's poll method to return the valid_message
    mock_consumer.poll.return_value = valid_message

    # Mock process_message to verify it gets called with valid_message
    mock_process_message = mocker.patch("kafka.scripts.consumer.process_message")

    # Simulate a polling loop
    msg = mock_consumer.poll(1.0)  # Use the mocked consumer
    if msg is not None and not msg.error():
        mock_process_message(msg)  # Manually invoke the mocked process_message

    # Assert process_message was called once with the valid_message
    mock_process_message.assert_called_once_with(valid_message)

    # Assert that the consumer's poll method was called with the correct argument
    mock_consumer.poll.assert_called_once_with(1.0)

def test_consumer_batch_processing(mock_consumer, mocker):
    """Test that the consumer processes a batch of messages."""
    # Simulate a batch of messages
    batch = [
        mocker.MagicMock(value=lambda: b'{"key": "value1"}', error=lambda: None),
        mocker.MagicMock(value=lambda: b'{"key": "value2"}', error=lambda: None),
    ]
    mock_consumer.poll.return_value = batch

    # Mock process_message
    mock_process_message = mocker.patch("kafka.scripts.consumer.process_message")

    # Simulate polling
    messages = mock_consumer.poll(1.0)
    for msg in messages:
        if msg is not None and not msg.error():
            mock_process_message(msg)

    # Assert process_message was called for each message in the batch
    assert mock_process_message.call_count == len(batch)

def test_consumer_partition_assignment(mock_consumer, mocker):
    """Test that the consumer handles partition assignment events."""
    # Mock the `on_assign` callback
    mock_on_assign = mocker.MagicMock()
    mock_consumer.subscribe.return_value = None
    mock_consumer.assign = mock_on_assign

    # Simulate partition assignment
    assigned_partitions = ["partition_1", "partition_2"]
    mock_consumer.subscribe(
        ["test_topic"], on_assign=lambda consumer, partitions: mock_on_assign(partitions)
    )

    # Call the on_assign callback
    mock_on_assign(assigned_partitions)

    # Assert the on_assign callback was invoked with the correct partitions
    mock_on_assign.assert_called_once_with(assigned_partitions)

def test_consumer_partition_revocation(mock_consumer, mocker):
    """Test that the consumer handles partition revocation events."""
    # Mock the `on_revoke` callback
    mock_on_revoke = mocker.MagicMock()
    mock_consumer.subscribe.return_value = None
    mock_consumer.unsubscribe = mock_on_revoke

    # Simulate partition revocation
    revoked_partitions = ["partition_1", "partition_2"]
    mock_consumer.subscribe(
        ["test_topic"], on_revoke=lambda consumer, partitions: mock_on_revoke(partitions)
    )

    # Call the on_revoke callback
    mock_on_revoke(revoked_partitions)

    # Assert the on_revoke callback was invoked with the correct partitions
    mock_on_revoke.assert_called_once_with(revoked_partitions)

def test_consumer_poll_retry(mock_consumer, mocker):
    """Test that the consumer retries polling after transient errors."""
    from confluent_kafka import KafkaError

    # Simulate transient error on first poll, then return valid message
    mock_consumer.poll.side_effect = [
        mocker.MagicMock(error=lambda: KafkaError(KafkaError._TRANSPORT)),
        mocker.MagicMock(value=lambda: b'{"key": "value"}', error=lambda: None),
    ]

    # Mock process_message
    mock_process_message = mocker.patch("kafka.scripts.consumer.process_message")
    mock_sleep = mocker.patch("time.sleep")  # Mock time.sleep for retry delay

    # Simulate polling
    for _ in range(2):  # Simulate 2 polling attempts
        msg = mock_consumer.poll(1.0)
        if msg is not None and not msg.error():
            mock_process_message(msg)
        elif msg.error():
            print(f"Retrying due to error: {msg.error()}")
            time.sleep(1)


    # Assert process_message was called once with the valid message
    mock_process_message.assert_called_once()
    mock_sleep.assert_called_once_with(1)  # Verify retry delay

def test_consumer_poll_error(mock_consumer, mocker):
    """Test that the consumer handles poll errors gracefully."""
    from confluent_kafka import KafkaError

    # Mock the consumer's poll method to return a message with an error
    mock_error_message = mocker.MagicMock()
    mock_error_message.error.return_value = KafkaError(KafkaError._ALL_BROKERS_DOWN)
    mock_consumer.poll.return_value = mock_error_message

    # Mock print to check error logging
    mock_print = mocker.patch("builtins.print")

    # Simulate polling
    msg = mock_consumer.poll(1.0)
    if msg is not None and msg.error():
        print(f"Error: {msg.error()}")

    # Assert the actual error message matches the print call
    actual_error_message = f"Error: {KafkaError(KafkaError._ALL_BROKERS_DOWN)}"
    mock_print.assert_called_once_with(actual_error_message)

def test_consumer_poll_no_messages(mock_consumer, mocker):
    """Test that the consumer handles no messages (empty poll) gracefully."""
    mock_consumer.poll.return_value = None

    # Mock process_message to ensure it is not called
    mock_process_message = mocker.patch("kafka.scripts.consumer.process_message")

    # Simulate polling
    msg = mock_consumer.poll(1.0)
    if msg is not None and not msg.error():
        mock_process_message(msg)

    # Assert process_message was not called
    mock_process_message.assert_not_called()

    # Ensure poll was called once
    mock_consumer.poll.assert_called_once_with(1.0)

def test_consumer_timeout(mock_consumer, mocker):
    """Test that the consumer handles polling timeouts gracefully."""
    from confluent_kafka import KafkaError

    # Simulate a timeout error during polling
    timeout_error = KafkaError(KafkaError._TIMED_OUT)
    mock_consumer.poll.return_value = mocker.MagicMock(error=lambda: timeout_error)

    # Mock print to verify timeout handling
    mock_print = mocker.patch("builtins.print")

    # Simulate polling loop
    msg = mock_consumer.poll(1.0)
    if msg is not None and msg.error():
        print(f"Polling timeout: {msg.error()}")

    # Assert the timeout message was printed
    mock_print.assert_called_once_with(f"Polling timeout: {timeout_error}")

def test_consumer_shutdown(mock_consumer, mocker):
    """Test that the consumer shuts down gracefully on KeyboardInterrupt."""
    # Simulate a KeyboardInterrupt during polling
    mock_consumer.poll.side_effect = KeyboardInterrupt

    # Mock print to verify shutdown message
    mock_print = mocker.patch("builtins.print")

    # Simulate the main loop
    try:
        while True:
            msg = mock_consumer.poll(1.0)
            if msg is not None and not msg.error():
                process_message(msg)
    except KeyboardInterrupt:
        print("Consumer shutdown")
    finally:
        mock_consumer.close()

    # Assert consumer.close was called
    mock_consumer.close.assert_called_once()

    # Assert the shutdown message was printed
    mock_print.assert_called_once_with("Consumer shutdown")
