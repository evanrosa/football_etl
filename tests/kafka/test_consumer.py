import pytest
from kafka.scripts.consumer import process_message
from unittest.mock import MagicMock

def test_process_message_valid():
    # Mock a valid Kafka message
    mock_message = MagicMock()
    mock_message.value.return_value = b'{"id":"1","region":"Europe","name":"Premier League"}'

    # Call the function
    process_message(mock_message)

    # Verify the output (you can customize based on your `process_message` implementation)
    assert True  # Add assertions for side effects if needed

def test_process_message_invalid():
    # Mock an invalid Kafka message
    mock_message = MagicMock()
    mock_message.value.return_value = b'invalid json'

    # Call the function and ensure no exception is raised
    try:
        process_message(mock_message)
    except Exception as e:
        pytest.fail(f"process_message raised an exception: {e}")
