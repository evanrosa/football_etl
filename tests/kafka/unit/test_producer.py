from kafka.scripts.producer import fetch_football_data, produce_topic, delivery_report
import kafka.scripts.producer
import pytest
import requests
import json
import importlib


@pytest.fixture
def mock_env_variables(monkeypatch):
    # Mock environment variables
    monkeypatch.setenv("KAFKA_BROKER", "mock_broker")
    monkeypatch.setenv("CONFLUENT_STAGE_KEY", "mock_api_key")
    monkeypatch.setenv("CONFLUENT_STAGE_SECRET", "mock_api_secret")
    monkeypatch.setenv("SPORTS_RADAR_KEY", "mock_sport_radar_key")

    # Reload the module to apply mocked environment variables
    importlib.reload(kafka.scripts.producer)

@pytest.fixture
def mock_requests(mocker):
    return mocker.patch("kafka.scripts.producer.requests.get")


def test_fetch_football_data_success(mock_env_variables, mock_requests):
    # Mock the API response with nested fields
    mock_requests.return_value.status_code = 200
    mock_requests.return_value.json.return_value = {
        "competitions": [
            {
                "id": "sr:competition:1",
                "name": "UEFA Euro",
                "gender": "men",
                "category": {
                    "id": "sr:category:4",
                    "name": "International"
                }
            },
            {
                "id": "sr:competition:7",
                "name": "UEFA Champions League",
                "gender": "men",
                "category": {
                    "id": "sr:category:393",
                    "name": "International Clubs"
                }
            }
        ]
    }

    # Call the function
    endpoint = "Competitions"
    result = fetch_football_data(endpoint)

    # Assertions
    assert result == {
        "competitions": [
            {
                "id": "sr:competition:1",
                "name": "UEFA Euro",
                "gender": "men",
                "category": {
                    "id": "sr:category:4",
                    "name": "International"
                }
            },
            {
                "id": "sr:competition:7",
                "name": "UEFA Champions League",
                "gender": "men",
                "category": {
                    "id": "sr:category:393",
                    "name": "International Clubs"
                }
            }
        ]
    }
    mock_requests.assert_called_once_with(
        "https://api.sportradar.com/soccer/trial/v4/en/competitions.json",
        params={"api_key": "mock_sport_radar_key"},
    )


def test_fetch_football_data_error(mock_requests):
    # Mock a request exception
    mock_requests.side_effect = requests.exceptions.RequestException("Mocked error")

    # Call the function
    endpoint = "Competitions"
    result = fetch_football_data(endpoint)

    # Assertions
    assert result is None
    mock_requests.assert_called_once()


@pytest.mark.parametrize("status_code, response_data, expected", [
    (400, {}, None),  # Bad request
    (404, {}, None),  # Not found
    (500, {}, None),  # Internal server error
    (200, {"unexpected_key": "unexpected_value"}, None),  # Malformed response
    (200, None, None),  # Response.json() returns None
])
def test_fetch_football_data_edge_cases(mock_requests, status_code, response_data, expected):
    # Mock the API response
    mock_requests.return_value.status_code = status_code
    if response_data is None:
        mock_requests.return_value.json.side_effect = ValueError("Invalid JSON")
    else:
        mock_requests.return_value.json.return_value = response_data

    # Call the function
    endpoint = "Competitions"
    result = fetch_football_data(endpoint)

    # Assertions
    assert result == expected

def test_fetch_football_data_large_response(mock_requests):
    # Mock a large API response
    competitions = [{"id": f"sr:competition:{i}"} for i in range(10000)]
    mock_requests.return_value.status_code = 200
    mock_requests.return_value.json.return_value = {"competitions": competitions}

    # Call the function
    endpoint = "Competitions"
    result = fetch_football_data(endpoint)

    # Assertions
    assert len(result["competitions"]) == 10000
    mock_requests.assert_called_once()

def test_fetch_football_data_empty_response(mock_requests):
    # Mock an empty response
    mock_requests.return_value.status_code = 200
    mock_requests.return_value.json.return_value = {}

    # Call the function
    endpoint = "Competitions"
    result = fetch_football_data(endpoint)

    # Assertions
    assert result is None  # Should return None for empty response
    mock_requests.assert_called_once()

def test_fetch_football_data_timeout(mock_requests):
    # Mock a timeout exception
    mock_requests.side_effect = requests.Timeout("Mocked timeout")

    # Call the function
    endpoint = "Competitions"
    result = fetch_football_data(endpoint)

    # Assertions
    assert result is None
    mock_requests.assert_called_once()

def test_fetch_football_data_non_json_response(mock_requests):
    # Mock a non-JSON response
    mock_requests.return_value.status_code = 200
    mock_requests.return_value.text = "<html>Not JSON</html>"
    mock_requests.return_value.json.side_effect = ValueError("Invalid JSON")

    # Call the function
    endpoint = "Competitions"
    result = fetch_football_data(endpoint)

    # Assertions
    assert result is None  # Should gracefully handle non-JSON response
    mock_requests.assert_called_once()


def test_delivery_report_success(mocker):
    # Mock the message
    mock_msg = mocker.Mock()
    mock_msg.topic.return_value = "mock_topic"
    mock_msg.partition.return_value = 1

    # Capture the print output
    mock_print = mocker.patch("builtins.print")

    # Call the function
    delivery_report(None, mock_msg)

    # Assertions
    mock_print.assert_called_once_with("Message delivered to mock_topic [1]")


def test_delivery_report_failure(mocker):
    # Capture the print output
    mock_print = mocker.patch("builtins.print")

    # Call the function
    delivery_report("Mock error", None)

    # Assertions
    mock_print.assert_called_once_with("Message delivery failed: Mock error")

def test_delivery_report_with_none_arguments(mocker):
    # Capture the print output
    mock_print = mocker.patch("builtins.print")

    # Call the function with None values
    delivery_report(None, None)

    # Assertions
    mock_print.assert_called_once_with("Message delivery report received with no message.")

def test_delivery_report_partial_message(mocker):
    # Mock a message with missing attributes
    mock_msg = mocker.Mock()
    mock_msg.topic.side_effect = AttributeError("Missing topic attribute")
    mock_msg.partition.return_value = 1

    # Capture the print output
    mock_print = mocker.patch("builtins.print")

    # Call the function
    delivery_report(None, mock_msg)

    # Assertions
    mock_print.assert_called_once_with("Message delivery failed: Missing topic attribute")

def test_delivery_report_unexpected_exception(mocker):
    # Mock a message with a broken topic method
    mock_msg = mocker.Mock()
    mock_msg.topic.side_effect = Exception("Unexpected exception")
    mock_msg.partition.return_value = 1

    # Capture the print output
    mock_print = mocker.patch("builtins.print")

    # Call the function
    delivery_report(None, mock_msg)

    # Assertions
    mock_print.assert_called_once_with("Message delivery failed: Unexpected exception")


@pytest.mark.parametrize("data", [
    ({"competitions": [{"id": "sr:competition:1", "name": "UEFA Euro", "gender": "men"}]}),
    ({"competitions": [
        {"id": "sr:competition:2", "name": "FIFA World Cup", "gender": "men"},
        {"id": "sr:competition:3", "name": "Copa America", "gender": "men"}
    ]}),
    ({"competitions": []}),
])
def test_produce_topic_success(mock_producer, mocker, data):
    # Mock the delivery_report function
    mocker.patch("kafka.scripts.producer.delivery_report")

    # Call the function
    topic = "mock_topic"
    produce_topic(topic, data)

    # Assertions
    for record in data["competitions"]:
        mock_producer.produce.assert_any_call(
            topic=topic,
            key=str(record["id"]),
            value=json.dumps(record),
            callback=mocker.ANY,
        )
    mock_producer.flush.assert_called_once()


def test_produce_topic_no_data(mock_producer):
    # Call the function with empty data
    topic = "mock_topic"
    produce_topic(topic, {})

    # Assertions
    mock_producer.produce.assert_not_called()
    mock_producer.flush.assert_called_once()

def test_produce_topic_multiple_retries(mock_producer):
    # Mock produce to fail three times and then succeed
    mock_producer.produce.side_effect = iter([
        Exception("Mock error"),  # First failure
        Exception("Mock error"),  # Second failure
        Exception("Mock error"),  # Third failure
        None  # Success on the fourth attempt
    ])

    data = {
        "competitions": [
            {"id": "sr:competition:1", "name": "UEFA Euro", "gender": "men"}
        ]
    }
    topic = "mock_topic"

    # Call the function with max_retries=4
    produce_topic(topic, data, max_retries=4)

    # Assertions
    assert mock_producer.produce.call_count == 4  # Initial attempt + 3 retries
    mock_producer.flush.assert_called_once()

def test_produce_topic_retries_exceeded(mock_producer):
    # Mock produce to fail on all attempts
    mock_producer.produce.side_effect = Exception("Mock error")

    data = {
        "competitions": [
            {"id": "sr:competition:1", "name": "UEFA Euro", "gender": "men"}
        ]
    }
    topic = "mock_topic"

    # Call the function
    produce_topic(topic, data, max_retries=3)

    # Assertions
    assert mock_producer.produce.call_count == 3  # Three failed attempts
    mock_producer.flush.assert_called_once()

def test_produce_topic_mixed_records(mock_producer, mocker):
    data = {
        "competitions": [
            {"id": "valid", "name": "Valid Competition"},  # Valid record
            "Not a dictionary",  # Invalid record
            {"name": "Missing ID"}  # Invalid record: Missing ID
        ]
    }
    topic = "mock_topic"

    # Mock the delivery_report function
    mocker.patch("kafka.scripts.producer.delivery_report")

    # Call the function
    produce_topic(topic, data)

    # Assertions
    mock_producer.produce.assert_called_once_with(
        topic=topic,
        key="valid",
        value=json.dumps({"id": "valid", "name": "Valid Competition"}),
        callback=mocker.ANY,
    )
    mock_producer.flush.assert_called_once()


def test_produce_topic_serialization(mock_producer, mocker):
    data = {
        "competitions": [
            {
                "id": "sr:competition:1",
                "name": "UEFA Euro",
                "gender": "men",
                "category": {"id": "sr:category:4", "name": "International"}
            }
        ]
    }
    topic = "mock_topic"

    # Call the function
    produce_topic(topic, data)

    # Assertions
    mock_producer.produce.assert_called_once_with(
        topic=topic,
        key="sr:competition:1",
        value=json.dumps(data["competitions"][0]),
        callback=mocker.ANY,
    )

@pytest.mark.benchmark
def test_produce_topic_performance(mock_producer, benchmark):
    data = {
        "competitions": [{"id": f"sr:competition:{i}"} for i in range(1000)]
    }
    topic = "mock_topic"

    # Benchmark the produce_topic function without flushing
    benchmark(produce_topic, topic, data, flush=False)

    # Manually call flush after benchmarking
    produce_topic(topic, data, flush=True)

    # Ensure flush is called exactly once after the benchmark
    mock_producer.flush.assert_called_once()

def test_produce_topic_invalid_data(mock_producer):
    data = {"competitions": [{"name": "Missing ID"}]}  # Missing 'id'
    topic = "mock_topic"

    # Call the function
    produce_topic(topic, data)

    # Assertions
    mock_producer.produce.assert_not_called()
    mock_producer.flush.assert_called_once()

def test_produce_topic_empty_payload(mock_producer):
    # Call the function with an empty payload
    topic = "mock_topic"
    produce_topic(topic, {})

    # Assertions
    mock_producer.produce.assert_not_called()
    mock_producer.flush.assert_called_once()

def test_produce_topic_invalid_data_types(mock_producer):
    data = {"competitions": "This should be a list, not a string"}  # Invalid data type
    topic = "mock_topic"

    # Call the function
    produce_topic(topic, data)

    # Assertions
    mock_producer.produce.assert_not_called()
    mock_producer.flush.assert_called_once()

def test_produce_topic_invalid_records(mock_producer):
    data = {"competitions": ["Not a dictionary", 123, None]}  # Invalid record types
    topic = "mock_topic"

    # Call the function
    produce_topic(topic, data)

    # Assertions
    mock_producer.produce.assert_not_called()
    mock_producer.flush.assert_called_once()

@pytest.mark.benchmark
def test_produce_topic_large_payload_benchmark(mock_producer, benchmark):
    data = {
        "competitions": [{"id": f"sr:competition:{i}"} for i in range(100000)]  # Large dataset
    }
    topic = "mock_topic"

    # Benchmark the produce_topic function without flushing
    benchmark(produce_topic, topic, data, flush=False)

    # Manually call flush on the mocked producer after benchmarking
    mock_producer.flush()

    # Ensure flush is called exactly once
    mock_producer.flush.assert_called_once()

def test_produce_topic_nested_invalid_records(mock_producer):
    # Invalid nested record
    data = {"competitions": [{"id": "sr:competition:1", "details": {"object": object()}}]}  # Non-serializable field
    topic = "mock_topic"

    # Call the function
    produce_topic(topic, data)

    # Assertions
    mock_producer.produce.assert_not_called()
    mock_producer.flush.assert_called_once()

def test_produce_topic_large_payload_mixed_records(mock_producer):
    data = {
        "competitions": [
            {"id": f"sr:competition:{i}"} if i % 2 == 0 else {"name": f"Invalid {i}"}
            for i in range(10000)
        ]
    }
    topic = "mock_topic"

    # Call the function
    produce_topic(topic, data)

    # Assertions
    assert mock_producer.produce.call_count == 5000  # Only valid records are processed
    mock_producer.flush.assert_called_once()




