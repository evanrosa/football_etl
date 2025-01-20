from confluent_kafka import Producer
from dotenv import load_dotenv
import requests
import json
import os

load_dotenv()

# Configure the producer for Confluent Cloud
producer = Producer({
    'bootstrap.servers': os.getenv("KAFKA_BROKER"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("CONFLUENT_STAGE_KEY"),
    'sasl.password': os.getenv("CONFLUENT_STAGE_SECRET"),
    'client.id': 'football-data-producer',
})

# Competition API Endpoint
API_URL = 'https://api.sportradar.com/soccer/trial/v4/en/competitions.json'

def fetch_football_data(ep):
    try:
        url = API_URL.format(endpoint=ep)
        response = requests.get(url, params={'api_key': os.getenv("SPORTS_RADAR_KEY")})
        response.raise_for_status()
        res_data = response.json()
        if res_data and "competitions" in res_data:  # Validate response is not None
            return res_data
        else:
            return None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None
    except ValueError as e:  # Handle invalid JSON response
        print(f"Invalid JSON response: {e}")
        return None


def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    elif msg:
        try:
            msg_topic = msg.topic()
            msg_partition = msg.partition()
            print(f"Message delivered to {msg_topic} [{msg_partition}]")
        except Exception as e:  # Catch any unexpected exception
            print(f"Message delivery failed: {e}")
    else:
        print("Message delivery report received with no message.")





def produce_topic(top, d, flush=True, max_retries=3):
    competitions = d.get('competitions', [])
    if not isinstance(competitions, list):
        print(f"Invalid data type for 'competitions': {type(competitions)}. Expected a list.")
        if flush:
            producer.flush()
        return

    for record in competitions:
        if not isinstance(record, dict):
            print(f"Invalid record type: {type(record)}. Skipping...")
            continue

        if not record.get('id'):
            print(f"Skipping record with missing 'id': {record}")
            continue

        try:
            json.dumps(record)  # Ensure the record is JSON serializable
        except (TypeError, ValueError) as e:
            print(f"Skipping non-serializable record: {record}. Error: {e}")
            continue

        # Handle flush=False: Skip retry logic entirely
        if not flush:
            try:
                producer.produce(
                    topic=top,
                    key=str(record.get('id')),
                    value=json.dumps(record),
                    callback=delivery_report,
                )
            except Exception as e:
                print(f"Message for record {record.get('id')} failed during benchmarking: {e}")
            continue  # Skip retries when benchmarking

        # Retry logic only if flush=True
        if flush:
            attempt = 0
            while attempt < max_retries:
                try:
                    producer.produce(
                        topic=top,
                        key=str(record.get('id')),
                        value=json.dumps(record),
                        callback=delivery_report,
                    )
                    break  # Exit retry loop on success
                except Exception as e:
                    attempt += 1
                    print(f"Retry {attempt} failed for record {record.get('id')}: {e}")
                    if attempt == max_retries:
                        print(f"Message delivery failed after {max_retries} attempts: {record.get('id')}")

    if flush:
        producer.flush()


if __name__ == '__main__':
    endpoint = "Competitions"
    topic = "football_global_competitions"
    data = fetch_football_data(endpoint)
    if data:
        produce_topic(topic, data)
