from confluent_kafka import Producer
from dotenv import load_dotenv
import requests
import json
import os

load_dotenv()

kafka_broker = os.getenv("KAFKA_BROKER")
confluent_api_key = os.getenv("CONFLUENT_STAGE_KEY")
confluent_api_secret = os.getenv("CONFLUENT_STAGE_SECRET")
sport_radar_key = os.getenv("SPORTS_RADAR_KEY")

# Configure the producer for Confluent Cloud
producer = Producer({
    'bootstrap.servers': kafka_broker,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': confluent_api_key,
    'sasl.password': confluent_api_secret,
    'client.id': 'football-data-producer',
})

# Competition API Endpoint
API_URL = 'https://api.sportradar.com/soccer/trial/v4/en/competitions.json'

def fetch_football_data(ep):
    url = API_URL.format(endpoint=ep)
    response = requests.get(url, params={'api_key': sport_radar_key})
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data: {response.status_code}, {response.text}")
        return None

def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def produce_topic(top, d):
    for record in d.get('competitions', []):
        try:
            producer.produce(
                topic=top,
                key=str(record.get('id')),
                value=json.dumps(record),
                callback=delivery_report,
            )
        except Exception as e:
            print(f"Retrying message due to error: {e}")
            producer.produce(
                topic=top,
                key=str(record.get('id')),
                value=json.dumps(record),
                callback=delivery_report,
            )
    producer.flush()


if __name__ == '__main__':
    endpoint = "Competitions"
    topic = "football_global_competitions"
    data = fetch_football_data(endpoint)
    if data:
        produce_topic(topic, data)
