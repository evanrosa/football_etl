from fastapi import FastAPI
from kafka import KafkaProducer, KafkaConsumer
import json

app = FastAPI()

# Kafka configuration
KAFKA_BROKER = "kafka:9092"
TOPIC_NAME = "soccer-data"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


@app.post("/produce")
def produce_message(message: dict):
    """Produce a message to Kafka."""
    producer.send(TOPIC_NAME, message)
    return {"status": "Message sent", "message": message}


@app.get("/consume")
def consume_messages():
    """Consume messages from Kafka."""
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    messages = [msg.value for msg in consumer]
    return {"messages": messages}
