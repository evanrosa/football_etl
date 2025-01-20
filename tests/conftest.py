import subprocess
import pytest
import time
import socket

DOCKER_COMPOSE_FILE = "tests/kafka/integration/docker-compose.integration.yaml"

def is_kafka_ready(host: str, port: int, timeout: int = 10) -> bool:
    """Check if Kafka is ready to accept connections."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except (socket.error, ConnectionRefusedError):
            time.sleep(1)
    return False

@pytest.fixture(scope="session", autouse=True)
def kafka_docker_environment():
    """Use the existing Kafka environment for integration tests."""
    print("Reusing the existing Kafka instance. No additional containers are started.")
    yield
