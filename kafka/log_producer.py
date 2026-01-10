import json
import uuid
import random
import time
from datetime import datetime
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC = "app-logs"
SERVICES = ["payment", "order", "auth"]
STATUS_CODES = [200, 200, 200, 500, 404]

def create_kafka_producer(servers):
    """Initializes and returns a Kafka Producer instance."""
    return KafkaProducer(
        bootstrap_servers=servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks='all'
    )

def generate_log_event():
    service_name = random.choice(SERVICES)
    return service_name, {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.utcnow().isoformat(),
        "service": service_name,
        "status_code": random.choice(STATUS_CODES)
    }

def run_producer():
    producer = create_kafka_producer(BOOTSTRAP_SERVERS)
    print(f"Producer started. Sending data to {TOPIC}...")

    try:
        while True:
            service, log_data = generate_log_event()

            producer.send(
                TOPIC,
                key=service.encode("utf-8"),
                value=log_data
            )
            
            print(f"Sent: {service} - {log_data['status_code']}")
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    run_producer()