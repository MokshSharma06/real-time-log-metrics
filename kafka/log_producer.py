import json
import time
import uuid
import random
from datetime import datetime

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "app-logs"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

SERVICES = ["payment", "order", "auth"]
STATUS_CODES = [200, 200, 200, 500]

print("Kafka log producer started...")

while True:
    log = {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.utcnow().isoformat(),
        "service": random.choice(SERVICES),
        "status_code": random.choice(STATUS_CODES)
    }

    producer.send(TOPIC, value=log)
    producer.flush()

    print("Sent:", log)
    time.sleep(1)
