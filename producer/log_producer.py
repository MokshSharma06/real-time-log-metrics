import json
import uuid
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from src.utils import cfg
import os
import conf.env_loader
from dotenv import load_dotenv
from pathlib import Path
import os



TOPIC = cfg.kafka["topic"]
SERVICES = ["payment", "order", "auth"]
STATUS_CODES = [200, 200, 200, 500, 404]



def create_kafka_producer():
    load_dotenv()
    return KafkaProducer(
        bootstrap_servers=cfg.kafka["bootstrap_servers"],
        security_protocol=cfg.kafka.get("security_protocol", "SASL_SSL"),
        sasl_mechanism=cfg.kafka.get("sasl_mechanism", "PLAIN"),
        sasl_plain_username=cfg.kafka.get("username", "$ConnectionString"),
        sasl_plain_password=os.getenv("CONNECTION_STRING"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all"
    )


def generate_log_event(previous_event=None):
    # 10% duplicate → resend exact previous event
    if previous_event and random.random() < 0.10:
        log_data = previous_event.copy()
        log_data["producer_is_duplicate"] = True
        return log_data["service"], log_data, log_data

    # Fresh event
    service_name = random.choice(SERVICES)
    event_id = str(uuid.uuid4())

    event_time = datetime.utcnow()
    is_late = False

    # 10% late data
    if random.random() < 0.10:
        event_time -= timedelta(minutes=random.randint(10, 30))
        is_late = True

    log_data = {
        "event_id": event_id,
        "event_time": event_time.isoformat(),
        "service": service_name,
        "status_code": random.choice(STATUS_CODES),
        "producer_is_late": is_late,
        "producer_is_duplicate": False,
        "producer_is_bad": False
    }

    # 5% BAD DATA
    if random.random() < 0.05:
        corruption_type = random.choice(["missing_time", "wrong_type"])
        if corruption_type == "missing_time":
            log_data["event_time"] = None
        else:
            log_data["status_code"] = "ERROR"
        log_data["producer_is_bad"] = True

    return service_name, log_data, log_data


def run_producer():
    producer = create_kafka_producer()
    print(f"Producer started. Sending data to {TOPIC}...")

    last_event = None

    try:
        while True:
            service, log_data, last_event = generate_log_event(last_event)

            producer.send(
                TOPIC,
                key=service.encode("utf-8"),
                value=log_data
            )

            print(
                f"Sent | service={service} "
                f"| event_id={log_data['event_id']} "
                f"| late={log_data.get('producer_is_late')} "
                f"| dup={log_data.get('producer_is_duplicate')} "
                f"| bad={log_data.get('producer_is_bad')}"
            )

            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.close()


if __name__ == "__main__":
    run_producer()
