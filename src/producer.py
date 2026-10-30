"""Producer Kafka: mengirim event JSON ber-key setiap 5 detik."""
import json
import logging
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from faker import Faker

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "events_topic"
USER_KEYS = ["user_1", "user_2", "user_3", "user_4"]
INTERVAL_SECONDS = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("producer")
fake = Faker()


def build_producer():
    """Buat objek Producer yang terhubung ke broker."""
    return Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "partitioner": "murmur2_random",
        }
    )


def generate_event(user_key):
    """Buat satu payload event berupa dictionary."""
    return {
        "user_id": user_key,
        "event_type": random.choice(["click", "view", "purchase", "login"]),
        "item": fake.word(),
        "amount": round(random.uniform(5, 500), 2),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def delivery_report(err, msg):
    """Dipanggil Kafka setelah pesan berhasil/gagal terkirim."""
    if err is not None:
        logger.error("Gagal kirim: %s", err)
        return
    logger.info(
        "Terkirim key=%s partition=%d offset=%d",
        msg.key().decode(),
        msg.partition(),
        msg.offset(),
    )


def main():
    producer = build_producer()
    logger.info("Producer mulai, topic=%s", TOPIC_NAME)
    try:
        while True:
            user_key = random.choice(USER_KEYS)
            event = generate_event(user_key)
            producer.produce(
                topic=TOPIC_NAME,
                key=user_key,
                value=json.dumps(event),
                callback=delivery_report,
            )
            producer.poll(0)
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("Dihentikan oleh user.")
    finally:
        producer.flush()
        logger.info("Producer ditutup.")


if __name__ == "__main__":
    main()