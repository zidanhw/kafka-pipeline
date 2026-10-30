"""Consumer Kafka: baca event, hitung per user, simpan ke SQLite."""
import argparse
import json
import logging
import sqlite3
from collections import Counter
from pathlib import Path

from confluent_kafka import Consumer, KafkaException

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "events_topic"
GROUP_ID = "events_consumer_group"
DB_PATH = "data/events.db"
SUMMARY_PATH = "data/summary.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("consumer")


def parse_args():
    """Baca nama consumer dari argumen terminal."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--consumer-id", default="consumer_1")
    return parser.parse_args()


def build_consumer():
    return Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def log_assignment(consumer, partitions):
    """Dipanggil saat consumer mendapat partisi (bahan laporan)."""
    logger.info("ASSIGNED partitions: %s", [p.partition for p in partitions])


def log_revocation(consumer, partitions):
    """Dipanggil saat partisi diambil dari consumer (rebalance)."""
    logger.info("REVOKED partitions: %s", [p.partition for p in partitions])


def init_db():
    Path(DB_PATH).parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_events (
            topic TEXT, partition INTEGER, offset INTEGER,
            user_key TEXT, event_type TEXT, amount REAL,
            consumer_id TEXT,
            PRIMARY KEY (topic, partition, offset)
        )
        """
    )
    conn.commit()
    return conn


def save_event(conn, msg, payload, consumer_id):
    """INSERT OR IGNORE: aman kalau pesan yang sama terbaca ulang."""
    conn.execute(
        "INSERT OR IGNORE INTO processed_events VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            msg.topic(), msg.partition(), msg.offset(),
            msg.key().decode(), payload["event_type"], payload["amount"],
            consumer_id,
        ),
    )
    conn.commit()


def export_summary(conn):
    """Tulis agregasi per user ke file JSON."""
    rows = conn.execute(
        "SELECT user_key, COUNT(*), ROUND(SUM(amount), 2) "
        "FROM processed_events GROUP BY user_key ORDER BY user_key"
    ).fetchall()
    summary = [
        {"user_key": u, "event_count": c, "total_amount": t} for u, c, t in rows
    ]
    with open(SUMMARY_PATH, "w", encoding="utf-8") as file:
        json.dump(summary, file, indent=2)


def process_message(msg, consumer_id, counter, conn):
    payload = json.loads(msg.value().decode("utf-8"))
    user_key = msg.key().decode()
    counter[user_key] += 1
    save_event(conn, msg, payload, consumer_id)
    logger.info(
        "[%s] key=%s partition=%d offset=%d | count_lokal=%s",
        consumer_id, user_key, msg.partition(), msg.offset(), dict(counter)
    )

def main():
    args = parse_args()
    consumer = build_consumer()
    conn = init_db()
    counter = Counter()
    consumer.subscribe(
        [TOPIC_NAME], on_assign=log_assignment, on_revoke=log_revocation
    )
    logger.info("[%s] mulai, group=%s", args.consumer_id, GROUP_ID)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            try:
                process_message(msg, args.consumer_id, counter, conn)
                consumer.commit(message=msg, asynchronous=False)
            except (json.JSONDecodeError, KeyError) as exc:
                logger.warning("Pesan dilewati (offset=%d): %s", msg.offset(), exc)
    except KeyboardInterrupt:
        logger.info("Dihentikan oleh user.")
    except KafkaException as exc:
        logger.error("Kafka error: %s", exc)
    finally:
        export_summary(conn)
        conn.close()
        consumer.close()
        logger.info("[%s] ditutup. Total: %s", args.consumer_id, dict(counter))


if __name__ == "__main__":
    main()