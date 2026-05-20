from __future__ import annotations

import csv
import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError


FIELD_ORDER = [
    "id", "customer_first_name", "customer_last_name", "customer_age",
    "customer_email", "customer_country", "customer_postal_code",
    "customer_pet_type", "customer_pet_name", "customer_pet_breed",
    "seller_first_name", "seller_last_name", "seller_email",
    "seller_country", "seller_postal_code", "product_name",
    "product_category", "product_price", "product_quantity",
    "sale_date", "sale_customer_id", "sale_seller_id",
    "sale_product_id", "sale_quantity", "sale_total_price",
    "store_name", "store_location", "store_city", "store_state",
    "store_country", "store_phone", "store_email", "pet_category",
    "product_weight", "product_color", "product_size", "product_brand",
    "product_material", "product_description", "product_rating",
    "product_reviews", "product_release_date", "product_expiry_date",
    "supplier_name", "supplier_contact", "supplier_email",
    "supplier_phone", "supplier_address", "supplier_city", "supplier_country",
]

INT_FIELDS = {"id", "customer_age", "product_quantity", "sale_customer_id",
              "sale_seller_id", "sale_product_id", "sale_quantity", "product_reviews"}

DECIMAL_FIELDS = {"product_price", "sale_total_price", "product_weight", "product_rating"}


@dataclass(frozen=True)
class Settings:
    bootstrap: str
    topic: str
    data_dir: Path
    delay_sec: float


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value and value.strip() else default


def load_settings() -> Settings:
    return Settings(
        bootstrap=_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        topic=_env("KAFKA_TOPIC", "sales_topic"),
        data_dir=Path(_env("CSV_DIR", "/app/data")),
        delay_sec=float(_env("PRODUCER_SLEEP_SEC", "0")),
    )


def _coerce(name: str, raw: str) -> Any:
    text = (raw or "").strip()
    if not text:
        return None
    if name in INT_FIELDS:
        return int(float(text))
    if name in DECIMAL_FIELDS:
        return float(text)
    return text


def _natural_csv_sort(path: Path) -> tuple[int, str]:
    name = path.name.lower()
    if name == "mock_data.csv":
        return (0, name)
    match = re.search(r"\((\d+)\)", name)
    return (int(match.group(1)) if match else 999, name)


def iter_records(data_dir: Path) -> Iterable[dict[str, Any]]:
    files = sorted(
        (p for p in data_dir.iterdir() if p.suffix.lower() == ".csv"),
        key=_natural_csv_sort
    )
    for csv_path in files:
        if "mock_data" not in csv_path.name.lower():
            continue
        print(f"Reading {csv_path}")
        with csv_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                yield {field: _coerce(field, row.get(field, "")) for field in FIELD_ORDER}


def ensure_topic(bootstrap: str, topic: str) -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
        print(f"Topic '{topic}' created.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic}' already exists.")
    finally:
        admin.close()


def connect(bootstrap: str, topic: str, retries: int = 90, delay: float = 2.0) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda payload: json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                acks=1,
                linger_ms=50,
                batch_size=65536,
                retries=3,
            )
            ensure_topic(bootstrap, topic)
            print("Connected to Kafka.")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka not ready ({attempt}/{retries}), retry in {delay}s...")
        except Exception as exc:
            print(f"Kafka init error ({attempt}/{retries}): {exc}")
        time.sleep(delay)
    raise SystemExit(f"Kafka недоступен после {retries} попыток ({bootstrap}).")


def main() -> None:
    cfg = load_settings()
    producer = connect(cfg.bootstrap, cfg.topic)

    sent = 0
    try:
        for record in iter_records(cfg.data_dir):
            producer.send(cfg.topic, value=record)
            sent += 1
            if cfg.delay_sec > 0:
                time.sleep(cfg.delay_sec)
        producer.flush()
    finally:
        producer.close()

    print(f"All rows sent to Kafka. Total: {sent}")


if __name__ == "__main__":
    main()