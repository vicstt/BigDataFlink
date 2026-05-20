#!/bin/bash

echo "[1/4] Сборка и запуск сервисов..."
docker-compose build python-runner
docker-compose up -d
sleep 20

echo "[2/4] Отправка данных в Kafka..."
docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic sales_topic 2>/dev/null || true
sleep 2
docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --create --topic sales_topic --partitions 1 --replication-factor 1 2>/dev/null
docker-compose exec -T python-runner python kafka_producer.py

echo "[3/4] Запуск Flink Streaming Job..."
docker-compose exec -T python-runner python flink_job.py

echo ""
echo "  Результаты:"
docker-compose exec -T postgres psql -U flinkuser -d flinkdb -c \
    "SELECT 'fact_sales' as Таблица, count(*) FROM fact_sales
     UNION ALL SELECT 'dim_customer', count(*) FROM dim_customer
     UNION ALL SELECT 'dim_product', count(*) FROM dim_product
     UNION ALL SELECT 'dim_seller', count(*) FROM dim_seller
     UNION ALL SELECT 'dim_store', count(*) FROM dim_store
     UNION ALL SELECT 'dim_supplier', count(*) FROM dim_supplier;"

echo ""
echo "  PostgreSQL: localhost:5432 (flinkdb)"
