from __future__ import annotations

import os
import glob
import time
from dataclasses import dataclass
from urllib.request import urlretrieve

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

JAR_DIR = '/opt/flink/lib'
os.makedirs(JAR_DIR, exist_ok=True)

REQUIRED_JARS = {
    'flink-sql-connector-kafka-3.0.2-1.18.jar':
        'https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar',
    'flink-connector-jdbc-3.1.2-1.18.jar':
        'https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar',
    'postgresql-42.7.1.jar':
        'https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar',
    'kafka-clients-3.4.0.jar':
        'https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar',
}

for jar_name, jar_url in REQUIRED_JARS.items():
    jar_path = os.path.join(JAR_DIR, jar_name)
    if not os.path.exists(jar_path):
        print(f'Downloading {jar_name}...')
        urlretrieve(jar_url, jar_path)

jars = glob.glob(f'{JAR_DIR}/*.jar')
print(f' {len(jars)} JAR files ready\n')


@dataclass(frozen=True)
class AppConfig:
    kafka_bootstrap: str
    kafka_topic: str
    kafka_group: str
    pg_host: str
    pg_port: str
    pg_db: str
    pg_user: str
    pg_password: str


def env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value and value.strip() else default


def load_config() -> AppConfig:
    return AppConfig(
        kafka_bootstrap=env("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        kafka_topic=env("KAFKA_TOPIC", "sales_topic"),
        kafka_group=env("KAFKA_GROUP_ID", "flink-final-" + str(int(time.time()))),
        pg_host=env("POSTGRES_HOST", "postgres"),
        pg_port=env("POSTGRES_PORT", "5432"),
        pg_db=env("POSTGRES_DB", "flinkdb"),
        pg_user=env("POSTGRES_USER", "flinkuser"),
        pg_password=env("POSTGRES_PASSWORD", "flinkpassword"),
    )


def main() -> None:
    cfg = load_config()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_buffer_timeout(1000)
    
    jar_urls = [f'file://{jar}' for jar in jars]
    env.add_jars(*jar_urls)
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env, environment_settings=settings)

    jdbc = f"jdbc:postgresql://{cfg.pg_host}:{cfg.pg_port}/{cfg.pg_db}"
    creds = f"'username' = '{cfg.pg_user}', 'password' = '{cfg.pg_password}'"

    print("Creating tables...")
    t_env.execute_sql(f"""
    CREATE TABLE sales_events (
        id INT, customer_first_name STRING, customer_last_name STRING,
        customer_age INT, customer_email STRING, customer_country STRING,
        customer_postal_code STRING, customer_pet_type STRING,
        customer_pet_name STRING, customer_pet_breed STRING,
        seller_first_name STRING, seller_last_name STRING, seller_email STRING,
        seller_country STRING, seller_postal_code STRING,
        product_name STRING, product_category STRING,
        product_price DECIMAL(10,2), product_quantity INT,
        sale_date STRING, sale_customer_id INT, sale_seller_id INT,
        sale_product_id INT, sale_quantity INT, sale_total_price DECIMAL(12,2),
        store_name STRING, store_location STRING, store_city STRING,
        store_state STRING, store_country STRING, store_phone STRING, store_email STRING,
        pet_category STRING, product_weight DECIMAL(10,2), product_color STRING,
        product_size STRING, product_brand STRING, product_material STRING,
        product_description STRING, product_rating DECIMAL(3,1), product_reviews INT,
        product_release_date STRING, product_expiry_date STRING,
        supplier_name STRING, supplier_contact STRING, supplier_email STRING,
        supplier_phone STRING, supplier_address STRING, supplier_city STRING, supplier_country STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{cfg.kafka_topic}',
        'properties.bootstrap.servers' = '{cfg.kafka_bootstrap}',
        'properties.group.id' = '{cfg.kafka_group}',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
    """)

    t_env.execute_sql(f"""CREATE TABLE dim_customer (
        customer_id INT, customer_first_name STRING, customer_last_name STRING,
        customer_email STRING, customer_age INT, customer_country STRING,
        customer_postal_code STRING, customer_pet_type STRING,
        customer_pet_name STRING, customer_pet_breed STRING,
        PRIMARY KEY (customer_id) NOT ENFORCED
    ) WITH ('connector' = 'jdbc', 'url' = '{jdbc}', 'table-name' = 'dim_customer',
        'sink.buffer-flush.max-rows' = '100', 'sink.buffer-flush.interval' = '1s', {creds})""")

    t_env.execute_sql(f"""CREATE TABLE dim_seller (
        seller_id INT, seller_first_name STRING, seller_last_name STRING,
        seller_email STRING, seller_country STRING, seller_postal_code STRING,
        PRIMARY KEY (seller_id) NOT ENFORCED
    ) WITH ('connector' = 'jdbc', 'url' = '{jdbc}', 'table-name' = 'dim_seller',
        'sink.buffer-flush.max-rows' = '100', 'sink.buffer-flush.interval' = '1s', {creds})""")

    t_env.execute_sql(f"""CREATE TABLE dim_product (
        product_id INT, product_name STRING, product_category STRING,
        product_brand STRING, product_price DECIMAL(10,2),
        product_weight DECIMAL(10,2), product_color STRING, product_size STRING,
        product_material STRING, product_rating DECIMAL(3,1), product_reviews INT,
        pet_category STRING,
        PRIMARY KEY (product_id) NOT ENFORCED
    ) WITH ('connector' = 'jdbc', 'url' = '{jdbc}', 'table-name' = 'dim_product',
        'sink.buffer-flush.max-rows' = '100', 'sink.buffer-flush.interval' = '1s', {creds})""")

    t_env.execute_sql(f"""CREATE TABLE dim_store (
        store_name STRING, store_location STRING, store_city STRING,
        store_state STRING, store_country STRING, store_phone STRING, store_email STRING,
        PRIMARY KEY (store_name) NOT ENFORCED
    ) WITH ('connector' = 'jdbc', 'url' = '{jdbc}', 'table-name' = 'dim_store',
        'sink.buffer-flush.max-rows' = '100', 'sink.buffer-flush.interval' = '1s', {creds})""")

    t_env.execute_sql(f"""CREATE TABLE dim_supplier (
        supplier_name STRING, supplier_contact STRING, supplier_email STRING,
        supplier_phone STRING, supplier_address STRING, supplier_city STRING, supplier_country STRING,
        PRIMARY KEY (supplier_name) NOT ENFORCED
    ) WITH ('connector' = 'jdbc', 'url' = '{jdbc}', 'table-name' = 'dim_supplier',
        'sink.buffer-flush.max-rows' = '100', 'sink.buffer-flush.interval' = '1s', {creds})""")

    t_env.execute_sql(f"""CREATE TABLE fact_sales (
        sale_date DATE, customer_id INT, product_id INT, seller_id INT,
        quantity INT, total_price DECIMAL(12,2)
    ) WITH ('connector' = 'jdbc', 'url' = '{jdbc}', 'table-name' = 'fact_sales',
        'sink.buffer-flush.max-rows' = '100', 'sink.buffer-flush.interval' = '1s', {creds})""")

    print("Submitting INSERT jobs...")
    table_result = t_env.execute_sql("INSERT INTO dim_customer SELECT sale_customer_id, customer_first_name, customer_last_name, customer_email, customer_age, customer_country, customer_postal_code, customer_pet_type, customer_pet_name, customer_pet_breed FROM sales_events")
    t_env.execute_sql("INSERT INTO dim_seller SELECT sale_seller_id, seller_first_name, seller_last_name, seller_email, seller_country, seller_postal_code FROM sales_events")
    t_env.execute_sql("INSERT INTO dim_product SELECT sale_product_id, product_name, product_category, product_brand, product_price, product_weight, product_color, product_size, product_material, product_rating, product_reviews, pet_category FROM sales_events")
    t_env.execute_sql("INSERT INTO dim_store SELECT store_name, store_location, store_city, store_state, store_country, store_phone, store_email FROM sales_events")
    t_env.execute_sql("INSERT INTO dim_supplier SELECT supplier_name, supplier_contact, supplier_email, supplier_phone, supplier_address, supplier_city, supplier_country FROM sales_events")
    t_env.execute_sql("INSERT INTO fact_sales SELECT TO_DATE(sale_date, 'M/d/yyyy'), sale_customer_id, sale_product_id, sale_seller_id, sale_quantity, sale_total_price FROM sales_events")
    
    print(" All jobs submitted! Waiting 30s for processing...")
    time.sleep(30)
    print(" Done!")


if __name__ == "__main__":
    main()