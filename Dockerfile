FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y default-jre wget && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /opt/flink/lib && \
    cd /opt/flink/lib && \
    wget -q "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar" || \
    wget -q "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar" && \
    wget -q "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar" || \
    wget -q "https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar" && \
    wget -q "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar" || \
    wget -q "https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar" && \
    ls -la

ENV JAVA_HOME=/usr/lib/jvm/default-java

COPY kafka_producer.py .
COPY flink_job.py .

CMD ["bash"]