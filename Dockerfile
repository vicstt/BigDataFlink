FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y default-jre && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV JAVA_HOME=/usr/lib/jvm/default-java

COPY kafka_producer.py .
COPY flink_job.py .

CMD ["bash"]