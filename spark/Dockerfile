# spark/Dockerfile
FROM apache/spark:3.5.3

LABEL maintainer="Data Engineer - InduTech POC"
LABEL description="Application PySpark Structured Streaming pour l'analyse des tickets clients"

USER root
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pré-téléchargement et mise en cache des JARs Kafka dans l'image Docker
RUN /opt/spark/bin/spark-submit \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    --version

COPY consumer_spark.py .

RUN mkdir -p /app/output

CMD ["/opt/spark/bin/spark-submit", "--conf", "spark.jars.ivy=/tmp/.ivy2", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3", "/app/consumer_spark.py", "--bootstrap-servers", "redpanda:29092", "--output-dir", "/app/output", "--duration", "0"]