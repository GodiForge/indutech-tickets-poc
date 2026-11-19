# redpanda/Dockerfile
FROM docker.redpanda.com/redpandadata/redpanda:latest

LABEL maintainer="Data Engineer - InduTech POC"
LABEL description="Broker Redpanda temps réel pour l'ingestion des tickets clients"

# Ports : 9092 (Kafka externe), 29092 (Kafka interne Docker), 9644 (Admin API), 18081 (Schema Registry), 18082 (Pandaproxy)
EXPOSE 9092 29092 9644 18081 18082

HEALTHCHECK --interval=10s --timeout=5s --retries=10 \
  CMD rpk cluster health | grep -E "Healthy:\s*true" || exit 1

CMD ["redpanda", "start", "--smp", "1", "--memory", "1G", "--reserve-memory", "0M", "--overprovisioned", "--node-id", "0", "--kafka-addr", "internal://0.0.0.0:29092,external://0.0.0.0:9092", "--advertise-kafka-addr", "internal://redpanda:29092,external://localhost:9092", "--pandaproxy-addr", "internal://0.0.0.0:28082,external://0.0.0.0:18082", "--advertise-pandaproxy-addr", "internal://redpanda:28082,external://localhost:18082", "--schema-registry-addr", "internal://0.0.0.0:28081,external://0.0.0.0:18081", "--rpc-addr", "redpanda:33145", "--advertise-rpc-addr", "redpanda:33145"]