# producer/Dockerfile
FROM python:3.12-slim

LABEL maintainer="Data Engineer - InduTech POC"
LABEL description="Producteur Python générant des tickets clients en temps réel vers Redpanda"

# Empêche Python de mettre en mémoire tampon les logs stdout (affichage temps réel dans docker logs)
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Par défaut : envoi continu (count=0) toutes les 1.5 secondes vers redpanda:29092
CMD ["python", "producer.py", "--bootstrap-servers", "redpanda:29092", "--count", "0", "--interval", "1.5"]