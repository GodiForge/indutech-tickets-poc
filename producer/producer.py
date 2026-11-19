import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer

TYPES_DEMANDE = [
    "Incident technique",
    "Maintenance capteur IoT",
    "Facturation & Contrat",
    "Accès & Identifiants",
    "Demande d'information",
]

PRIORITES = ["Basse", "Moyenne", "Haute", "Critique"]

DEMANDES_EXEMPLES = [
    "Perte de connexion sur la passerelle IoT de l'atelier B",
    "Erreur d'export des relevés de température dans l'ERP",
    "Demande de réinitialisation d'accès utilisateur",
    "Anomalie sur le montant de la dernière facture mensuelle",
    "Latence élevée constatée lors de la remontée des logs capteurs",
    "Demande d'ajout de 5 nouveaux capteurs sur la ligne de production 2",
]


def generer_ticket() -> dict:
    return {
        "ticket_id": f"TKT-{uuid.uuid4().hex[:8].upper()}",
        "client_id": f"CLI-{random.randint(1000, 1050)}",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "request": random.choice(DEMANDES_EXEMPLES),
        "request_type": random.choice(TYPES_DEMANDE),
        "priority": random.choices(PRIORITES, weights=[25, 40, 25, 10], k=1)[0],
    }


def main():
    parser = argparse.ArgumentParser(description="Producteur de tickets clients vers Redpanda")
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Adresse du broker Redpanda/Kafka",
    )
    parser.add_argument("--count", type=int, default=15, help="Nombre de tickets à envoyer (0 = infini)")
    parser.add_argument("--interval", type=float, default=0.5, help="Pause en secondes entre chaque ticket")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    print(f"Producteur connecté à Redpanda ({args.bootstrap_servers}) -> topic 'client_tickets'")

    compteur = 0
    try:
        while args.count == 0 or compteur < args.count:
            ticket = generer_ticket()
            producer.send(
                topic="client_tickets",
                key=ticket["ticket_id"],
                value=ticket,
            )
            producer.flush()
            compteur += 1
            print(f"[{compteur}] Ticket envoyé : {json.dumps(ticket, ensure_ascii=False)}")
            if args.count == 0 or compteur < args.count:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nArrêt manuel du producteur.")
    finally:
        producer.close()
        print(f"Total envoyé : {compteur} tickets.")


if __name__ == "__main__":
    main()