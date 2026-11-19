
# POC — Pipeline Temps Réel de Gestion des Tickets Clients (InduTech)

Ce dépôt contient le Proof of Concept (POC) d'un pipeline Data Engineering temps réel conçu pour **InduTech**. Il permet d'ingérer, d'enrichir et d'analyser en continu les tickets clients à l'aide de **Redpanda**, **Python**, **PySpark Structured Streaming** et **Docker Compose**.

---

## Architecture du Pipeline ETL (Diagramme Mermaid)

```mermaid
flowchart LR
    subgraph Ingestion ["1. Génération & Ingestion Temps Réel"]
        P["🐍 Producteur Python<br/>(producer/producer.py)"]
        RP[("⚡ Cluster Redpanda<br/>Topic : client_tickets<br/>(3 partitions)")]
        UI["🖥️ Redpanda Console<br/>http://localhost:8080"]
        P -->|"JSON UTF-8<br/>(API Kafka :29092)"| RP
        RP <-->|"Monitoring &<br/>Inspection"| UI
    end

    subgraph Processing ["2. Traitement & Analyse Streaming"]
        SP["🔥 PySpark Structured Streaming<br/>(spark/consumer_spark.py)"]
        T1["⚙️ Enrichissement Métier<br/>• Assignation équipe support<br/>• Calcul délai SLA cible"]
        T2["📊 Agrégations Temps Réel<br/>• Total tickets / équipe<br/>• Clients distincts & SLA moyen"]
        RP -->|"readStream<br/>(micro-batch 5s)"| SP
        SP --> T1
        T1 --> T2
    end

    subgraph Storage ["3. Stockage & Restitution Analytique"]
        PQ[("📦 Export Parquet<br/>output/enriched_tickets_parquet/")]
        JS[("📄 Export JSON<br/>output/analytics_summary_json/")]
        CP[("🛡️ Checkpoints Spark<br/>output/_checkpoints/")]
        T1 -->|"Mode Append"| PQ
        T2 -->|"Mode Complete"| JS
        SP -.->|"Reprise sur panne"| CP
    end
```

---

## Démonstration Vidéo

* **Vidéo de présentation et démonstration du pipeline ETL :** [Voir la vidéo de démonstration (Loom / YouTube)](https://www.loom.com/share/REMPLACER_PAR_VOTRE_LIEN_VIDEO)

*(La vidéo présente l'architecture conteneurisée, le démarrage complet via `docker compose up --build -d`, l'inspection en direct des messages dans Redpanda Console et la génération automatique des rapports Parquet et JSON par PySpark.)*

---

## Structure du Projet

```text
indutech-tickets-poc/
├── docker-compose.yml              # Orchestration complète (Redpanda, Console, Producer, Spark)
├── README.md                       # Documentation du projet et diagramme Mermaid
├── redpanda/
│   └── Dockerfile                  # Image Docker du broker Redpanda
├── producer/
│   ├── Dockerfile                  # Image Docker du générateur de tickets
│   ├── producer.py                 # Script Python générant les tickets clients aléatoires
│   └── requirements.txt            # Dépendance : kafka-python-ng
├── spark/
│   ├── Dockerfile                  # Image Docker Apache Spark 3.5.3 + connecteur Kafka
│   ├── consumer_spark.py           # Pipeline PySpark Structured Streaming
│   └── requirements.txt            # Dépendance : pyspark==3.5.3
└── output/                         # Répertoire monté en volume contenant les exports Parquet & JSON
```

---

## Prérequis & Lancement Rapide (Docker Compose)

### Prérequis

* **Docker Desktop** (avec moteur Linux / WSL2 actif) et **Docker Compose v2+**.

### 1. Démarrer l'ensemble du pipeline en une commande

Depuis la racine du dossier `indutech-tickets-poc` :

```bash
docker compose up --build -d
```

Cette commande :

1. Construit les 3 images Docker (`redpanda`, `ticket-producer`, `spark-processor`).
2. Démarre le broker **Redpanda** et attend qu'il soit opérationnel (`healthcheck`).
3. Crée automatiquement le topic **`client_tickets`** (3 partitions) via le conteneur `redpanda-init`.
4. Lance **Redpanda Console** sur [http://localhost:8080](http://localhost:8080).
5. Démarre l'envoi continu de tickets par `indutech-producer` et leur analyse temps réel par `indutech-spark`.

### 2. Vérifier le fonctionnement en direct

* **Suivre les tickets envoyés par le producteur :**
  ```bash
  docker logs -f indutech-producer
  ```
* **Suivre les micro-batchs traités par PySpark :**
  ```bash
  docker logs -f indutech-spark
  ```
* **Inspecter les topics et messages dans l'interface web :**
  Ouvrez [http://localhost:8080](http://localhost:8080) > **Topics** > **`client_tickets`**.
* **Consulter les résultats exportés :**
  * Données détaillées enrichies : `output/enriched_tickets_parquet/*.snappy.parquet`
  * Synthèse analytique agrégée : `output/analytics_summary_json/*.json`

### 3. Arrêter la stack proprement

```bash
docker compose down
```

*(Ajoutez `-v` uniquement si vous souhaitez supprimer également le volume de données Redpanda.)*

---

## Modèle de Données & Règles Métier

### Format des messages entrants (`client_tickets`)

Chaque ticket généré en JSON contient les 6 attributs suivants :

* `ticket_id` : identifiant unique du ticket (ex. `TKT-70103BB1`)
* `client_id` : identifiant du client (ex. `CLI-1014`)
* `created_at` : horodatage de création (`YYYY-MM-DD HH:MM:SS`)
* `request` : description textuelle de la demande
* `request_type` : catégorie de la demande
* `priority` : niveau d'urgence (`Basse`, `Moyenne`, `Haute`, `Critique`)

### Enrichissements et agrégations PySpark

| Type de demande (`request_type`) | Équipe assignée automatiquement (`assigned_team`) |
| :--------------------------------- | :---------------------------------------------------- |
| `Incident technique`             | Support Niveau 2 - Infrastructure                     |
| `Maintenance capteur IoT`        | Équipe IoT & Terrain                                 |
| `Facturation & Contrat`          | Service Administration des Ventes                     |
| `Accès & Identifiants`          | Équipe IAM & Sécurité AD                           |
| `Demande d'information`          | Support Niveau 1 - Accueil                            |

* **SLA cible (`sla_target_hours`) :** `2h` (Critique), `4h` (Haute), `24h` (Moyenne), `48h` (Basse).
* **Performance & Résilience Spark :** partitions de shuffle ajustées (`spark.sql.shuffle.partitions=4`), arrêt gracieux (`stopGracefullyOnShutdown=true`), filtrage des messages JSON invalides et gestion des *checkpoints* (`output/_checkpoints/`) permettant une reprise sans perte ni doublon en cas de redémarrage.
