import json
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import (
    col,
    from_json,
    when,
    current_timestamp,
    count,
    avg,
    approx_count_distinct,
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "client_tickets")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/app/output")
PARQUET_PATH = os.path.join(OUTPUT_DIR, "enriched_tickets_parquet")
SUMMARY_JSON_FILE = os.path.join(OUTPUT_DIR, "analytics_summary.json")
CHECKPOINT_PARQUET = os.path.join(OUTPUT_DIR, "_checkpoints", "parquet")
CHECKPOINT_JSON = os.path.join(OUTPUT_DIR, "_checkpoints", "json")
RUN_DURATION = int(os.getenv("SPARK_RUN_DURATION", "25"))


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("InduTech-Tickets-RealTime-ETL")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_analytics_json(batch_df, batch_id: int):
    if not batch_df.isEmpty():
        rows = [row.asDict(recursive=True) for row in batch_df.collect()]
        tmp_file = SUMMARY_JSON_FILE + ".tmp"
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
        os.replace(tmp_file, SUMMARY_JSON_FILE)
        print(f"[Batch {batch_id}] Synthèse analytique exportée vers {SUMMARY_JSON_FILE}")


def main():
    spark = None
    try:
        spark = create_spark_session()
        print("=================================================================")
        print(f"Connexion PySpark -> Redpanda ({KAFKA_BOOTSTRAP}) | Topic : {TOPIC_NAME}")
        print(f"Export Parquet   -> {PARQUET_PATH}")
        print(f"Export JSON      -> {SUMMARY_JSON_FILE}")
        print("=================================================================\n")

        ticket_schema = StructType([
            StructField("ticket_id", StringType(), True),
            StructField("client_id", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("request", StringType(), True),
            StructField("request_type", StringType(), True),
            StructField("priority", StringType(), True),
        ])

        raw_df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", TOPIC_NAME)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )

        parsed_df = (
            raw_df
            .selectExpr(
                "CAST(value AS STRING) AS json_payload",
                "partition AS kafka_partition",
                "offset AS kafka_offset",
            )
            .withColumn("data", from_json(col("json_payload"), ticket_schema))
            .filter(col("data").isNotNull() & col("data.ticket_id").isNotNull())
            .select(
                col("data.ticket_id").alias("ticket_id"),
                col("data.client_id").alias("client_id"),
                col("data.created_at").alias("created_at"),
                col("data.request").alias("request"),
                col("data.request_type").alias("request_type"),
                col("data.priority").alias("priority"),
                col("kafka_partition"),
                col("kafka_offset"),
            )
        )

        enriched_df = (
            parsed_df
            .withColumn(
                "assigned_team",
                when(col("request_type") == "Incident technique", "Support Niveau 2 - Infrastructure")
                .when(col("request_type") == "Maintenance capteur IoT", "Équipe IoT & Terrain")
                .when(col("request_type") == "Facturation & Contrat", "Service Administration des Ventes")
                .when(col("request_type") == "Accès & Identifiants", "Équipe IAM & Sécurité AD")
                .when(col("request_type") == "Demande d'information", "Support Niveau 1 - Accueil")
                .otherwise("Support Général"),
            )
            .withColumn(
                "sla_target_hours",
                when(col("priority") == "Critique", 2)
                .when(col("priority") == "Haute", 4)
                .when(col("priority") == "Moyenne", 24)
                .otherwise(48),
            )
            .withColumn("processed_at", current_timestamp())
        )

        analytics_df = (
            enriched_df
            .groupBy("assigned_team", "request_type", "priority")
            .agg(
                count("*").alias("total_tickets"),
                approx_count_distinct("client_id").alias("unique_clients"),
                avg("sla_target_hours").alias("avg_sla_hours"),
            )
        )

        query_parquet = (
            enriched_df.writeStream
            .format("parquet")
            .outputMode("append")
            .option("path", PARQUET_PATH)
            .option("checkpointLocation", CHECKPOINT_PARQUET)
            .trigger(processingTime="5 seconds")
            .start()
        )

        query_json = (
            analytics_df.writeStream
            .outputMode("complete")
            .option("checkpointLocation", CHECKPOINT_JSON)
            .foreachBatch(write_analytics_json)
            .trigger(processingTime="5 seconds")
            .start()
        )

        if RUN_DURATION == 0:
            print("Pipeline streaming actif en mode continu...")
            while True:
                for q in (query_parquet, query_json):
                    if q.exception():
                        raise q.exception()
                time.sleep(2)
        else:
            print(f"Pipeline streaming actif pendant {RUN_DURATION} secondes...")
            start_time = time.time()
            while time.time() - start_time < RUN_DURATION:
                for q in (query_parquet, query_json):
                    if q.exception():
                        raise q.exception()
                time.sleep(2)

        query_parquet.stop()
        query_json.stop()
        print("\nTraitement terminé avec succès.")

    except KeyboardInterrupt:
        print("\nArrêt manuel du pipeline PySpark.")
    except Exception as e:
        print(f"\nERREUR CRITIQUE dans le pipeline PySpark : {e}")
        raise
    finally:
        if spark is not None:
            spark.stop()
            print("Session Spark fermée proprement.")


if __name__ == "__main__":
    main()