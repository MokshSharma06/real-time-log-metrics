# Real-Time Log metrics Pipeline on Azure (End-to-End Streaming Data Engineering Project)
```
This project implements a real-time log analytics system using Apache Kafka and Spark Structured Streaming, designed to ingest, process, and analyze application logs at scale.
The pipeline follows a Bronze–Silver–Gold layered architecture to ensure:

Data reliability

Fault tolerance

Late data handling , Duplicate Data handling

Analytics-ready outputs for dashboards
```
# Problem Statement (system generates tones of logs)
How do we reliably ingest, clean, aggregate, and visualize logs in near real time while handling late and faulty data?
```
Arrive out of order
Can be delayed or duplicated
Are noisy and unstructured
Require near real-time visibility
Traditional batch pipelines introduce high latency and are unsuitable for:
Real-time monitoring
Error detection and Operational dashboards
```
# Solution ( this pipeline)
Kafka / Event HUbs used for distributed log ingestion
Databricks : Spark Structured Streaming for real-time processing
Storage : Delta Lake for reliable, replayable storage ADLS GEN 2
Architecture : Bronze–Silver–Gold architecture for separation of concerns
# The pipeline is designed to handle:
```
✅ High-throughput streaming data
✅ Late-arriving events
✅ Bad data quarantine
✅ Scalable distributed processing
✅ Medallion architecture (Bronze → Silver → Gold)
✅ Near real-time analytics
```
## 📘 Detailed Documentation

For architecture decisions, design thinking, and deep technical explanations,  
refer to the full project documentation:

👉[![Docs](https://img.shields.io/badge/Documentation-Notion-black)](https://equable-need-9b3.notion.site/Daily-Progress-of-real-time-log-metrics-and-learnings-2e03d4bcadaa80e8a8a3c65c11b5ff8b?source=copy_link)

```
real-time-log-streaming/
│
├── README.md
├── requirements.txt
│
├── config/
│   ├── app_config.yaml           # topic names, paths, window size
│   └── spark.conf                # Spark + Delta configs
│
├── kafka/
│   
│   
│   └── producer/
│       └── log_producer.py       # JSON log generator → Kafka/EventHubs
│
├── src/
│   ├── bronze/
│   │   └── kafka_to_bronze.py    # Kafka → Bronze Delta (raw)
│   │
│   ├── silver/
│   │   └── bronze_to_silver.py   # event-time, watermark, dedup
│   │
│   ├── gold/
│   │   └── silver_to_gold.py     # windowed aggregations
│   │
│   ├── utils/
│   │   ├── spark_session.py      # Spark session + Delta setup
│   │   └── schema.py             # log schema definition
│
│   └── __init__.py
│
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── checkpoints/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── cloud/
│   └── adls_setup.md             # ADLS auth + configs
│
├── scripts/
│   ├── start_kafka.sh
│   └── submit_streams.sh         # spark-submit commands
│
└── run_pipeline.py               # optional orchestration entry
```

```
JSON Log Producer 
        ↓
     Apache Kafka / Event Hubs
        ↓
Spark Structured Streaming
        ↓
  Bronze Delta (Raw)
        ↓
  Silver Delta (Clean & Trusted)
        ↓
   Gold Delta (Aggregated Metrics)
```


# ⚙️ Streaming Features Implemented

This project intentionally focuses on core, real-world streaming problems:

#1. Event-Time Processing
Metrics are computed using the event’s timestamp, not processing time.

#2.Late-Arriving Data Handling
Watermarks allow late events to update windows within a defined tolerance.

#3. Windowed Aggregations
Metrics are computed over fixed time windows (e.g., 1 minute).

#4. Deduplication
Duplicate events (due to retries or at-least-once delivery) are removed using event_id.

#5. Fault Tolerance & Exactly-Once Semantics
Checkpointing + Delta Lake ensure safe restarts without data corruption.

