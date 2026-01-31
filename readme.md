# Real-Time Log metrics Pipeline on Azure (End-to-End Streaming Data Engineering Project)
This project demonstrates a production-style, environment-agnostic streaming data pipeline built using Apache Kafka, Spark Structured Streaming, and Delta Lake. The pipeline is designed to run consistently across different environments by externalizing all configuration and avoiding platform-specific logic. It ensures idempotent and exactly-once processing through checkpointing and transactional storage, allowing safe restarts without data duplication. Using a Bronze–Silver–Gold architecture, the system cleanly separates ingestion, data quality, and analytics layers, enabling reliable handling of late-arriving events, duplicate data, and real-time metric computation.

# Problem Statement (system generates tones of logs)
How do we reliably ingest, clean, aggregate, and visualize logs in near real time while handling late and faulty data?
```
• Arrive out of order
• Can be delayed or duplicated
• Are noisy and unstructured
• Require near real-time visibility
• Traditional batch pipelines introduce high latency and are unsuitable for:
• Real-time monitoring
• Error detection and Operational dashboards
```

## 💡 Solution (This Pipeline)
- **Ingestion**: Kafka / Event Hubs for distributed log ingestion  
- **Processing**: Spark Structured Streaming on Azure Databricks  
- **Storage**: Delta Lake on ADLS Gen2 for reliable, replayable storage  
- **Architecture**: Bronze–Silver–Gold for clear separation of concerns  

## 📘 Detailed Documentation

For architecture decisions, design thinking, and deep technical explanations,  
refer to the full project documentation:

👉[![Docs](https://img.shields.io/badge/Documentation-Notion-black)](https://equable-need-9b3.notion.site/Daily-Progress-of-real-time-log-metrics-and-learnings-2e03d4bcadaa80e8a8a3c65c11b5ff8b?source=copy_link)

```
The pipeline follows a Bronze–Silver–Gold layered architecture to ensure:

• Data reliability

• Fault tolerance

• Late data handling , Duplicate Data handling

• Analytics-ready outputs for dashboards
```


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
└── main.py               # orchestration entry
```

# High Level Architecture
```
Log Producer (Python)
        ↓
Kafka (Multi-Partition)
        ↓
Spark Structured Streaming (Databricks)
        ↓
Bronze Delta Table (Raw Logs)
        ↓
Silver Delta Table (Clean & Validated)
        ↓
Gold Delta Table (Aggregated Metrics)
        ↓
Dashboard / BI Tool

```
## 🤎 Bronze Layer ( — Raw Ingestion)

### Purpose:
**Capture logs exactly as they arrive.**

### Characteristics:
- Append-only
- Minimal transformations
- Stores raw Kafka messages
- Supports replay and backfills

## 🜛 Silver Layer (clean and trusted data)
### Purpose:
**Ensure Data quality and correctness**

### Characteristics:
- schema enforcement
- invalid record filtering
- late data handling using *event-time watermarking*
- segreggating data to different sinks based on type (clean, bad , late)

## 🟡 Gold layer ( Business Metrics)
### Purpose:
**Provides analytics ready data**

### Characteristics:
- window based aggregations (5 mins)
- Error counts and trends
- uses event time tumbling window



# ⚙️ Streaming and Engineering Concepts Implemented

This project intentionally focuses on core, real-world streaming problems:

#### 1. Event-Time Processing and micro batching
Metrics are computed using the event’s timestamp, not processing time in micro batches.

#### 2.Late-Arriving Data Handling
Watermarks allow late events to update windows within a defined tolerance.

#### 3. Windowed Aggregations
Metrics are computed over fixed time windows (e.g., 5 minute).

#### 4. Deduplication
Duplicate events (due to retries or at-least-once delivery) are removed using event_id.

#### 5. Fault Tolerance & Exactly-Once Semantics
Checkpointing + Delta Lake ensure safe restarts without data corruption.

