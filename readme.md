# Real-Time Log metrics Pipeline on Azure (End-to-End Streaming Data Engineering Project)
This project implements a production-style real-time streaming data pipeline that ingests application logs, processes them with distributed computing, and delivers actionable insights through analytics dashboards.
The pipeline is designed to handle:

✅ High-throughput streaming data
✅ Late-arriving events
✅ Bad data quarantine
✅ Scalable distributed processing
✅ Medallion architecture (Bronze → Silver → Gold)
✅ Near real-time analytics
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

