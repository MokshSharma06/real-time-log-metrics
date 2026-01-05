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
│   ├── docker-compose.yml        # Kafka + Zookeeper
│   ├── create_topics.sh
│   └── producer/
│       └── log_producer.py       # JSON log generator → Kafka
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



