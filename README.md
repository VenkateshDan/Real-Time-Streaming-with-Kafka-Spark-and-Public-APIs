# Real-Time-Streaming-with-Kafka-Spark-and-Public-APIs
Retrieve Data from public API's and write into kafka producer. Spark reads from kafka consumer and filter the data
📌 Project Overview
This project demonstrates a real-time data pipeline using:
    Public APIs as data sources (JSON/XML formats), one with (OAuth2)
    Kafka as the messaging layer
    Spark Structured Streaming for processing
    Airflow Integration
    Write to PostgreSQL, Console, or Azure ADLS

🏗️ Architecture Diagram    

⚙️ Tech Stack

    Python: API integration + Kafka producers
    Apache Kafka: Real-time event streaming
    Apache Spark: Stream processing
    Docker Compose: For Kafka setup (Zookeeper, Kafka Broker, UI)
    Airflow Integration : Automates API polling to Kafka, Spark job submission,Health checks for output sinks
    PostgreSQL : For storing transformed data
    Azure ADLS/Blob (future): Cloud storage for curated datasets

    🌐 Public APIs Used
      Open-Meteo	Weather (forecast)	Time-series JSON	https://api.open-meteo.com/v1/forecast?latitude=35&longitude=139&hourly=temperature_2m
      CoinGecko	Cryptocurrency Prices	Flat JSON (Key-Value)	https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd
      DummyJSON	E-commerce data	Nested JSON	https://dummyjson.com/products
      Spotify Integration
            Uses Spotify Web API (OAuth2)
            Fetches new releases, top tracks, or artist metadata
            Sends payloads to spotify_data Kafka topic
            Parsed and processed using Spark

⏳ Airflow Integration
    Automates:
        API polling to Kafka
        Spark job submission
        Health checks for output sinks

    DAGs include:
        Weather to Kafka
        Crypto to Kafka
        Spark job trigger
        
  Optional: PostgreSQL ingestion and ADLS sync
