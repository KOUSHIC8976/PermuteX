<div align="center">
  
  #  PermuteX
  **Real-Time Streaming Architecture**
  
  [![Python](https://img.shields.io/badge/Python-3.10-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
  [![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
  [![Apache Kafka](https://img.shields.io/badge/Kafka-KRaft_Mode-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
  [![Apache Flink](https://img.shields.io/badge/Flink-Stateful_Streaming-E6522C?style=for-the-badge&logo=apacheflink&logoColor=white)](https://flink.apache.org/)
  [![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Gold_Layer-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
  [![Apache Airflow](https://img.shields.io/badge/Airflow-Orchestration-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
  [![Grafana](https://img.shields.io/badge/Grafana-Observability-F46800?style=for-the-badge&logo=grafana&logoColor=white)](https://grafana.com/)
  [![Terraform](https://img.shields.io/badge/Terraform-IaC-844FBA?style=for-the-badge&logo=terraform&logoColor=white)](https://www.terraform.io/)

  <p align="center">
    An end-to-end, stateful streaming data architecture designed to ingest, validate, process, and visualize high-velocity IoT telemetry data in real-time.
  </p>

</div>

---

##  Table of Contents
- [Overview](#-overview)
- [Architecture & Tech Stack](#-architecture--tech-stack)
- [Key Technical Challenges Overcome](#-key-technical-challenges-overcome)
- [Quick Start](#-quick-start)

---

##  Overview

Built entirely on local infrastructure using Docker, this project simulates a fleet of aerospace satellites streaming live temperature and CPU metrics. 

**PermuteX** demonstrates how to handle chaotic, late-arriving data streams, enforce strict schema contracts, and generate sub-second anomaly alerts using modern Data Engineering best practices.

---

##  Architecture & Tech Stack

![Dashboard Screenshot](https://github.com/KOUSHIC8976/PermuteX/blob/main/orchestration/include/Grafana_Dashboard.png)

The pipeline is entirely decoupled and operates in the following sequential flow:

1. **Data Generation:** Async Python simulators generating continuous, chaotic IoT telemetry.
2. **Nervous System (Message Broker):** **Apache Kafka** (running in KRaft mode, eliminating Zookeeper dependencies).
3. **Data Governance:** **Confluent Schema Registry** enforcing strict Avro data contracts to prevent schema drift.
4. **Stateful Compute:** **Apache Flink (PyFlink)** executing 10-second tumbling window aggregations and handling event-time watermarking.
5. **Real-Time Serving Layer:** **PostgreSQL** acting as the indexed Gold Layer for sub-second dashboard querying.
6. **Data Quality & Orchestration:** **Apache Airflow** running continuous SQL-based circuit breakers to detect thermal anomalies.
7. **Observability:** **Grafana** for live, auto-refreshing time-series visualization.

---
##  Demo

https://github.com/user-attachments/assets/302efa57-66c8-4a03-8d73-47993672c5e9

---
##  Key Technical Challenges Resolved

Key problems solved in this project include:

* **Dependency Hell & Classpath Management:** Manually mapping Java transitive dependencies (Guava, Jackson, Avro, Confluent Clients) for PyFlink to successfully deserialize schema-encoded messages.
* **Network Proxy Evasion:** Bypassing strict host OS firewall/VPN rules to allow raw HTTP Python requests to reach the Schema Registry.
* **The "Idle Partition" Flink Hang:** Implementing Watermark idle-timeouts to prevent global clock freezing when individual Kafka partitions drop in throughput.
* **JVM Timezone Collisions:** Forcing JVM initialization environments (`_JAVA_OPTIONS`) to `UTC` to prevent modern PostgreSQL drivers from rejecting legacy OS timezone strings during JDBC sink connections.

---

##  Quick Start

### Prerequisites
Before you begin, ensure you have the following installed:
* **[Docker Desktop](https://www.docker.com/products/docker-desktop/)** (Ensure Docker engine is running)
* **[Python 3.10](https://www.python.org/downloads/)**
* **[Astronomer CLI](https://docs.astronomer.io/astro/cli/install-cli)** (For Airflow orchestration)
* Java

### Usage
Spin up the decoupled microservices (Kafka, Schema Registry, Postgres, Grafana).
```bash
cd infrastructure
docker-compose up -d

cd ingestion
python producer.py

cd processing
python init_db.py       
python processor.py

cd orchestration
astro dev start
```

#  **Airflow**

* Navigate to Airflow UI at http://localhost:8080 to view the anomaly detection DAG.

* Go to Connections and select PostgreSQL.

* Configure the connection exactly as follows:

  **Connection Id:** permutex_pg_conn 

  **Connection Type:** Postgres

  **Host:** host.docker.internal 

  **Schema/Database:** permutex_db

  **Login:** permutex_user

  **Password:** permutex_password

   **Port:** 5432

#  **Grafana**

* Navigate to http://localhost:3000 (credentials: Username : admin | Password : admin)

* Go to Connections > Data Sources > Add data source and select PostgreSQL.

* Configure the connection exactly as follows:

  **Host:** postgres:5432 (Docker internal network routing)

  **Database:** permutex_db

  **User:** permutex_user

  **Password:** permutex_password

  **TLS/SSL Mode:** disable

* Click Save & test.

* Go to Dashboards > New Dashboard > Add Visualization and select PostgreSQL source.

* Switch the query builder to Code and paste the following SQL to generate the Tumbling Window time-series chart:
```bash
SELECT
  window_end AS "time",
  satellite_id AS "metric",
  max_temperature
FROM satellite_telemetry_gold
WHERE
  $__timeFilter(window_end)
ORDER BY window_end ASC;
```
* Apply and run it.


