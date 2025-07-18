# Real-Time Portfolio Optimization & Data Streaming

This project aims to optimize an asset portfolio in **real-time**, specifically using a use case based on the **Moroccan stock exchange** (Casablanca Stock Exchange). It is a fully backend system focused on **stream processing**, **portfolio reallocation**, and **metric visualization**. There is no web interface — everything is handled through distributed data pipelines.

---

## 🧠 Project Overview

The core idea is to simulate and optimize portfolio weights based on real-time price streaming of 10 selected assets. It leverages the **Black-Litterman model** for initial portfolio weight allocation.

The system:
- Streams simulated stock prices.
- Updates the **covariance matrix** and **expected returns** using the **Welford online algorithm**.
- Recalculates the **Sharpe ratio** as a key metric.
- If the Sharpe ratio falls below a defined threshold, triggers **portfolio reallocation**.

---

## ⚙️ Architecture

```text
+-------------------------+
|     Price Producer      |
|  (Kafka + Scheduler)    |
+-------------------------+
           |
           v
+-------------------------+
|      Flink Consumer     |
|  - Windowed Log Return  |
|  - Portfolio Stats      |
|  - Sharpe Ratio Calc    |
+-------------------------+
           |
           v
+-------------------------+
|    Portfolio Updater    |
|  (Python Consumer +     |
|   Rebalancing Logic)    |
+-------------------------+
           |
           v
+-------------------------+
|  Metrics Visualization  |
| (Grafana + InfluxDB)    |
+-------------------------+
````

---

## 🧩 Key Components

### 1. Kafka Producer (Java/Scala)

* **Simulated Real-Time Pricing**:

  * Uses a scheduler to fetch stock prices every 2ms (approximate simulation).
  * Data is pulled from the Casablanca Stock Exchange public API.
* **AVRO Serialization**:

  * Prices are serialized using AVRO for efficiency and schema enforcement.
  * AVRO schemas are located in:

    ```
    /kafka/Producer/src/main/resources/avro/
    ```
* **Topics**:

  * `stock-prices`
  * `portfolio-stats`
  * `weights`

> 📁 *Kafka producer config files can be found under:*
> `/kafka/Producer/src/main/resources/config/`

---

### 2. Apache Flink (Java/Scala)

* **Real-Time Stream Processing**:

  * Watermarks are used to ensure event-time accuracy.
  * Each price is mapped by `ticker`, and weights/stats are mapped by `portfolioId`.

* **Windowed Log Return Calculation**:

  * Implemented in:

    ```
    /flink/src/main/java/utils/LogReturnWindowFunction.java
    ```
  * Adds a small epsilon for numerical consistency during simulation.

* **Portfolio Statistics Update**:

  * Covariance matrix and expected returns are updated with Welford's algorithm.
  * Handled in:

    ```
    /flink/src/main/java/functions/PortfolioUpdateFunction.java
    ```

* **Sharpe Ratio Calculation**:

  * Combines latest weights and stats using a `CoProcessFunction`.
  * Formula:

    ```
    Sharpe Ratio = (Expected Return - Risk Free Rate) / Portfolio Risk
    ```

---

### 3. Python Rebalancer

* **Consumer using `kafka-python`**:

  * Consumes:

    * Portfolio Weights
    * Portfolio Metrics (Sharpe Ratio, Expected Returns, etc.)
    * Portfolio Stats (Covariance Matrix)

* **Mean-Variance Optimization**:

  * Reallocation triggered if Sharpe ratio breaches a threshold.
  * Uses [`PyPortfolioOpt`](https://github.com/robertmartin8/PyPortfolioOpt) with regularization for diversification.

* **Publishes New Weights**:

  * Sends updated weights back to the Kafka topic `weights`.

---

## 📊 Visualization

* **Grafana + InfluxDB**:

  * All metrics and portfolio updates are stored and visualized in real-time.
  * Alerts can be configured (e.g., via email) if thresholds are crossed.

* **Optional Storage**:

  * Data can be persisted to:

    * **TimeScaleDB** (aka TigerDB)
    * **MongoDB Time Series**

---

## 🔐 DevOps & Deployment

* **Kubernetes**:

  * Manages deployment for:

    * Kafka brokers
    * Flink jobs
    * Grafana dashboards
    * InfluxDB

* **Cloud Deployment (Planned)**:

  * Future support for AWS:

    * **MSK (Managed Kafka)**
    * **Amazon Flink**
    * **EC2 Instances**

---

## 📌 Getting Started

### Prerequisites

* Apache Kafka
* Apache Flink
* Python 3.9+
* Docker / Kubernetes (Optional)
* InfluxDB & Grafana
* TimeScaleDB / MongoDB (optional for long-term storage)

---

## 🧪 Running the Project

1. **Kafka Setup**

   * Run Kafka and Zookeeper.
   * Configure AVRO serializers.
   * Launch the price producer.

     > 📍 Code location: `/kafka/Producer/`

2. **Flink Jobs**

   * Launch the Flink streaming job:

     * Log return calculator
     * Portfolio update logic
     * Sharpe ratio calculation

     > 📍 Code location: `/flink/`

3. **Python Optimizer**

   * Run the Python consumer.
   * Start weight reallocation and streaming based on metric threshold.

     > 📍 Code location: `/python/optimizer/`

4. **Visualization**

   * Connect InfluxDB to Grafana.
   * Configure dashboards and alerts.

---

## 🧠 Notes & Considerations

* **Risk Profile**: Assumes a neutral risk-averse investor (risk aversion = 1). You can modify this as needed.
* **Real-Time Simulation**: Casablanca Exchange doesn’t provide free real-time WebSocket APIs, so prices are **simulated using polling**.
* **Latency**: While Morocco's stock market has lower liquidity than major global exchanges, the simulation tries to maintain consistent behavior.

---

## 🛠️ Contributing

Contributions are welcome! If you want to improve this project, feel free to fork it, submit PRs, or open issues.

---

## 📜 License

This project is licensed under the MIT License.

---

## 🙋 Contact

For questions or feedback, reach out via GitHub Issues or open a discussion in the project.

