# Real-Time Product Review Intelligence System

A real-time analytics platform for processing, analysing, and monitoring e-commerce product reviews using **Apache Kafka, Apache Spark Structured Streaming, Spark MLlib, and Streamlit**.

The system simulates live review streams, performs sentiment analysis on incoming reviews, detects sudden negative sentiment spikes, and visualizes product-level insights on a live dashboard.

---

# Overview

Modern e-commerce platforms receive a massive number of product reviews every minute. Traditional batch-processing systems are unable to provide immediate insights into customer sentiment or detect emerging product issues quickly.

This project addresses that limitation by building a complete end-to-end streaming analytics pipeline capable of processing reviews in near real time.

The pipeline includes:

* Real-time review ingestion using Kafka
* Stream processing with Spark Structured Streaming
* Machine learning-based sentiment classification
* Product-wise aggregation and scoring
* Negative sentiment spike detection
* Live analytics dashboard using Streamlit

---

# Features

## Real-Time Review Streaming

Reviews are continuously streamed into Kafka topics using a Python producer that simulates real-world e-commerce traffic.

## Sentiment Analysis

Each review is classified as:

* Positive
* Negative

using a Spark MLlib Logistic Regression model trained on the Amazon Review Polarity Dataset.

## Product-Level Analytics

The system computes:

* Positive review percentage
* Negative review percentage
* Total review count
* Product quality score

for every product in real time.

## Trend Detection

The pipeline monitors sudden increases in negative reviews and raises alerts whenever a negative sentiment spike is detected.

## Live Dashboard

A Streamlit dashboard displays:

* Real-time sentiment metrics
* Product scores
* Recent reviews
* Trend alerts
* Live processing updates

---

# Tech Stack

| Technology                        | Purpose                       |
| --------------------------------- | ----------------------------- |
| Apache Kafka                      | Real-time message streaming   |
| Apache Spark Structured Streaming | Stream processing             |
| Spark MLlib                       | Sentiment classification      |
| PySpark                           | Spark integration with Python |
| Streamlit                         | Dashboard and visualization   |
| Docker                            | Kafka deployment              |
| Python                            | Backend scripts               |
| WSL2 Ubuntu                       | Development environment       |

---

# System Architecture

The project follows a four-layer architecture:

## 1. Ingestion Layer

A Kafka Producer streams product reviews into a Kafka topic.

## 2. Processing Layer

Spark Structured Streaming reads reviews in micro-batches and performs sentiment prediction.

## 3. Analytics Layer

Aggregated product metrics and trend analysis are computed.

## 4. Visualization Layer

A Streamlit dashboard displays live analytics and alerts.

---

# Machine Learning Pipeline

The sentiment classifier is built using Spark MLlib.

## Pipeline Stages

1. Tokenizer
2. StopWordsRemover
3. HashingTF
4. IDF
5. Logistic Regression

---

# Dataset

Dataset used:

* Amazon Review Polarity Dataset

Dataset fields:

* label
* title
* text

A balanced subset of approximately 60,000 reviews was used for training and evaluation.

---

# Project Structure

```bash id="3r09qn"
review-intelligence/
├── docker-compose.yml
├── requirements.txt
├── data/
│   └── amazon_reviews.csv
├── train_model.py
├── producer.py
├── stream_consumer.py
├── dashboard.py
└── models/
```

---

# Installation and Setup

## Prerequisites

Install the following before starting:

* Python 3.10+
* Java JDK 11 or 17
* Apache Spark 3.4+
* Docker Desktop
* WSL2 Ubuntu

---

# Install Dependencies

```bash id="epvqhl"
pip install pyspark==3.4.1 kafka-python streamlit pandas
```

---

# Start Kafka and Zookeeper

```bash id="3wn5yq"
docker-compose up -d
```

Verify containers:

```bash id="xyg4qt"
docker ps
```

---

# Train the ML Model

```bash id="wkf8vq"
python train_model.py
```

The trained model will be saved locally and reused during streaming.

---

# Run Spark Streaming Consumer

```bash id="d2x7b0"
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
stream_consumer.py
```

---

# Start Kafka Producer

```bash id="k1p5u4"
python producer.py
```

---

# Launch Dashboard

```bash id="v8f5tc"
streamlit run dashboard.py
```

Open in browser:

```bash id="o8hrz8"
http://localhost:8501
```

---

# Output

The dashboard displays:

* Product sentiment percentages
* Positive vs negative review counts
* Product quality scores
* Live review feed
* Negative sentiment alerts

---

# Results

* Real-time review ingestion successfully implemented
* Near real-time sentiment classification achieved
* Micro-batch processing working correctly
* Trend detection alerts generated successfully
* Live dashboard updates functioning properly

---

# Future Improvements

* BERT-based sentiment classifier
* Multilingual review processing
* Fake review detection
* Persistent database integration
* Real-time recommendation integration
* LLM-based sentiment summaries


