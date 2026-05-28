# ============================================================
# 2_spark_streaming.py — Spark Structured Streaming Consumer
# Reads Kafka → ML Prediction → Aggregation → SQLite DB
# ============================================================

import sqlite3
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml import PipelineModel

# ── Initialize SQLite Database ──────────────────────────────
DB_PATH = "reviews.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS product_stats (
            product_id     TEXT PRIMARY KEY,
            total_reviews  INTEGER DEFAULT 0,
            positive_count INTEGER DEFAULT 0,
            negative_count INTEGER DEFAULT 0,
            positive_pct   REAL    DEFAULT 0.0,
            score          REAL    DEFAULT 0.0,
            alert          TEXT    DEFAULT '',
            last_updated   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS review_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id  TEXT,
            review      TEXT,
            sentiment   TEXT,
            timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

init_db()
print("✅ SQLite database initialized → reviews.db")

# ── Track previous negative rates for trend detection ───────
prev_neg_rates = {}

def process_batch(batch_df, batch_id):
    """Called every 2 seconds with a micro-batch of predictions."""
    count = batch_df.count()
    if count == 0:
        return

    rows = batch_df.collect()
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()

    for row in rows:
        pid       = row["product_id"]
        review    = row["review"]
        pred      = row["prediction"]    # 1.0 = positive, 0.0 = negative
        sentiment = "positive" if pred == 1.0 else "negative"
        is_pos    = 1 if sentiment == "positive" else 0
        is_neg    = 1 - is_pos

        # ── Insert into review history ──
        c.execute(
            "INSERT INTO review_history (product_id, review, sentiment) VALUES (?, ?, ?)",
            (pid, review, sentiment)
        )

        # ── Upsert product stats (INSERT or UPDATE) ──
        c.execute('''
            INSERT INTO product_stats (product_id, total_reviews, positive_count, negative_count)
            VALUES (?, 1, ?, ?)
            ON CONFLICT(product_id) DO UPDATE SET
                total_reviews  = total_reviews  + 1,
                positive_count = positive_count + ?,
                negative_count = negative_count + ?
        ''', (pid, is_pos, is_neg, is_pos, is_neg))

        # ── Recalculate derived metrics ──
        c.execute(
            "SELECT total_reviews, positive_count, negative_count FROM product_stats WHERE product_id=?",
            (pid,)
        )
        stats = c.fetchone()

        if stats and stats[0] > 0:
            total, pos, neg = stats

            # Sentiment Percentage
            pos_pct = round((pos / total) * 100, 2)

            # Product Score: (positive - negative) / total  ∈ [-1, 1]
            score = round((pos - neg) / total, 4)

            # ── Trend Detection (negative spike) ──
            curr_neg_rate = neg / total
            prev_neg_rate = prev_neg_rates.get(pid, 0.0)
            alert = ""
            if total >= 5 and (curr_neg_rate - prev_neg_rate) > 0.15:
                alert = "⚠️ Negative spike detected!"
                print(f"\n🚨 ALERT [{pid}]: Negative spike! "
                      f"{prev_neg_rate:.0%} → {curr_neg_rate:.0%} negative rate\n")
            prev_neg_rates[pid] = curr_neg_rate

            c.execute('''
                UPDATE product_stats
                SET positive_pct = ?, score = ?, alert = ?, last_updated = CURRENT_TIMESTAMP
                WHERE product_id = ?
            ''', (pos_pct, score, alert, pid))

    conn.commit()
    conn.close()

    sentiments = [("+" if r["prediction"] == 1.0 else "-") for r in rows]
    summary    = "".join(sentiments)
    print(f"✅ Batch {batch_id:04d} | {count} reviews processed | {summary}")


# ── Spark Session ────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("ProductReviewStreaming") \
    .master("local[2]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoint") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark Session started")

# ── Load Trained ML Model ────────────────────────────────────
MODEL_PATH = "models/sentiment_model"
if not os.path.exists(MODEL_PATH):
    print("❌ Model not found at models/sentiment_model")
    print("   Please run:  python 1_train_model.py  first!")
    spark.stop()
    exit(1)

print("📦 Loading ML model...")
model = PipelineModel.load(MODEL_PATH)
print("✅ ML Model loaded successfully")

# ── Kafka Message Schema ─────────────────────────────────────
schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("review",     StringType(), True),
    StructField("timestamp",  DoubleType(), True)
])

# ── Read Stream from Kafka ───────────────────────────────────
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "product-reviews") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# ── Parse JSON Payload ───────────────────────────────────────
parsed_df = raw_df \
    .select(from_json(col("value").cast("string"), schema).alias("d")) \
    .select("d.*") \
    .filter(col("review").isNotNull() & col("product_id").isNotNull())

# ── Apply ML Model for Sentiment Prediction ──────────────────
predictions_df = model.transform(parsed_df)

# ── Select Output Columns ────────────────────────────────────
output_df = predictions_df.select("product_id", "review", "prediction")

# ── Start Streaming Query ────────────────────────────────────
query = output_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .trigger(processingTime="2 seconds") \
    .start()

print("\n" + "="*60)
print("🚀 Spark Streaming is RUNNING")
print("   Listening on Kafka topic: product-reviews")
print("   Writing results to:       reviews.db")
print("   Processing every:         2 seconds")
print("\n   → In another terminal, run:  python 3_kafka_producer.py")
print("   → Dashboard at:              http://localhost:8501")
print("="*60 + "\n")

query.awaitTermination()
