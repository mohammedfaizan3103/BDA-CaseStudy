# ============================================================
# 1_train_model.py — Train Spark ML Sentiment Model (run once)
# ============================================================

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# ------ Spark Session ------
spark = SparkSession.builder \
    .appName("SentimentModelTraining") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark Session started")

# ------ Load Dataset ------
dataset_path = "data/train.csv"

if os.path.exists(dataset_path):
    print("📂 Loading Amazon Reviews dataset...")
    df = spark.read.csv(dataset_path, header=False, inferSchema=True)
    df = df.toDF("polarity", "title", "text")
    # polarity: 1=negative → label 0,  polarity: 2=positive → label 1
    df = df.withColumn("label", (col("polarity") - 1).cast("double"))
    df = df.select("label", "text").filter(col("text").isNotNull())
    df = df.limit(60000)
    print(f"✅ Dataset loaded. Using 60,000 rows for training.")
else:
    print("⚠️  Dataset not found. Generating synthetic sample data for demo...")
    positive = [
        "This product is absolutely amazing and works perfectly every time",
        "Great quality very satisfied with my purchase would recommend to everyone",
        "Excellent fast delivery exceeded my expectations completely impressed",
        "Best product I have bought this year love it so much will buy again",
        "Works exactly as described very happy superb quality great value",
        "Outstanding performance build quality is top notch five stars",
        "Fantastic product does exactly what it says highly satisfied customer",
        "Very impressed durable and well made perfect for everyday use",
        "Superb item arrived quickly and was well packaged five stars",
        "Incredible value for money this product has changed my life",
        "Brilliant product easy to use and set up works flawlessly",
        "Really happy with this purchase great construction and design",
        "Perfect gift works beautifully and looks great highly recommend",
        "Solid product good materials and great finish excellent value",
        "Exceeded all my expectations this is a must buy product",
    ]
    negative = [
        "Terrible product broke after one use complete waste of money",
        "Worst purchase ever very disappointed does not work at all",
        "Poor quality stopped working after two days total garbage avoid",
        "Do not buy this product complete scam not as described at all",
        "Very bad quality returned immediately after receiving this awful item",
        "Broken on arrival customer service was absolutely no help whatsoever",
        "Absolute rubbish falling apart after first use would not recommend",
        "Shocking quality product failed immediately not recommended avoid this",
        "Cheap nasty product regret buying this waste of hard earned money",
        "Arrived damaged and defective requested refund immediately terrible",
        "Complete disappointment nothing as advertised poor materials used",
        "Stopped working within a week terrible quality control avoid buying",
        "Misleading product description total scam do not waste your money",
        "Awful experience broken parts and terrible build quality overall",
        "Returned for full refund useless product that simply does not work",
    ]
    data = [(1.0, r) for r in positive] + [(0.0, r) for r in negative]
    data = data * 200   # ~6000 rows
    schema = StructType([
        StructField("label", DoubleType(), True),
        StructField("text",  StringType(), True),
    ])
    df = spark.createDataFrame(data, schema)
    print(f"✅ Sample data generated: {df.count()} rows")

# ------ Train / Test Split ------
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
print(f"📊 Train: {train_df.count()}  |  Test: {test_df.count()}")

# ------ ML Pipeline ------
#  Text → Tokenizer → StopWordsRemover → HashingTF → IDF → LogisticRegression
tokenizer = Tokenizer(inputCol="text",     outputCol="words")
stopwords = StopWordsRemover(inputCol="words",    outputCol="filtered")
hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=10000)
idf       = IDF(inputCol="rawFeatures",    outputCol="features")
lr        = LogisticRegression(maxIter=20, regParam=0.01,
                               featuresCol="features", labelCol="label")

pipeline = Pipeline(stages=[tokenizer, stopwords, hashingTF, idf, lr])

# ------ Train ------
print("\n🔧 Training model... (this may take 2–5 minutes for real dataset)")
model = pipeline.fit(train_df)
print("✅ Training complete!")

# ------ Evaluate ------
predictions = model.transform(test_df)
evaluator   = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol="label")
auc         = evaluator.evaluate(predictions)
print(f"📈 Model AUC Score: {auc:.4f}")

# ------ Show Sample Predictions ------
print("\n--- Sample Predictions (label=actual, prediction=model output) ---")
predictions.select("text", "label", "prediction").show(10, truncate=55)

# ------ Save Model ------
model_path = "models/sentiment_model"
if os.path.exists(model_path):
    import shutil
    shutil.rmtree(model_path)

model.save(model_path)
print(f"\n💾 Model saved → {model_path}")

spark.stop()
print("✅ Done! You can now run 2_spark_streaming.py")
