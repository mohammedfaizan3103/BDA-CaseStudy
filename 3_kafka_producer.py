# ============================================================
# 3_kafka_producer.py — Simulates a stream of product reviews
# Run in a SEPARATE terminal after Spark Streaming starts
# ============================================================

import json
import time
import random
from kafka import KafkaProducer

# ── Connect to Kafka ─────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

PRODUCTS = ["P101", "P102", "P103", "P104", "P105"]

POSITIVE_REVIEWS = [
    "This product is absolutely amazing and works perfectly every time",
    "Great quality very satisfied with my purchase would highly recommend",
    "Excellent fast delivery exceeded my expectations completely impressed",
    "Best product I have bought this year love it so much will buy again",
    "Works exactly as described very happy superb quality great value for money",
    "Outstanding performance build quality is top notch definitely five stars",
    "Fantastic product does exactly what it says highly satisfied customer here",
    "Very impressed durable and well made perfect for everyday use overall",
    "Superb item arrived quickly and was well packaged would buy again",
    "Incredible value for money this product has completely changed my life",
    "Brilliant product easy to use and set up works absolutely flawlessly",
    "Really happy with this purchase great construction and design overall",
    "Perfect item works beautifully and looks great highly recommend to all",
    "Solid product good materials and great finish excellent value overall",
    "Exceeded all my expectations this is definitely a must buy product",
    "Wonderful purchase fast shipping great quality everything I wanted",
    "Amazing product exactly what I needed arrived on time well packaged",
    "Love this product so much use it every day without any issues",
]

NEGATIVE_REVIEWS = [
    "Terrible product broke after one use complete and utter waste of money",
    "Worst purchase ever very disappointed does not work at all useless",
    "Poor quality stopped working after two days total garbage avoid this",
    "Do not buy this product complete scam not at all as described",
    "Very bad quality returned immediately after receiving this awful item",
    "Broken on arrival customer service was absolutely no help whatsoever",
    "Absolute rubbish falling apart after first use would never recommend",
    "Shocking quality product failed immediately not recommended avoid",
    "Cheap nasty product regret buying this waste of hard earned money",
    "Arrived damaged and defective had to request full refund terrible",
    "Complete disappointment nothing as advertised poor materials used here",
    "Stopped working within a week terrible quality control avoid buying",
    "Misleading product description total scam do not waste your money",
    "Awful experience broken parts and terrible build quality overall",
    "Returned for full refund useless product that simply does not work",
    "Do not waste your money on this cheaply made garbage product",
    "Nothing works as advertised terrible customer support avoid at all costs",
]


def send_review(product_id=None, force_negative=False):
    pid = product_id or random.choice(PRODUCTS)

    if force_negative:
        is_positive = random.random() > 0.85    # 85% negative during spike
    else:
        is_positive = random.random() > 0.35    # 65% positive normally

    review = random.choice(POSITIVE_REVIEWS if is_positive else NEGATIVE_REVIEWS)

    message = {
        "product_id": pid,
        "review":     review,
        "timestamp":  time.time()
    }
    producer.send("product-reviews", value=message)

    icon = "✅" if is_positive else "❌"
    print(f"{icon} [{pid}] {review[:65]}...")


# ── Main ─────────────────────────────────────────────────────
print("=" * 65)
print("📡  Kafka Producer — Simulating Product Review Stream")
print("=" * 65)
print("Phase 1  (reviews  0–79):   Normal traffic    (~65% positive)")
print("Phase 2  (reviews 80–129):  Negative SPIKE on P102 (85% negative)")
print("Phase 3  (reviews 130+):    Back to normal traffic")
print("\nSending 1 review every 0.5 seconds. Press Ctrl+C to stop.\n")

count       = 0
spike_on    = False

try:
    while True:
        # ── Phase transitions ──
        if count == 80 and not spike_on:
            spike_on = True
            print("\n" + "="*50)
            print("🚨 NEGATIVE SPIKE STARTING on P102!")
            print("   Watch for alert on the dashboard...")
            print("="*50 + "\n")

        elif count == 130 and spike_on:
            spike_on = False
            print("\n" + "="*50)
            print("✅ Spike ended — traffic returning to normal")
            print("="*50 + "\n")

        # ── Send review ──
        if spike_on:
            # During spike: P102 gets heavily negative, others normal
            pid = random.choice(PRODUCTS)
            if pid == "P102":
                send_review(product_id="P102", force_negative=True)
            else:
                send_review(product_id=pid)
        else:
            send_review()

        count += 1
        time.sleep(0.5)

except KeyboardInterrupt:
    print(f"\n\n🛑 Producer stopped after sending {count} reviews.")
    producer.flush()
    producer.close()
