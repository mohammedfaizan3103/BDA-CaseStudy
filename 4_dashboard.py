# ============================================================
# 4_dashboard.py — Streamlit Real-Time Dashboard
# Review submission form + live charts + alerts
# ============================================================

import time
import json
import sqlite3
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from kafka import KafkaProducer

# ── Page Config ──────────────────────────────────────────────
st.set_page_config(
    page_title="Review Intelligence System",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Custom CSS ───────────────────────────────────────────────
st.markdown("""
<style>
    .block-container { padding-top: 1rem; }
    .alert-box {
        background: rgba(255, 75, 75, 0.12);
        border-left: 4px solid #ff4b4b;
        padding: 10px 16px;
        border-radius: 6px;
        margin-bottom: 8px;
    }
    .feed-item {
        padding: 6px 0;
        border-bottom: 1px solid rgba(255,255,255,0.07);
    }
</style>
""", unsafe_allow_html=True)

# ── Kafka Producer ───────────────────────────────────────────
@st.cache_resource
def get_kafka_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            request_timeout_ms=3000
        )
    except Exception:
        return None

# ── DB Helpers ───────────────────────────────────────────────
DB_PATH = "reviews.db"

def fetch_data():
    try:
        conn    = sqlite3.connect(DB_PATH)
        stats   = pd.read_sql("SELECT * FROM product_stats ORDER BY total_reviews DESC", conn)
        history = pd.read_sql(
            "SELECT * FROM review_history ORDER BY timestamp DESC LIMIT 100", conn
        )
        conn.close()
        return stats, history
    except Exception:
        return pd.DataFrame(), pd.DataFrame()

# ============================================================
#  SIDEBAR
# ============================================================
with st.sidebar:
    st.header("📝 Submit a Review")
    product_id  = st.selectbox("Select Product", ["P101", "P102", "P103", "P104", "P105"])
    review_text = st.text_area("Review Text", height=130,
                               placeholder="Type your product review here...")

    submitted = st.button("🚀 Send to Pipeline", type="primary", use_container_width=True)
    if submitted:
        if review_text.strip():
            p = get_kafka_producer()
            if p:
                p.send("product-reviews", value={
                    "product_id": product_id,
                    "review":     review_text.strip(),
                    "timestamp":  time.time()
                })
                p.flush()
                st.success("✅ Review sent! Refresh in ~3 seconds.")
            else:
                st.error("❌ Cannot connect to Kafka. Is Docker running?")
        else:
            st.warning("Please write a review first.")

    st.divider()
    st.subheader("⚙️ Dashboard Settings")
    auto_refresh = st.checkbox("Auto-refresh (5s)", value=True)
    if st.button("🔄 Refresh Now", use_container_width=True):
        st.rerun()
    st.caption("Data pulled live from reviews.db")

# ============================================================
#  MAIN LAYOUT
# ============================================================
st.title("🧠 Real-Time Product Review Intelligence")
st.caption("Pipeline: **Kafka** → **Spark Structured Streaming** → **Spark ML** → **SQLite** → **Dashboard**")
st.divider()

stats_df, history_df = fetch_data()

# ── Empty state ──────────────────────────────────────────────
if stats_df.empty:
    st.info("""
    ### ⏳ Waiting for data...

    Run these scripts **in order** (each in its own WSL terminal):

    ```
    Terminal 1:  python 1_train_model.py       ← Run ONCE
    Terminal 2:  python 2_spark_streaming.py   ← Keep running
    Terminal 3:  python 3_kafka_producer.py    ← Keep running
    ```

    Then refresh this dashboard or enable auto-refresh.
    """)
    if auto_refresh:
        time.sleep(5)
        st.rerun()
    st.stop()

# ── Summary Metrics ──────────────────────────────────────────
st.subheader("📊 Pipeline Summary")

total      = int(stats_df["total_reviews"].sum())
pos        = int(stats_df["positive_count"].sum())
neg        = int(stats_df["negative_count"].sum())
pos_pct    = round(pos / total * 100, 1) if total > 0 else 0.0
avg_score  = round(float(stats_df["score"].mean()), 3)
n_alerts   = int((stats_df["alert"] != "").sum())

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("📦 Total Reviews",   total)
c2.metric("✅ Positive",        pos,  delta=f"{pos_pct}%")
c3.metric("❌ Negative",        neg)
c4.metric("📈 Avg Score",       avg_score,
          delta_color="normal" if avg_score >= 0 else "inverse")
c5.metric("🚨 Active Alerts",   n_alerts,
          delta_color="off" if n_alerts == 0 else "inverse")

st.divider()

# ── Alerts ───────────────────────────────────────────────────
alerts = stats_df[stats_df["alert"] != ""]
if not alerts.empty:
    st.subheader("🚨 Active Alerts")
    for _, row in alerts.iterrows():
        neg_pct = round(row["negative_count"] / row["total_reviews"] * 100, 1) if row["total_reviews"] > 0 else 0
        st.markdown(
            f'<div class="alert-box">🔴 <b>{row["product_id"]}</b>: {row["alert"]} '
            f'&nbsp;|&nbsp; Negative rate: <b>{neg_pct}%</b> '
            f'({int(row["negative_count"])}/{int(row["total_reviews"])} reviews)</div>',
            unsafe_allow_html=True
        )
    st.divider()

# ── Charts Row 1 ─────────────────────────────────────────────
col_l, col_r = st.columns(2)

with col_l:
    st.subheader("📊 Reviews by Sentiment")
    fig1 = go.Figure()
    fig1.add_trace(go.Bar(
        name="✅ Positive",
        x=stats_df["product_id"],
        y=stats_df["positive_count"],
        marker_color="#2ecc71",
        text=stats_df["positive_count"],
        textposition="outside"
    ))
    fig1.add_trace(go.Bar(
        name="❌ Negative",
        x=stats_df["product_id"],
        y=stats_df["negative_count"],
        marker_color="#e74c3c",
        text=stats_df["negative_count"],
        textposition="outside"
    ))
    fig1.update_layout(
        barmode="group", height=360,
        margin=dict(t=15, b=10, l=10, r=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#eee"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02)
    )
    st.plotly_chart(fig1, use_container_width=True)

with col_r:
    st.subheader("🎯 Product Quality Score")
    score_colors = [
        "#e74c3c" if s < -0.2 else "#f39c12" if s < 0.3 else "#2ecc71"
        for s in stats_df["score"]
    ]
    fig2 = go.Figure(go.Bar(
        x=stats_df["product_id"],
        y=stats_df["score"],
        marker_color=score_colors,
        text=[f"{s:.3f}" for s in stats_df["score"]],
        textposition="outside"
    ))
    fig2.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.3)")
    fig2.update_layout(
        height=360,
        yaxis=dict(range=[-1.15, 1.15], title="Score (−1 to +1)"),
        margin=dict(t=15, b=10, l=10, r=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#eee")
    )
    st.plotly_chart(fig2, use_container_width=True)

# ── Sentiment Pie Charts ──────────────────────────────────────
st.subheader("🥧 Sentiment Distribution per Product")
pie_cols = st.columns(len(stats_df))
for i, (_, row) in enumerate(stats_df.iterrows()):
    with pie_cols[i]:
        if row["total_reviews"] > 0:
            fig3 = go.Figure(go.Pie(
                labels=["Positive", "Negative"],
                values=[max(int(row["positive_count"]), 0),
                        max(int(row["negative_count"]), 0)],
                marker_colors=["#2ecc71", "#e74c3c"],
                hole=0.5,
                textinfo="percent",
                hovertemplate="%{label}: %{value} reviews<extra></extra>"
            ))
            fig3.update_layout(
                title=dict(text=f"<b>{row['product_id']}</b>", x=0.5, font=dict(size=14)),
                height=210,
                margin=dict(t=35, b=5, l=5, r=5),
                showlegend=False,
                paper_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#eee")
            )
            st.plotly_chart(fig3, use_container_width=True)
            alert_badge = " 🚨" if row["alert"] else ""
            st.caption(f"Score: **{row['score']:.2f}**  ·  {int(row['total_reviews'])} total{alert_badge}")

st.divider()

# ── Stats Table ───────────────────────────────────────────────
st.subheader("📋 Full Product Statistics")
tbl = stats_df[["product_id", "total_reviews", "positive_count",
                 "negative_count", "positive_pct", "score", "alert"]].copy()
tbl.columns = ["Product", "Total", "👍 Positive", "👎 Negative",
               "Pos%", "Score", "Alert"]
tbl["Pos%"]  = tbl["Pos%"].apply(lambda x: f"{x:.1f}%")
tbl["Score"] = tbl["Score"].apply(lambda x: f"{x:.3f}")
st.dataframe(tbl, use_container_width=True, hide_index=True)

st.divider()

# ── Live Review Feed ──────────────────────────────────────────
st.subheader("🕒 Live Review Feed (last 15)")
if not history_df.empty:
    for _, row in history_df.head(15).iterrows():
        icon  = "✅" if row["sentiment"] == "positive" else "❌"
        label = row["sentiment"].upper()
        st.markdown(
            f'<div class="feed-item">{icon} <b>{row["product_id"]}</b> '
            f'— {row["review"][:90]}… '
            f'<code style="font-size:11px">{label}</code></div>',
            unsafe_allow_html=True
        )
else:
    st.info("No reviews in history yet.")

# ── Auto Refresh ──────────────────────────────────────────────
if auto_refresh:
    time.sleep(5)
    st.rerun()
