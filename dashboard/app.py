import os
import sys
import json
import glob
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
from config.local_config import GOLD_DIR, LOGS_DIR

st.set_page_config(
    page_title="Olist Analytics",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    .main { background: #F8FAFC; }
    .block-container { padding: 1.5rem 2.5rem; }
    section[data-testid="stSidebar"] { background: #0F172A; }
    div[data-testid="stTabs"] button {
        font-size: 13px; font-weight: 500; color: #64748B; padding: 8px 16px;
    }
    div[data-testid="stTabs"] button[aria-selected="true"] {
        color: #1D4ED8; font-weight: 600; border-bottom: 2px solid #1D4ED8;
    }
    .kpi {
        background: white; border-radius: 10px; padding: 18px 20px;
        border-left: 4px solid #1D4ED8;
        box-shadow: 0 1px 2px rgba(0,0,0,0.05); margin-bottom: 8px;
    }
    .kpi-label {
        font-size: 10px; font-weight: 600; color: #94A3B8;
        text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 4px;
    }
    .kpi-value { font-size: 24px; font-weight: 700; color: #0F172A; line-height: 1; }
    .kpi-sub { font-size: 11px; color: #94A3B8; margin-top: 3px; }
    .kpi-trend { font-size: 11px; margin-top: 4px; font-weight: 600; }
    .sec {
        font-size: 10px; font-weight: 600; color: #94A3B8;
        text-transform: uppercase; letter-spacing: 1px;
        border-bottom: 1px solid #E2E8F0; padding-bottom: 6px; margin-bottom: 14px;
    }
    .insight-box {
        background: #EFF6FF; border-left: 4px solid #1D4ED8;
        padding: 12px 16px; border-radius: 8px; margin: 12px 0;
        font-size: 12px; color: #1E40AF; line-height: 1.6;
    }
    .insight-box.warning {
        background: #FFF7ED; border-left-color: #D97706; color: #92400E;
    }
    .insight-box.danger {
        background: #FFF1F2; border-left-color: #DC2626; color: #991B1B;
    }
    .insight-box.success {
        background: #F0FDF4; border-left-color: #059669; color: #065F46;
    }
    hr.divider { border: none; border-top: 1px solid #E2E8F0; margin: 12px 0; }
</style>
""", unsafe_allow_html=True)

C = {
    "blue": "#1D4ED8", "blue2": "#3B82F6", "blue3": "#60A5FA", "blue4": "#93C5FD",
    "blue_light": "#EFF6FF", "dark": "#0F172A", "gray": "#64748B",
    "green": "#059669", "red": "#DC2626", "orange": "#D97706",
    "yellow": "#F59E0B", "teal": "#0D9488", "purple": "#7C3AED",
    "scale": [[0, "#DBEAFE"], [1, "#1D4ED8"]]
}

CATEGORY_COLORS = [
    C["blue"], C["teal"], C["purple"], C["orange"],
    C["green"], C["blue2"], C["blue3"]
]

BASE = dict(
    plot_bgcolor="white", paper_bgcolor="white",
    font=dict(family="Inter", size=11, color="#334155"),
    margin=dict(t=32, b=16, l=16, r=16),
    xaxis=dict(showgrid=True, gridcolor="#F1F5F9",
               linecolor="#E2E8F0", tickfont=dict(size=10)),
    yaxis=dict(showgrid=True, gridcolor="#F1F5F9",
               linecolor="#E2E8F0", tickfont=dict(size=10)),
    hoverlabel=dict(bgcolor="white", font_size=11,
                    font_family="Inter", bordercolor="#E2E8F0")
)


def L(fig):
    fig.update_layout(**BASE)
    return fig


def sec(t):
    st.markdown(f"<div class='sec'>{t}</div>", unsafe_allow_html=True)


def kpi(label, value, sub=None, color=None, trend=None):
    c = color or C["blue"]
    s = f"<div class='kpi-sub'>{sub}</div>" if sub else ""
    t = ""
    if trend is not None:
        if trend > 0:
            t = f"<div class='kpi-trend' style='color: {C['red']};'>▲ {abs(trend)}% vs prev period</div>"
        elif trend < 0:
            t = f"<div class='kpi-trend' style='color: {C['green']};'>▼ {abs(trend)}% vs prev period</div>"
        else:
            t = f"<div class='kpi-trend' style='color: {C['gray']};'>→ No change</div>"
    st.markdown(f"""
        <div class='kpi' style='border-left-color:{c};'>
            <div class='kpi-label'>{label}</div>
            <div class='kpi-value'>{value}</div>
            {s}{t}
        </div>
    """, unsafe_allow_html=True)


def insight(text, kind="info"):
    icons = {"info": "💡", "warning": "⚠️", "danger": "🚨", "success": "✅"}
    css = {"info": "", "warning": " warning", "danger": " danger", "success": " success"}
    st.markdown(f"""
        <div class='insight-box{css.get(kind, "")}'>
            {icons.get(kind, "💡")} <strong>Insight:</strong> {text}
        </div>
    """, unsafe_allow_html=True)


def load(table):
    path = os.path.join(GOLD_DIR, table)
    if not os.path.exists(path):
        return None
    files = [f for f in os.listdir(path) if f.endswith(".parquet")]
    if not files:
        return None
    try:
        df = pd.read_parquet(path)
        return df if len(df) > 0 else None
    except Exception:
        return None


def apply_date_filter(df, date_col, start, end):
    if date_col not in df.columns:
        return df
    try:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        return df[(df[date_col] >= pd.Timestamp(start)) &
                  (df[date_col] <= pd.Timestamp(end))]
    except Exception:
        return df


def compute_trend(df, date_col, metric_func):
    if date_col not in df.columns or len(df) < 2:
        return None
    try:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.dropna(subset=[date_col])
        mid = df[date_col].min() + (df[date_col].max() - df[date_col].min()) / 2
        prev = metric_func(df[df[date_col] < mid])
        curr = metric_func(df[df[date_col] >= mid])
        if prev and prev != 0:
            return round((curr - prev) / prev * 100, 1)
        return 0
    except Exception:
        return None


def sidebar_filters():
    with st.sidebar:
        st.markdown("""
            <div style='padding:16px 0 10px;
                        font-size:15px; font-weight:700; color:white;'>
                Olist Analytics
            </div>
        """, unsafe_allow_html=True)

        st.markdown("""
            <div style='font-size:10px; font-weight:600; color:#475569;
                        text-transform:uppercase; letter-spacing:1px;
                        margin:12px 0 8px;'>
                Pipeline Status
            </div>
        """, unsafe_allow_html=True)

        for layer in ["bronze", "silver", "gold"]:
            p = os.path.join(os.path.dirname(GOLD_DIR), layer)
            ok = (os.path.exists(p) and
                  any(f for f in os.listdir(p) if not f.startswith("."))
                  ) if os.path.exists(p) else False
            dot = "●" if ok else "○"
            col = "#34D399" if ok else "#475569"
            st.markdown(f"""
                <div style='padding:6px 0; border-bottom:1px solid #1E293B;'>
                    <span style='color:{col};font-size:12px;'>{dot}</span>
                    <span style='color:#CBD5E1;font-size:12px;margin-left:8px;'>
                        {layer.title()}
                    </span>
                </div>
            """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        st.markdown("""
            <div style='font-size:10px; font-weight:600; color:#475569;
                        text-transform:uppercase; letter-spacing:1px;
                        margin-bottom:8px;'>
                Date Range Filter
            </div>
        """, unsafe_allow_html=True)
        date_start = st.date_input("From", value=pd.to_datetime("2017-01-01"),
                                   key="date_start",
                                   label_visibility="collapsed")
        date_end = st.date_input("To", value=pd.to_datetime("2018-12-31"),
                                 key="date_end",
                                 label_visibility="collapsed")

        if st.button("↻ Refresh", use_container_width=True):
            st.rerun()

        st.markdown("""
            <div style='margin-top:16px; font-size:10px;
                        color:#475569; line-height:1.9;'>
                Sample · 1,000 orders<br>
                Source · Olist Brazil<br>
                Stack · Kafka + Spark<br>
                Period · 2016–2018
            </div>
        """, unsafe_allow_html=True)

    return date_start, date_end


def header():
    st.markdown(f"""
        <div style='margin-bottom:4px;'>
            <span style='font-size:20px;font-weight:700;color:{C["dark"]};'>
                Olist Analytics
            </span>
            <span style='font-size:12px;color:{C["gray"]};margin-left:10px;'>
                Real-Time E-Commerce Intelligence Dashboard
            </span>
        </div>
        <hr class='divider'>
    """, unsafe_allow_html=True)


def tab_delivery(date_start, date_end):
    df = load("delivery_kpis")
    if df is None:
        st.info("No delivery data. Run the pipeline first.")
        return

    df = apply_date_filter(df, "order_purchase_timestamp", date_start, date_end)
    if len(df) == 0:
        st.warning("No data in selected date range.")
        return

    total = len(df)
    late = int(df["is_late"].sum()) if "is_late" in df.columns else 0
    on_time = total - late
    late_pct = round(late / total * 100, 1) if total else 0
    avg_days = round(df["delivery_days"].mean(), 1) if "delivery_days" in df.columns else 0
    late_df = df[df["delay_days"] > 0] if "delay_days" in df.columns else pd.DataFrame()
    avg_delay = round(late_df["delay_days"].mean(), 1) if len(late_df) else 0

    late_trend = compute_trend(
        df, "order_purchase_timestamp",
        lambda d: (d["is_late"].sum() / len(d) * 100) if len(d) > 0 else 0
    )

    if late_pct > 40:
        insight(
            f"Late delivery rate is {late_pct}% — significantly above industry benchmark of 10-15%. "
            f"Immediate logistics review recommended.",
            "danger"
        )
    elif late_pct > 20:
        insight(
            f"Late delivery rate is {late_pct}% — above acceptable threshold. "
            f"Focus on top states showing highest delay rates.",
            "warning"
        )
    else:
        insight(
            f"Delivery performance is on track at {late_pct}% late rate.",
            "success"
        )

    sec("OVERVIEW")
    c = st.columns(5)
    with c[0]: kpi("Total Orders", f"{total:,}")
    with c[1]: kpi("On Time", f"{on_time:,}", f"{100-late_pct}%", C["green"])
    with c[2]: kpi("Late", f"{late:,}", f"{late_pct}%", C["red"],
                   trend=late_trend)
    with c[3]: kpi("Avg Delivery", f"{avg_days}d")
    with c[4]: kpi("Avg Delay", f"{avg_delay}d", "late orders", C["orange"])

    st.markdown("<br>", unsafe_allow_html=True)

    if "customer_state" in df.columns:
        states = ["All"] + sorted(df["customer_state"].dropna().unique().tolist())
        selected_state = st.selectbox(
            "Filter by State", states, key="del_state"
        )
        if selected_state != "All":
            df = df[df["customer_state"] == selected_state]

    c1, c2 = st.columns(2)

    with c1:
        sec("DELAY DISTRIBUTION")
        if "delay_days" in df.columns:
            fig = go.Figure(go.Histogram(
                x=df["delay_days"], nbinsx=40,
                marker_color=C["blue"], marker_line_width=0,
                hovertemplate="Delay: %{x:.1f} days<br>Orders: %{y}<extra></extra>"
            ))
            fig.add_vline(x=0, line_dash="dot", line_color=C["red"],
                          line_width=1.5,
                          annotation_text="On-Time Threshold",
                          annotation_font_size=10,
                          annotation_font_color=C["red"])
            late_avg = df[df["delay_days"] > 0]["delay_days"].mean() if len(df[df["delay_days"] > 0]) else 0
            if late_avg > 0:
                fig.add_vline(x=late_avg, line_dash="dash",
                              line_color=C["orange"], line_width=1.5,
                              annotation_text=f"Avg Delay: {late_avg:.1f}d",
                              annotation_font_size=10,
                              annotation_font_color=C["orange"])
            L(fig)
            fig.update_layout(xaxis_title="Days", yaxis_title="Orders")
            st.plotly_chart(fig, use_container_width=True)

    with c2:
        sec("ON-TIME VS LATE")
        fig = go.Figure(go.Pie(
            labels=["On Time", "Late"],
            values=[on_time, late],
            hole=0.68,
            marker_colors=[C["green"], C["red"]],
            textinfo="percent+label",
            textfont=dict(size=11), showlegend=False,
            hovertemplate="%{label}: %{value:,} orders (%{percent})<extra></extra>"
        ))
        fig.add_annotation(
            text=f"<b>{100-late_pct}%</b><br><span style='font-size:10px'>On Time</span>",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=18, color=C["dark"], family="Inter")
        )
        L(fig)
        st.plotly_chart(fig, use_container_width=True)

    if "customer_state" in df.columns:
        sec("PERFORMANCE BY STATE")
        s = df.groupby("customer_state").agg(
            orders=("order_id", "count"),
            late=("is_late", "sum"),
            avg_d=("delivery_days", "mean")
        ).reset_index()
        s["late_pct"] = (s["late"] / s["orders"] * 100).round(1)
        s["avg_d"] = s["avg_d"].round(1)
        top = s.nlargest(12, "orders")

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=top["customer_state"], y=top["orders"],
            name="Orders", marker_color=C["blue"],
            marker_line_width=0,
            hovertemplate="State: %{x}<br>Orders: %{y:,}<extra></extra>"
        ))
        fig.add_trace(go.Scatter(
            x=top["customer_state"], y=top["late_pct"],
            name="Late %", yaxis="y2", mode="lines+markers",
            line=dict(color=C["red"], width=1.5),
            marker=dict(size=5),
            hovertemplate="State: %{x}<br>Late: %{y:.1f}%<extra></extra>"
        ))
        L(fig)
        fig.update_layout(
            yaxis2=dict(overlaying="y", side="right",
                        showgrid=False, range=[0, 100],
                        tickfont=dict(size=10), title="Late %"),
            legend=dict(orientation="h", y=1.08, font=dict(size=10)),
            bargap=0.3
        )
        st.plotly_chart(fig, use_container_width=True)

        worst = top.nlargest(1, "late_pct")
        if len(worst):
            w = worst.iloc[0]
            insight(
                f"State <strong>{w['customer_state']}</strong> has the highest late rate at "
                f"<strong>{w['late_pct']}%</strong> across {w['orders']:,} orders. "
                f"Recommend reviewing logistics partners in this region.",
                "warning"
            )


def tab_reviews(date_start, date_end):
    df = load("review_kpis")
    if df is None:
        st.info("No review data. Run the pipeline first.")
        return

    df = apply_date_filter(df, "review_creation_date", date_start, date_end)
    if len(df) == 0:
        st.warning("No data in selected date range.")
        return
    total = len(df)
    bad = int(df["is_bad_review"].sum()) if "is_bad_review" in df.columns else 0
    dlv = int(df["delivery_complaint_in_bad_review"].sum()) if "delivery_complaint_in_bad_review" in df.columns else 0
    avg = round(df["review_score"].mean(), 2) if "review_score" in df.columns else 0
    bad_pct = round(bad / total * 100, 1) if total else 0
    dlv_pct = round(dlv / bad * 100, 1) if bad else 0

    if dlv_pct >= 60:
        insight(
            f"<strong>{dlv_pct}%</strong> of bad reviews mention delivery issues — "
            f"not product quality. Fixing logistics could eliminate most negative reviews. "
            f"Reference benchmark from EDA: 67%.",
            "danger"
        )
    elif dlv_pct >= 40:
        insight(
            f"<strong>{dlv_pct}%</strong> of bad reviews are delivery-related. "
            f"Logistics improvement would have significant impact on customer satisfaction.",
            "warning"
        )
    else:
        insight(
            f"Delivery complaints account for {dlv_pct}% of bad reviews. "
            f"Product quality may also be a factor worth investigating.",
            "info"
        )

    sec("OVERVIEW")
    c = st.columns(4)
    with c[0]: kpi("Reviews", f"{total:,}")
    with c[1]: kpi("Avg Score", f"{avg} / 5",
                   color=C["green"] if avg >= 4 else C["orange"] if avg >= 3 else C["red"])
    with c[2]: kpi("Bad Reviews", f"{bad_pct}%", f"{bad:,} reviews", C["red"])
    with c[3]: kpi("Delivery in Bad", f"{dlv_pct}%", "of bad reviews", C["orange"])

    st.markdown("<br>", unsafe_allow_html=True)

    c1, c2 = st.columns([1.3, 1])

    with c1:
        sec("DELIVERY COMPLAINTS IN BAD REVIEWS — KEY METRIC")
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=dlv_pct,
            number=dict(
                suffix="%",
                font=dict(size=48, color=C["dark"], family="Inter"),
                valueformat=".1f"
            ),
            gauge=dict(
                axis=dict(range=[0, 100], tickwidth=1,
                          tickcolor="#E2E8F0",
                          tickfont=dict(size=9, color=C["gray"]),
                          nticks=6),
                bar=dict(color=C["blue"], thickness=0.28),
                bgcolor="white", borderwidth=0,
                steps=[
                    dict(range=[0, 40], color="#F0FDF4"),
                    dict(range=[40, 60], color="#FFFBEB"),
                    dict(range=[60, 100], color="#FFF1F2"),
                ],
                threshold=dict(
                    line=dict(color=C["red"], width=2),
                    thickness=0.75, value=67
                )
            )
        ))
        fig.add_annotation(
            text="▼ EDA benchmark: 67%",
            x=0.5, y=0.08, showarrow=False,
            font=dict(size=11, color=C["red"], family="Inter")
        )
        fig.add_annotation(
            text="of bad reviews are delivery-related",
            x=0.5, y=-0.05, showarrow=False,
            font=dict(size=10, color=C["gray"], family="Inter")
        )
        fig.update_layout(
            paper_bgcolor="white", height=290,
            margin=dict(t=16, b=24, l=24, r=24)
        )
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        sec("SCORE DISTRIBUTION")
        if "review_score" in df.columns:
            sc = df["review_score"].value_counts().reset_index()
            sc.columns = ["score", "count"]
            sc = sc.sort_values("score")
            cmap = {1: C["red"], 2: C["orange"],
                    3: C["yellow"], 4: "#34D399", 5: C["green"]}
            fig = go.Figure(go.Bar(
                x=sc["score"].astype(str), y=sc["count"],
                marker_color=[cmap.get(s, C["blue"]) for s in sc["score"]],
                marker_line_width=0,
                text=sc["count"].apply(lambda x: f"{x:,}"),
                textposition="outside",
                textfont=dict(size=10, color=C["gray"]),
                hovertemplate="Score: %{x}<br>Reviews: %{y:,}<extra></extra>"
            ))
            L(fig)
            fig.update_layout(
                showlegend=False, bargap=0.3,
                xaxis_title="Score", yaxis_title="Reviews"
            )
            st.plotly_chart(fig, use_container_width=True)

    if "sentiment" in df.columns:
        sec("SENTIMENT BREAKDOWN")
        c1, c2 = st.columns(2)

        with c1:
            s = df["sentiment"].value_counts().reset_index()
            s.columns = ["sentiment", "count"]
            cmap = {"positive": C["green"],
                    "negative": C["red"], "neutral": "#94A3B8"}
            fig = go.Figure(go.Bar(
                x=s["sentiment"].str.capitalize(), y=s["count"],
                marker_color=[cmap.get(x, C["blue"]) for x in s["sentiment"]],
                marker_line_width=0,
                text=s["count"].apply(lambda x: f"{x:,}"),
                textposition="outside",
                textfont=dict(size=10),
                hovertemplate="%{x}: %{y:,} reviews<extra></extra>"
            ))
            L(fig)
            fig.update_layout(showlegend=False, bargap=0.4,
                              yaxis_title="Reviews")
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            sec("TOP COMPLAINT KEYWORDS")
            delivery_keywords = {
                "atraso/delay": int(df["is_delivery_complaint"].sum()) if "is_delivery_complaint" in df.columns else 0,
                "bad review": bad,
                "low score (1-2★)": int((df["review_score"] <= 2).sum()) if "review_score" in df.columns else 0,
                "score 3★": int((df["review_score"] == 3).sum()) if "review_score" in df.columns else 0,
                "no comment": int(df["review_score"].notna().sum()) - int(df.get("review_comment_message", pd.Series()).notna().sum()) if "review_score" in df.columns else 0,
            }
            kw_df = pd.DataFrame(
                list(delivery_keywords.items()),
                columns=["keyword", "count"]
            ).sort_values("count", ascending=True)

            fig = go.Figure(go.Bar(
                x=kw_df["count"],
                y=kw_df["keyword"],
                orientation="h",
                marker_color=C["blue"],
                marker_line_width=0,
                text=kw_df["count"].apply(lambda x: f"{x:,}"),
                textposition="outside",
                textfont=dict(size=10),
                hovertemplate="%{y}: %{x:,}<extra></extra>"
            ))
            L(fig)
            fig.update_layout(showlegend=False, height=240,
                              xaxis_title="Count", yaxis_title="")
            st.plotly_chart(fig, use_container_width=True)


def tab_payments(date_start, date_end):
    df = load("payment_kpis")
    if df is None:
        st.info("No payment data. Run the pipeline first.")
        return

    total = len(df)
    rev = round(df["payment_value"].sum(), 0) if "payment_value" in df.columns else 0
    avg = round(df["payment_value"].mean(), 0) if "payment_value" in df.columns else 0
    inst = round(df["used_installments"].sum() / total * 100, 1) if "used_installments" in df.columns else 0

    if inst >= 45:
        insight(
            f"<strong>{inst}%</strong> of orders use installment plans — "
            f"matches EDA benchmark of ~50%. Payment flexibility is a key growth driver in Brazil.",
            "success"
        )
    else:
        insight(
            f"Installment usage at {inst}% — below the expected 50% benchmark. "
            f"Consider promoting installment options more prominently.",
            "warning"
        )
        sec("OVERVIEW")
    c = st.columns(4)
    with c[0]: kpi("Transactions", f"{total:,}")
    with c[1]: kpi("Total Revenue", f"R$ {rev:,.0f}")
    with c[2]: kpi("Avg Payment", f"R$ {avg:,.0f}")
    with c[3]: kpi("Installment Rate", f"{inst}%", "target ~50%",
                   C["green"] if 40 <= inst <= 60 else C["orange"])

    st.markdown("<br>", unsafe_allow_html=True)

    if "payment_type" in df.columns:
        p = df.groupby("payment_type").agg(
            orders=("order_id", "count"),
            avg_val=("payment_value", "mean"),
            total_rev=("payment_value", "sum")
        ).reset_index().round(0)
        p["label"] = p["payment_type"].str.replace("_", " ").str.title()
        p["color"] = [C["blue"], C["teal"], C["purple"], C["orange"]][:len(p)]

        c1, c2 = st.columns(2)

        with c1:
            sec("ORDERS BY PAYMENT METHOD")
            fig = go.Figure(go.Pie(
                labels=p["label"], values=p["orders"],
                hole=0.62,
                marker_colors=p["color"].tolist(),
                textinfo="percent+label",
                textfont=dict(size=10), showlegend=False,
                hovertemplate="%{label}<br>Orders: %{value:,}<br>Share: %{percent}<extra></extra>"
            ))
            L(fig)
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            sec("AVG PAYMENT VALUE BY METHOD")
            fig = go.Figure(go.Bar(
                x=p["label"], y=p["avg_val"],
                marker_color=p["color"].tolist(),
                marker_line_width=0,
                text=p["avg_val"].apply(lambda x: f"R$ {x:,.0f}"),
                textposition="outside",
                textfont=dict(size=10),
                hovertemplate="%{x}<br>Avg: R$ %{y:,.0f}<extra></extra>"
            ))
            L(fig)
            fig.update_layout(
                showlegend=False, bargap=0.35,
                yaxis_title="R$ (BRL)"
            )
            st.plotly_chart(fig, use_container_width=True)

        sec("REVENUE BY METHOD")
        fig = go.Figure(go.Bar(
            x=p["label"], y=p["total_rev"],
            marker_color=p["color"].tolist(),
            marker_line_width=0,
            text=p["total_rev"].apply(lambda x: f"R$ {x:,.0f}"),
            textposition="outside",
            textfont=dict(size=10),
            hovertemplate="%{x}<br>Revenue: R$ %{y:,.0f}<extra></extra>"
        ))
        L(fig)
        fig.update_layout(showlegend=False, bargap=0.35,
                          yaxis_title="Total Revenue (R$)")
        st.plotly_chart(fig, use_container_width=True)

        top_method = p.nlargest(1, "avg_val").iloc[0]
        insight(
            f"<strong>{top_method['label']}</strong> customers have the highest average payment "
            f"of R$ {top_method['avg_val']:,.0f} — "
            f"prioritize this segment for premium offerings and loyalty programs.",
            "info"
        )


def tab_categories(date_start, date_end):
    df = load("category_kpis")
    if df is None:
        st.info("No category data. Run the pipeline first.")
        return

    if "product_category_name_english" in df.columns:
        df = df.dropna(subset=["product_category_name_english"])

    total_cats = df["product_category_name_english"].nunique() if "product_category_name_english" in df.columns else 0
    rev = round(df["item_total_value"].sum(), 0) if "item_total_value" in df.columns else 0
    avg_p = round(df["price"].mean(), 0) if "price" in df.columns else 0
    avg_freight = round(df["freight_value"].mean(), 0) if "freight_value" in df.columns else 0
    freight_ratio = round(avg_freight / (avg_p + avg_freight) * 100, 1) if (avg_p + avg_freight) > 0 else 0

    if freight_ratio > 30:
        insight(
            f"Freight costs represent <strong>{freight_ratio}%</strong> of total order value — "
            f"above the sustainable threshold of 20-25%. "
            f"Consider warehouse location optimization or carrier renegotiation.",
            "danger"
        )
    elif freight_ratio > 20:
        insight(
            f"Freight ratio is {freight_ratio}% — worth monitoring. "
            f"Categories with high freight-to-price ratios may benefit from free shipping promotions.",
            "warning"
        )

    sec("OVERVIEW")
    c = st.columns(4)
    with c[0]: kpi("Categories", total_cats)
    with c[1]: kpi("Total Revenue", f"R$ {rev:,.0f}")
    with c[2]: kpi("Avg Price", f"R$ {avg_p:,.0f}")
    with c[3]: kpi("Freight Ratio", f"{freight_ratio}%",
                   "freight / order value",
                   C["red"] if freight_ratio > 30 else C["orange"] if freight_ratio > 20 else C["green"])

    st.markdown("<br>", unsafe_allow_html=True)

    if "product_category_name_english" not in df.columns:
        st.warning("Category column not found.")
        return

    cat = df.groupby("product_category_name_english").agg(
        orders=("order_id", "count"),
        revenue=("item_total_value", "sum"),
        avg_price=("price", "mean"),
        avg_freight=("freight_value", "mean")
    ).reset_index().round(1)

    cat["freight_ratio"] = (
        cat["avg_freight"] / (cat["avg_price"] + cat["avg_freight"]) * 100
    ).round(1)

    top = cat.nlargest(10, "orders").sort_values("orders")

    c1, c2 = st.columns(2)

    with c1:
        sec("TOP 10 CATEGORIES BY ORDERS")
        fig = go.Figure(go.Bar(
            x=top["orders"],
            y=top["product_category_name_english"].str.replace("_", " ").str.title(),
            orientation="h",
            marker=dict(
                color=top["orders"],
                colorscale=C["scale"], line_width=0
            ),
            text=top["orders"].apply(lambda x: f"{x:,}"),
            textposition="outside",
            textfont=dict(size=9, color=C["gray"]),
            hovertemplate="%{y}<br>Orders: %{x:,}<extra></extra>"
        ))
        L(fig)
        fig.update_layout(height=360, showlegend=False,
                          xaxis_title="Orders", yaxis_title="")
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        sec("FREIGHT RATIO BY CATEGORY")
        top_freight = cat.nlargest(10, "orders").sort_values("freight_ratio")
        colors = [C["green"] if r < 20 else C["orange"] if r < 30 else C["red"]
                  for r in top_freight["freight_ratio"]]
        fig = go.Figure(go.Bar(
            x=top_freight["freight_ratio"],
            y=top_freight["product_category_name_english"].str.replace("_", " ").str.title(),
            orientation="h",
            marker_color=colors, marker_line_width=0,
            text=top_freight["freight_ratio"].apply(lambda x: f"{x:.1f}%"),
            textposition="outside",
            textfont=dict(size=9, color=C["gray"]),
            hovertemplate="%{y}<br>Freight Ratio: %{x:.1f}%<extra></extra>"
        ))
        L(fig)
        fig.add_vline(x=20, line_dash="dot", line_color=C["orange"],
                      line_width=1.5,
                      annotation_text="20% threshold",
                      annotation_font_size=9)
        fig.update_layout(height=360, showlegend=False,
                          xaxis_title="Freight %", yaxis_title="")
        st.plotly_chart(fig, use_container_width=True)

    sec("PRICE VS DEMAND — BUBBLE SIZE = REVENUE")
    fig = px.scatter(
        cat, x="avg_price", y="orders",
        size="revenue",
        hover_name="product_category_name_english",
        color="freight_ratio",
        color_continuous_scale=[[0, "#F0FDF4"], [0.5, "#FEF3C7"], [1, "#FFF1F2"]],
        size_max=35,
        labels={
            "avg_price": "Avg Price (R$)",
            "orders": "Total Orders",
            "freight_ratio": "Freight Ratio %"
        }
    )
    L(fig)
    fig.update_traces(
        hovertemplate="<b>%{hovertext}</b><br>Avg Price: R$ %{x:.0f}<br>Orders: %{y:,}<extra></extra>"
    )
    st.plotly_chart(fig, use_container_width=True)
    high_freight = cat[cat["freight_ratio"] > 30].nlargest(3, "orders")
    if len(high_freight):
        cats_list = ", ".join(high_freight["product_category_name_english"].str.replace("_", " ").str.title().tolist())
        insight(
            f"Categories with freight ratio above 30%: <strong>{cats_list}</strong>. "
            f"These categories are losing margin to shipping costs. "
            f"Consider free shipping thresholds or bundling strategies.",
            "warning"
        )


def tab_quality():
    sec("DATA QUALITY SCORES")
    qfiles = sorted(glob.glob(os.path.join(LOGS_DIR, "quality_*.json")))

    if not qfiles:
        st.info("Run: python quality/metrics.py")
        return

    cols = st.columns(min(len(qfiles), 7))
    for i, qf in enumerate(qfiles):
        with open(qf) as f:
            r = json.load(f)
        score = r.get("overall_score", 0)
        c = C["green"] if score >= 90 else C["orange"] if score >= 70 else C["red"]
        with cols[i % 7]:
            kpi(r["table_name"].upper(), str(score),
                f"{r['row_count']:,} rows", c)

    st.markdown("<br>", unsafe_allow_html=True)
    sec("CONTRACT VIOLATIONS")

    log = os.path.join(LOGS_DIR, "contract_violations.log")
    if not os.path.exists(log):
        st.success("No violations logged.")
        return

    with open(log) as f:
        lines = f.readlines()

    found = False
    for line in lines[-20:]:
        try:
            e = json.loads(line)
            if not e.get("is_valid"):
                found = True
                for v in e.get("violations", []):
                    st.markdown(f"""
                        <div style='background:#FFF1F2;border-left:3px solid {C["red"]};
                                    padding:9px 13px;border-radius:6px;margin-bottom:5px;'>
                            <span style='font-size:10px;font-weight:600;color:{C["red"]};'>
                                {e["table_name"].upper()}
                            </span>
                            <span style='font-size:11px;color:#334155;margin-left:8px;'>
                                {v["details"]}
                            </span>
                        </div>
                    """, unsafe_allow_html=True)
        except Exception:
            pass

    if not found:
        st.success("All contracts passing.")


def main():
    date_start, date_end = sidebar_filters()
    header()

    t1, t2, t3, t4, t5 = st.tabs([
        "Delivery", "Reviews", "Payments", "Categories", "Quality"
    ])
    with t1: tab_delivery(date_start, date_end)
    with t2: tab_reviews(date_start, date_end)
    with t3: tab_payments(date_start, date_end)
    with t4: tab_categories(date_start, date_end)
    with t5: tab_quality()


main()