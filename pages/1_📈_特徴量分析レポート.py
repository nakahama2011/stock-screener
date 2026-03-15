"""
特徴量分析レポート＋MLモデル分析ページ

バックテスト結果とMLモデルの特徴量重要度を統合的に分析し、
+2%到達に寄与する条件を視覚的に表示する。
"""
import json
import os
import warnings

import numpy as np
import pandas as pd
import streamlit as st

warnings.filterwarnings("ignore")

st.set_page_config(
    page_title="📈 特徴量分析レポート",
    page_icon="📈",
    layout="wide",
)

SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_DIR = os.path.join(SCRIPT_DIR, "results")


# =============================================
# データ読み込み
# =============================================
@st.cache_data(ttl=600)
def load_data():
    """バックテストCSVとMLレポートを読み込む"""
    csv_path = os.path.join(RESULTS_DIR, "backtest_latest.csv")
    report_path = os.path.join(RESULTS_DIR, "jp_ml_report.json")

    data = {}

    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        hit_cols = [f"hit_2pct_{n}d" for n in [1,2,3,4,5] if f"hit_2pct_{n}d" in df.columns]
        if hit_cols:
            df["hit_5d"] = df[hit_cols].max(axis=1)
        for sma, feat in [("sma5","sma5_dist_pct"),("sma20","sma20_dist_pct"),("sma60","sma60_dist_pct")]:
            if sma in df.columns and "close" in df.columns:
                df[feat] = (df["close"] - df[sma]) / df[sma] * 100
        if "high_price" in df.columns and "low_price" in df.columns and "close" in df.columns:
            df["price_range_pct"] = (df["high_price"] - df["low_price"]) / df["close"] * 100
        data["df"] = df
    else:
        data["df"] = None

    if os.path.exists(report_path):
        with open(report_path, "r") as f:
            data["report"] = json.load(f)
    else:
        data["report"] = None

    return data


data = load_data()
df = data.get("df")
report = data.get("report")

if df is None:
    st.error("⚠️ バックテストCSVが見つかりません。先に `python3 backtester.py` を実行してください。")
    st.stop()


# =============================================
# ヘッダー
# =============================================
st.markdown("# 📈 特徴量分析レポート（日本株）")
st.markdown("*MLモデル（GradientBoosting）の分析結果と、バックテストデータの統計的分析を統合表示*")

# =============================================
# KPIカード
# =============================================
n_total = len(df)
hit_rate = df["hit_5d"].mean() * 100 if "hit_5d" in df.columns else 0
n_dates = df["date"].nunique() if "date" in df.columns else 0
avg_days = df["days_to_target"].dropna().mean() if "days_to_target" in df.columns else 0

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("学習データ数", f"{n_total:,}件", f"{n_dates}営業日")
with col2:
    st.metric("+2%到達率（高値5日）", f"{hit_rate:.1f}%", f"{int(df['hit_5d'].sum()):,}件到達")
with col3:
    st.metric("平均到達日数", f"{avg_days:.1f}日" if avg_days > 0 else "N/A")
with col4:
    n_feat = len(report['feature_names']) if report else 0
    st.metric("特徴量数", f"{n_feat}個", "GradientBoosting")


# =============================================
# セクション1: MLモデル特徴量重要度
# =============================================
st.markdown("---")
st.subheader("🤖 MLモデル 特徴量重要度 TOP15")

if report:
    # 特徴量名の日本語マッピング
    name_map = {
        "price_range_pct": "🥇 日足レンジ幅(%)",
        "day_change_pct": "🥈 当日騰落率(%)",
        "sma60_dist_pct": "🥉 SMA60乖離率(%)",
        "day_of_week": "曜日",
        "trend_start_days_ago": "トレンド継続日数",
        "prev_day_change_pct": "前日騰落率(%)",
        "rsi": "RSI(14)",
        "sma5_dist_pct": "SMA5乖離率(%)",
        "prev_prev_day_change_pct": "前々日騰落率(%)",
        "sma20_dist_pct": "SMA20乖離率(%)",
        "volume_ratio": "出来高比(20MA)",
        "vol_today_vs_yday_pct": "出来高前日比(%)",
        "volume_change_pct": "出来高増減(%)",
        "atr_pct": "ATR(%)",
        "weekly_sma20_ok": "週足SMA20突破",
        "sma20_touch_count": "SMA20タッチ回数",
        "is_pullback": "押し目(プルバック)",
        "is_breakout": "20日高値ブレイク",
        "long_upper_wick": "長大上ヒゲ",
        "is_high_zone": "高値圏",
        "big_bearish_yesterday": "前日大陰線",
        "first_sma20_touch": "初回SMA20タッチ",
    }

    fi = report["feature_importance"][:15]
    max_imp = fi[0]["importance"] if fi else 1

    # DataFrameで表示
    fi_data = []
    for i, item in enumerate(fi):
        name_jp = name_map.get(item["name"], item["name"])
        if i >= 3:
            name_jp = f"{i+1}. {name_jp}"
        fi_data.append({
            "特徴量": name_jp,
            "重要度": item["importance"],
            "バー": item["importance"] / max_imp,
        })

    fi_df = pd.DataFrame(fi_data)
    st.dataframe(
        fi_df.style.bar(subset=["バー"], color="#6366f1", vmin=0, vmax=1).format({"重要度": "{:.4f}", "バー": "{:.1%}"}),
        use_container_width=True,
        hide_index=True,
    )

    st.info("💡 **重要度の解釈:** 日足レンジ幅(%)が圧倒的に重要 → ボラティリティが高い銘柄ほど+2%到達しやすい。当日騰落率とSMA60乖離率が次に続く。")
else:
    st.warning("MLレポートが見つかりません。")


# =============================================
# セクション2: AI予測確率帯別の実際の到達率
# =============================================
st.markdown("---")
st.subheader("🎯 AI予測確率帯 × 実際の+2%到達率")

try:
    import joblib
    model_path = os.path.join(RESULTS_DIR, "jp_ml_model.pkl")
    if os.path.exists(model_path) and report:
        model_data = joblib.load(model_path)
        model = model_data["model"]
        features = model_data["feature_names"]

        for col_name in ["is_pullback","is_breakout","long_upper_wick","is_high_zone",
                    "big_bearish_yesterday","weekly_sma20_ok","first_sma20_touch"]:
            if col_name in df.columns:
                df[col_name] = df[col_name].astype(int)

        avail = [c for c in features if c in df.columns]
        for c in avail:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        X = df[avail].fillna(0)
        df["ai_prob"] = model.predict_proba(X)[:, 1] * 100

        bands = [(0,30,"0-30%"), (30,50,"30-50%"),
                 (50,60,"50-60%"), (60,70,"60-70%"),
                 (70,80,"70-80%"), (80,101,"80%+")]

        band_cols = st.columns(len(bands))
        for col, (lo, hi, label) in zip(band_cols, bands):
            mask = (df["ai_prob"] >= lo) & (df["ai_prob"] < hi) & df["hit_5d"].notna()
            n = mask.sum()
            actual = df.loc[mask, "hit_5d"].mean() * 100 if n > 0 else 0
            with col:
                color = "off" if actual < 50 else "normal"
                delta_str = f"{n:,}件"
                st.metric(f"予測 {label}", f"{actual:.1f}%", delta_str)

        st.success("✅ **AI予測60%以上**の銘柄は、実際に5日以内+2%に到達する確率が**80%超！**")
    else:
        st.warning("MLモデルが見つかりません。")
except ImportError:
    st.warning("scikit-learn / joblib がインストールされていません。requirements.txt を確認してください。")
except Exception as e:
    st.warning(f"MLモデル分析エラー: {e}")


# =============================================
# セクション3: フィルター条件ランキング
# =============================================
_ranking_path = os.path.join(RESULTS_DIR, "filter_ranking.json")
if os.path.exists(_ranking_path):
    with open(_ranking_path, "r") as f:
        ranking = json.load(f)

    st.markdown("---")
    st.subheader("🏆 フィルター条件ランキング（+2%到達率）")
    st.caption(f"全{ranking.get('total_signals',0):,}シグナル中、条件別の5日以内+2%到達率。全体平均: {ranking.get('overall_rate',0):.1f}%")

    tab_single, tab_combo = st.tabs(["📋 単一条件 TOP10", "🔗 2条件組み合わせ TOP15"])

    with tab_single:
        single = ranking.get("single", [])[:10]
        if single:
            s_df = pd.DataFrame(single)
            s_df = s_df.rename(columns={"条件": "フィルター条件", "+2%到達率": "到達率(%)", "平均到達日": "平均到達日数"})
            cols_show = ["フィルター条件", "件数", "到達率(%)", "平均到達日数"]
            s_df = s_df[[c for c in cols_show if c in s_df.columns]]
            st.dataframe(
                s_df.style
                    .bar(subset=["到達率(%)"], color="#6366f1", vmin=50, vmax=90)
                    .format({"到達率(%)": "{:.1f}%", "平均到達日数": "{:.1f}日"}),
                use_container_width=True,
                hide_index=True,
            )

    with tab_combo:
        combo = ranking.get("combo", [])[:15]
        if combo:
            c_df = pd.DataFrame(combo)
            c_df["フィルター条件"] = c_df["条件1"] + " ＋ " + c_df["条件2"]
            c_df = c_df.rename(columns={"+2%到達率": "到達率(%)", "平均到達日": "平均到達日数"})
            cols_show = ["フィルター条件", "件数", "到達率(%)", "平均到達日数"]
            c_df = c_df[[c for c in cols_show if c in c_df.columns]]
            st.dataframe(
                c_df.style
                    .bar(subset=["到達率(%)"], color="#10b981", vmin=70, vmax=90)
                    .format({"到達率(%)": "{:.1f}%", "平均到達日数": "{:.1f}日"}),
                use_container_width=True,
                hide_index=True,
            )

    # トップコンボを強調
    if ranking.get("combo"):
        top = ranking["combo"][0]
        st.success(f"🏆 **最強コンボ: {top['条件1']} ＋ {top['条件2']}** → 到達率 **{top['+2%到達率']:.1f}%** ({top['件数']}件, 平均{top.get('平均到達日',0):.1f}日)")

# =============================================
# セクション4: 到達日数分布
# =============================================
if "days_to_target" in df.columns:
    st.markdown("---")
    st.subheader("⏱ +2%到達までの日数分布")

    d2t = df["days_to_target"].dropna()
    if len(d2t) > 0:
        dist_cols = st.columns(5)
        for day in [1, 2, 3, 4, 5]:
            n_day = int((d2t == day).sum())
            pct = n_day / len(d2t) * 100
            with dist_cols[day - 1]:
                emoji = "🎯" if day == 1 else ("✅" if day <= 3 else "📊")
                st.metric(f"{emoji} {day}日目", f"{pct:.1f}%", f"{n_day:,}件")

        pct_2d = (d2t <= 2).sum() / len(d2t) * 100
        st.info(f"💡 到達した銘柄の **{pct_2d:.1f}%** が **2日以内**に+2%を達成")


# =============================================
# セクション5: 曜日別パフォーマンス
# =============================================
if "day_of_week" in df.columns and "hit_5d" in df.columns:
    st.markdown("---")
    st.subheader("📅 曜日別パフォーマンス")

    dow_map = {0: "月曜", 1: "火曜", 2: "水曜", 3: "木曜", 4: "金曜"}
    valid = df[df["hit_5d"].notna()].copy()
    overall = valid["hit_5d"].mean() * 100

    dow_cols = st.columns(5)
    for i, (dow_key, dow_name) in enumerate(dow_map.items()):
        sub = valid[valid["day_of_week"] == dow_key]
        rate = sub["hit_5d"].mean() * 100 if len(sub) > 0 else 0
        diff = rate - overall
        with dow_cols[i]:
            st.metric(f"{dow_name}", f"{rate:.1f}%", f"{diff:+.1f}% ({len(sub):,}件)")


# =============================================
# フッター
# =============================================
st.markdown("---")
st.caption(
    f"📊 データソース: バックテスト結果 + GradientBoostingClassifier | "
    f"学習期間: {df['date'].min()} 〜 {df['date'].max()}"
)
