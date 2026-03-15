"""
🇺🇸 米国株 特徴量分析レポート＋MLモデル分析ページ

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
    page_title="🇺🇸 米国株 特徴量分析レポート",
    page_icon="🇺🇸",
    layout="wide",
)

SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_DIR = os.path.join(SCRIPT_DIR, "us_results")


# =============================================
# データ読み込み
# =============================================
@st.cache_data(ttl=600)
def load_data():
    """バックテストCSVとMLレポートを読み込む"""
    csv_path = os.path.join(RESULTS_DIR, "us_backtest_latest.csv")
    report_path = os.path.join(RESULTS_DIR, "us_ml_report.json")

    data = {}

    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        # +2%到達ラベル
        hit_cols = [f"hit_2pct_{n}d" for n in [1,2,3,4,5] if f"hit_2pct_{n}d" in df.columns]
        if hit_cols:
            df["hit_5d"] = df[hit_cols].max(axis=1)
        # SMA乖離率を計算
        for sma, feat in [("sma5","sma5_dist_pct"),("sma20","sma20_dist_pct"),("sma60","sma60_dist_pct")]:
            if sma in df.columns and "close" in df.columns:
                df[feat] = (df["close"] - df[sma]) / df[sma] * 100
        # 日足レンジ幅
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
    st.error("⚠️ バックテストCSVが見つかりません。先に `python3 us_backtester.py` を実行してください。")
    st.stop()


# =============================================
# ヘッダー
# =============================================
st.markdown("# 🇺🇸 特徴量分析レポート（米国株）")
st.markdown("*MLモデル（GradientBoosting）の分析結果と、バックテストデータの統計的分析を統合表示 | 運用戦略: +2%固定指値*")

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
    hit_count = int(df['hit_5d'].sum()) if 'hit_5d' in df.columns else 0
    st.metric("+2%到達率（高値5日）", f"{hit_rate:.1f}%", f"{hit_count:,}件到達")
with col3:
    if "days_to_target" in df.columns:
        d2t = df["days_to_target"].dropna()
        st.metric("平均到達日数", f"{avg_days:.1f}日", f"{len(d2t):,}件中")
    else:
        st.metric("平均到達日数", "N/A")
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

    st.info("💡 **重要度の解釈:** 日足レンジ幅(%)が高いほどボラティリティが高く+2%到達しやすい。当日騰落率とSMA60乖離率が次に続く。")
else:
    st.warning("MLレポートが見つかりません。")


# =============================================
# セクション2: AI予測確率帯別の実際の到達率
# =============================================
st.markdown("---")
st.subheader("🎯 AI予測確率帯 × 実際の+2%到達率")

try:
    import joblib
    model_path = os.path.join(RESULTS_DIR, "us_ml_model.pkl")
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
                delta_str = f"{n:,}件"
                st.metric(f"予測 {label}", f"{actual:.1f}%", delta_str)

        st.success("✅ **AI予測60%以上**の銘柄は、実際に5日以内+2%に到達する確率が高い傾向にあります。")
    else:
        st.warning("MLモデルが見つかりません。")
except ImportError:
    st.warning("scikit-learn / joblib がインストールされていません。requirements.txt を確認してください。")
except Exception as e:
    st.warning(f"MLモデル分析エラー: {e}")


# =============================================
# セクション3: フィルター条件ランキング
# =============================================
_ranking_path = os.path.join(RESULTS_DIR, "us_top_combos.json")
if os.path.exists(_ranking_path):
    with open(_ranking_path, "r") as f:
        ranking = json.load(f)

    st.markdown("---")
    st.subheader("🏆 フィルター条件コンボランキング（+2%到達率）")

    # us_top_combos.jsonの形式に合わせる
    combos = ranking.get("combos_5d", ranking.get("combo", []))[:15]
    total_sigs = ranking.get("total_signals", 0)
    overall = ranking.get("overall_rate", 0)
    if total_sigs:
        st.caption(f"全{total_sigs:,}シグナル中、2条件組み合わせの5日以内+2%到達率。全体平均: {overall:.1f}%")

    if combos:
        c_data = []
        for combo in combos:
            c_data.append({
                "フィルター条件": combo.get("conditions", ""),
                "件数": combo.get("count", combo.get("件数", 0)),
                "到達率(%)": combo.get("hit_rate", combo.get("+2%到達率", 0)),
                "平均到達日数": combo.get("avg_days", combo.get("平均到達日", 0)),
            })
        c_df = pd.DataFrame(c_data)
        st.dataframe(
            c_df.style
                .bar(subset=["到達率(%)"], color="#10b981", vmin=70, vmax=90)
                .format({"到達率(%)": "{:.1f}%", "平均到達日数": "{:.1f}日"}),
            use_container_width=True,
            hide_index=True,
        )

    # トップコンボを強調
    if combos:
        top = combos[0]
        cond = top.get("conditions", "")
        hr = top.get("hit_rate", top.get("+2%到達率", 0))
        cnt = top.get("count", top.get("件数", 0))
        avg_d = top.get("avg_days", top.get("平均到達日", 0))
        st.success(f"🏆 **最強コンボ: {cond}** → 到達率 **{hr:.1f}%** ({cnt}件, 平均{avg_d:.1f}日)")


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
    f"📊 データソース: 米国株バックテスト結果 + GradientBoostingClassifier | "
    f"学習期間: {df['date'].min()} 〜 {df['date'].max()}"
)
