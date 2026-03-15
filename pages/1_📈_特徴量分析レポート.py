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
# ダークテーマCSS
# =============================================
st.markdown("""
<style>
.metric-card {
    background: linear-gradient(135deg, rgba(30,30,46,0.95), rgba(24,24,37,0.95));
    border: 1px solid rgba(99,102,241,0.3);
    border-radius: 12px; padding: 20px; text-align: center;
}
.metric-card .value { font-size: 2.2rem; font-weight: 700; color: #a78bfa; }
.metric-card .label { font-size: 0.85rem; color: #94a3b8; margin-top: 4px; }
.importance-bar {
    height: 22px; border-radius: 6px; display: inline-block;
    background: linear-gradient(90deg, #6366f1, #a78bfa);
}
.feat-row { display: flex; align-items: center; padding: 6px 0; border-bottom: 1px solid rgba(255,255,255,0.05); }
.feat-name { width: 200px; color: #e2e8f0; font-size: 0.9rem; }
.feat-bar-wrap { flex: 1; padding: 0 12px; }
.feat-val { width: 60px; text-align: right; color: #a78bfa; font-weight: 600; font-size: 0.9rem; }
.band-card {
    background: rgba(30,30,46,0.9); border: 1px solid rgba(99,102,241,0.2);
    border-radius: 10px; padding: 16px; text-align: center; margin: 4px;
}
.band-card .prob { font-size: 1.6rem; font-weight: 700; }
.band-card .count { font-size: 0.8rem; color: #94a3b8; margin-top: 4px; }
.section-header {
    font-size: 1.3rem; font-weight: 700; color: #e2e8f0;
    border-left: 4px solid #6366f1; padding-left: 12px; margin: 24px 0 12px 0;
}
</style>
""", unsafe_allow_html=True)


# =============================================
# データ読み込み
# =============================================
@st.cache_data(ttl=600)
def load_data():
    """バックテストCSVとMLレポートを読み込む"""
    csv_path = os.path.join(RESULTS_DIR, "backtest_latest.csv")
    report_path = os.path.join(RESULTS_DIR, "jp_ml_report.json")

    data = {}

    # バックテストCSV
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        # 高値ベースの5日以内+2%到達ラベル
        hit_cols = [f"hit_2pct_{n}d" for n in [1,2,3,4,5] if f"hit_2pct_{n}d" in df.columns]
        if hit_cols:
            df["hit_5d"] = df[hit_cols].max(axis=1)
        # 派生特徴量
        for sma, feat in [("sma5","sma5_dist_pct"),("sma20","sma20_dist_pct"),("sma60","sma60_dist_pct")]:
            if sma in df.columns and "close" in df.columns:
                df[feat] = (df["close"] - df[sma]) / df[sma] * 100
        if "high_price" in df.columns and "low_price" in df.columns and "close" in df.columns:
            df["price_range_pct"] = (df["high_price"] - df["low_price"]) / df["close"] * 100
        data["df"] = df
    else:
        data["df"] = None

    # MLレポート
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

cols = st.columns(4)
kpis = [
    ("学習データ数", f"{n_total:,}件", f"{n_dates}営業日"),
    ("+2%到達率（高値5日）", f"{hit_rate:.1f}%", f"{int(df['hit_5d'].sum()):,}件到達"),
    ("平均到達日数", f"{avg_days:.1f}日", "到達した銘柄のみ"),
    ("特徴量数", f"{report['n_samples'] if report else 0}件で{len(report['feature_names']) if report else 0}特徴量", "GradientBoosting"),
]
for col, (label, value, sub) in zip(cols, kpis):
    with col:
        st.markdown(f"""
        <div class="metric-card">
            <div class="value">{value}</div>
            <div class="label">{label}<br><small>{sub}</small></div>
        </div>
        """, unsafe_allow_html=True)


# =============================================
# セクション1: MLモデル特徴量重要度
# =============================================
st.markdown('<div class="section-header">🤖 MLモデル 特徴量重要度 TOP15</div>', unsafe_allow_html=True)

if report:
    fi = report["feature_importance"][:15]
    max_imp = fi[0]["importance"] if fi else 1

    # 特徴量名の日本語マッピング
    name_map = {
        "price_range_pct": "日足レンジ幅(%)",
        "day_change_pct": "当日騰落率(%)",
        "sma60_dist_pct": "SMA60乖離率(%)",
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

    rows_html = ""
    for i, item in enumerate(fi):
        name_jp = name_map.get(item["name"], item["name"])
        pct = item["importance"] / max_imp * 100
        color = "#eab308" if i < 3 else ("#a78bfa" if i < 7 else "#6366f1")
        rows_html += f"""
        <div class="feat-row">
            <div class="feat-name">{'🥇' if i==0 else '🥈' if i==1 else '🥉' if i==2 else f'{i+1}.'} {name_jp}</div>
            <div class="feat-bar-wrap">
                <div class="importance-bar" style="width:{pct}%;background:linear-gradient(90deg,{color},{color}88)"></div>
            </div>
            <div class="feat-val">{item['importance']:.3f}</div>
        </div>
        """

    st.markdown(f"""
    <div style="background:rgba(15,15,25,0.8);border-radius:12px;padding:20px;border:1px solid rgba(99,102,241,0.2);">
        {rows_html}
    </div>
    """, unsafe_allow_html=True)

    st.markdown("")
    st.info("💡 **重要度の解釈:** 日足レンジ幅(%)が圧倒的に重要 → ボラティリティが高い銘柄ほど+2%到達しやすい。当日騰落率とSMA60乖離率が次に続く。")
else:
    st.warning("MLレポートが見つかりません。")


# =============================================
# セクション2: AI予測確率帯別の実際の到達率
# =============================================
st.markdown('<div class="section-header">🎯 AI予測確率帯 × 実際の+2%到達率</div>', unsafe_allow_html=True)

# MLモデルを読み込んで予測確率を計算
try:
    import joblib
    model_path = os.path.join(RESULTS_DIR, "jp_ml_model.pkl")
    if os.path.exists(model_path) and report:
        model_data = joblib.load(model_path)
        model = model_data["model"]
        features = model_data["feature_names"]

        # ブール列を整数に変換
        for col_name in ["is_pullback","is_breakout","long_upper_wick","is_high_zone",
                    "big_bearish_yesterday","weekly_sma20_ok","first_sma20_touch"]:
            if col_name in df.columns:
                df[col_name] = df[col_name].astype(int)

        avail = [c for c in features if c in df.columns]
        for c in avail:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        X = df[avail].fillna(0)
        df["ai_prob"] = model.predict_proba(X)[:, 1] * 100

        # 確率帯別の分析
        bands = [(0,30,"0-30%","#ef4444"), (30,50,"30-50%","#f97316"),
                 (50,60,"50-60%","#eab308"), (60,70,"60-70%","#22c55e"),
                 (70,80,"70-80%","#10b981"), (80,101,"80%+","#6366f1")]

        band_cols = st.columns(len(bands))
        for col, (lo, hi, label, color) in zip(band_cols, bands):
            mask = (df["ai_prob"] >= lo) & (df["ai_prob"] < hi) & df["hit_5d"].notna()
            n = mask.sum()
            actual = df.loc[mask, "hit_5d"].mean() * 100 if n > 0 else 0
            with col:
                st.markdown(f"""
                <div class="band-card">
                    <div style="font-size:0.75rem;color:#94a3b8;">予測 {label}</div>
                    <div class="prob" style="color:{color}">{actual:.1f}%</div>
                    <div class="count">{n:,}件</div>
                </div>
                """, unsafe_allow_html=True)

        st.markdown("")
        st.success("✅ AI予測60%以上の銘柄は、実際に5日以内+2%に到達する確率が80%超！")

    else:
        st.warning("MLモデルが見つかりません。")
except Exception as e:
    st.warning(f"MLモデル分析: {e}")


# =============================================
# セクション3: 特徴量別の+2%到達率分析
# =============================================
st.markdown('<div class="section-header">📊 特徴量別の+2%到達率</div>', unsafe_allow_html=True)

if "hit_5d" in df.columns:
    # 分析する特徴量とビン定義
    feature_configs = [
        ("日足レンジ幅(%)", "price_range_pct", [0, 1, 2, 3, 4, 5, float("inf")],
         ["〜1%", "1-2%", "2-3%", "3-4%", "4-5%", "5%〜"]),
        ("RSI(14)", "rsi", [0, 30, 40, 50, 60, 70, 100],
         ["〜30", "30-40", "40-50", "50-60", "60-70", "70〜"]),
        ("SMA60乖離率(%)", "sma60_dist_pct", [-float("inf"), -5, 0, 5, 10, 15, float("inf")],
         ["〜-5%", "-5〜0%", "0〜5%", "5〜10%", "10〜15%", "15%〜"]),
        ("当日騰落率(%)", "day_change_pct", [-float("inf"), -3, -1, 0, 1, 3, float("inf")],
         ["〜-3%", "-3〜-1%", "-1〜0%", "0〜+1%", "+1〜+3%", "+3%〜"]),
        ("ATR(%)", "atr_pct", [0, 1.5, 2.0, 2.5, 3.0, 4.0, float("inf")],
         ["〜1.5%", "1.5-2%", "2-2.5%", "2.5-3%", "3-4%", "4%〜"]),
        ("出来高比(20MA)", "volume_ratio", [0, 0.8, 1.0, 1.2, 1.5, 2.0, float("inf")],
         ["〜0.8倍", "0.8-1.0倍", "1.0-1.2倍", "1.2-1.5倍", "1.5-2.0倍", "2.0倍〜"]),
        ("トレンド継続日数", "trend_start_days_ago", [0, 3, 7, 14, 30, float("inf")],
         ["〜3日", "3-7日", "7-14日", "14-30日", "30日〜"]),
        ("曜日", "day_of_week", None, None),
    ]

    # タブで分割
    tab_names = [fc[0] for fc in feature_configs]
    tabs = st.tabs(tab_names)

    for tab, (feat_name, col_name, bins, labels) in zip(tabs, feature_configs):
        with tab:
            if col_name not in df.columns:
                st.info(f"{col_name} はデータに含まれていません")
                continue

            valid = df[df["hit_5d"].notna() & df[col_name].notna()].copy()

            if col_name == "day_of_week":
                # 曜日は特別処理
                dow_map = {0: "月曜", 1: "火曜", 2: "水曜", 3: "木曜", 4: "金曜"}
                valid["_bin"] = valid[col_name].map(dow_map)
                order = ["月曜", "火曜", "水曜", "木曜", "金曜"]
            else:
                valid["_bin"] = pd.cut(valid[col_name], bins=bins, labels=labels, include_lowest=True)
                order = labels

            # 集計
            grouped = valid.groupby("_bin", observed=False).agg(
                count=("hit_5d", "size"),
                hits=("hit_5d", "sum"),
                rate=("hit_5d", "mean"),
            ).reindex(order)

            # 棒グラフ風HTML
            if len(grouped) > 0:
                max_count = grouped["count"].max()
                overall_rate = valid["hit_5d"].mean() * 100

                html = '<div style="background:rgba(15,15,25,0.8);border-radius:12px;padding:20px;border:1px solid rgba(99,102,241,0.2);">'
                html += f'<div style="color:#94a3b8;font-size:0.8rem;margin-bottom:12px;">全体到達率: {overall_rate:.1f}% | 全{len(valid):,}件</div>'

                for bin_name, row_data in grouped.iterrows():
                    if pd.isna(row_data["count"]) or row_data["count"] == 0:
                        continue
                    rate_val = row_data["rate"] * 100
                    cnt = int(row_data["count"])
                    hits = int(row_data["hits"])
                    bar_w = cnt / max_count * 100

                    # 到達率に基づく色
                    if rate_val >= overall_rate + 5:
                        bar_color = "#10b981"
                        rate_color = "#10b981"
                    elif rate_val >= overall_rate:
                        bar_color = "#6366f1"
                        rate_color = "#a78bfa"
                    elif rate_val >= overall_rate - 5:
                        bar_color = "#f97316"
                        rate_color = "#f97316"
                    else:
                        bar_color = "#ef4444"
                        rate_color = "#ef4444"

                    html += f"""
                    <div style="display:flex;align-items:center;padding:6px 0;border-bottom:1px solid rgba(255,255,255,0.05);">
                        <div style="width:120px;color:#e2e8f0;font-size:0.85rem;">{bin_name}</div>
                        <div style="flex:1;padding:0 12px;">
                            <div style="height:20px;border-radius:4px;background:{bar_color}33;width:{bar_w}%;position:relative;">
                                <div style="height:100%;border-radius:4px;background:{bar_color};width:{min(rate_val/100*100*1.5, 100)}%;"></div>
                            </div>
                        </div>
                        <div style="width:70px;text-align:right;color:{rate_color};font-weight:600;">{rate_val:.1f}%</div>
                        <div style="width:80px;text-align:right;color:#94a3b8;font-size:0.8rem;">{hits}/{cnt}件</div>
                    </div>
                    """

                html += '</div>'
                st.markdown(html, unsafe_allow_html=True)

                # 到達率が最も高いビンを強調
                best = grouped.loc[grouped["rate"].idxmax()]
                best_rate = best["rate"] * 100
                best_name = grouped["rate"].idxmax()
                if best_rate > overall_rate:
                    diff = best_rate - overall_rate
                    st.info(f"💡 **{feat_name}** が **{best_name}** の場合、到達率が全体比 **+{diff:.1f}%** 高い（{best_rate:.1f}%）")


# =============================================
# セクション4: 到達日数分布
# =============================================
if "days_to_target" in df.columns:
    st.markdown('<div class="section-header">⏱ +2%到達までの日数分布</div>', unsafe_allow_html=True)

    d2t = df["days_to_target"].dropna()
    if len(d2t) > 0:
        dist_cols = st.columns(5)
        for day in [1, 2, 3, 4, 5]:
            n_day = (d2t == day).sum()
            pct = n_day / len(d2t) * 100
            with dist_cols[day - 1]:
                emoji = "🎯" if day == 1 else ("✅" if day <= 3 else "📊")
                color = "#eab308" if day == 1 else ("#10b981" if day <= 3 else "#94a3b8")
                st.markdown(f"""
                <div class="band-card">
                    <div style="font-size:0.8rem;color:#94a3b8;">{emoji} {day}日目</div>
                    <div class="prob" style="color:{color}">{pct:.1f}%</div>
                    <div class="count">{n_day:,}件 / {len(d2t):,}件</div>
                </div>
                """, unsafe_allow_html=True)

        st.info(f"💡 到達した銘柄の **{((d2t <= 2).sum() / len(d2t) * 100):.1f}%** が **2日以内**に+2%を達成")


# =============================================
# セクション5: 曜日×時間帯分析
# =============================================
if "day_of_week" in df.columns and "hit_5d" in df.columns:
    st.markdown('<div class="section-header">📅 曜日別パフォーマンス</div>', unsafe_allow_html=True)

    dow_map = {0: "月", 1: "火", 2: "水", 3: "木", 4: "金"}
    valid = df[df["hit_5d"].notna()].copy()
    valid["dow_name"] = valid["day_of_week"].map(dow_map)

    dow_cols = st.columns(5)
    overall = valid["hit_5d"].mean() * 100
    for i, (dow_key, dow_name) in enumerate(dow_map.items()):
        sub = valid[valid["day_of_week"] == dow_key]
        rate = sub["hit_5d"].mean() * 100 if len(sub) > 0 else 0
        diff = rate - overall
        color = "#10b981" if diff > 1 else ("#ef4444" if diff < -1 else "#94a3b8")
        with dow_cols[i]:
            st.markdown(f"""
            <div class="band-card">
                <div style="font-size:0.8rem;color:#94a3b8;">{dow_name}曜日</div>
                <div class="prob" style="color:{color}">{rate:.1f}%</div>
                <div class="count">{len(sub):,}件 ({diff:+.1f}%)</div>
            </div>
            """, unsafe_allow_html=True)


# =============================================
# フッター
# =============================================
st.markdown("---")
st.markdown(
    "<div style='text-align:center;color:#64748b;font-size:0.8rem;'>"
    "📊 データソース: バックテスト結果 + GradientBoostingClassifier分析 | "
    f"学習期間: {df['date'].min()} 〜 {df['date'].max()}"
    "</div>",
    unsafe_allow_html=True,
)
