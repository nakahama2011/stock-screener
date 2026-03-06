"""
特徴量自動分析スクリプト

過去3ヶ月のスクリーニング結果を一括取得し、
「5日以内にプラスになる銘柄」の特徴量を統計的に分析する。

使い方:
    python3 analyze_features.py

出力:
    results/feature_analysis_report.html
"""

import argparse
import os
import sys
import warnings
from datetime import datetime, timedelta
from itertools import combinations

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# 同じディレクトリの backtester.py を読み込む
from backtester import (
    fetch_jpx_tickers,
    _fallback_tickers,
    run_backtest,
    DEFAULT_MIN_VOLUME,
    DEFAULT_HIT_THRESHOLD,
    SCRIPT_DIR,
    OUTPUT_DIR,
)


# =============================================================
# 定数
# =============================================================
ANALYSIS_MONTHS = 3  # 分析対象期間（月）
MIN_SAMPLE_SIZE = 10  # 分析に必要な最小サンプル数


# =============================================================
# 特徴量の定義（ビン分割ルール）
# =============================================================
FEATURE_BINS = {
    "RSI(14)": {
        "column": "rsi",
        "bins": [0, 30, 50, 65, 80, 100],
        "labels": ["0-30（売られすぎ）", "30-50", "50-65（適正）", "65-80", "80-100（買われすぎ）"],
    },
    "出来高比(20MA)": {
        "column": "volume_ratio",
        "bins": [0, 0.8, 1.0, 1.2, 1.5, 2.0, float("inf")],
        "labels": ["〜0.8倍", "0.8〜1.0倍", "1.0〜1.2倍", "1.2〜1.5倍", "1.5〜2.0倍", "2.0倍〜"],
    },
    "当日騰落率(%)": {
        "column": "day_change_pct",
        "bins": [-float("inf"), -3, -1, 0, 1, 3, float("inf")],
        "labels": ["〜-3%", "-3%〜-1%", "-1%〜0%", "0%〜+1%", "+1%〜+3%", "+3%〜"],
    },
    "前日騰落率(%)": {
        "column": "prev_day_change_pct",
        "bins": [-float("inf"), -3, -1, 0, 1, 3, float("inf")],
        "labels": ["〜-3%", "-3%〜-1%", "-1%〜0%", "0%〜+1%", "+1%〜+3%", "+3%〜"],
    },
    "当日出来高/前日比(%)": {
        "column": "vol_today_vs_yday_pct",
        "bins": [-float("inf"), -20, 0, 10, 30, 50, float("inf")],
        "labels": ["〜-20%", "-20%〜0%", "0%〜+10%", "+10%〜+30%", "+30%〜+50%", "+50%〜"],
    },
}

# フラグ型（True/False）の特徴量
FLAG_FEATURES = {
    "押し目(プルバック)": "is_pullback",
    "20日高値ブレイク": "is_breakout",
    "長大上ヒゲ": "long_upper_wick",
    "高値圏(20日HH 3%以内)": "is_high_zone",
    "前日大陰線": "big_bearish_yesterday",
    "週足SMA20上抜け": "weekly_sma20_ok",
}

# 曜日
WEEKDAY_NAMES = {0: "月曜", 1: "火曜", 2: "水曜", 3: "木曜", 4: "金曜"}


def run_analysis(use_sample: bool = False):
    """メイン分析処理を実行する。"""
    print("=" * 60)
    print("📊 特徴量自動分析システム")
    if use_sample:
        print("   ⚡ サンプル50銘柄モード（高速テスト用）")
    print("=" * 60)

    # ---- 分析期間の決定 ----
    end_dt = datetime.now() - timedelta(days=7)  # 直近1週間は先読みデータ不足のため除外
    start_dt = end_dt - timedelta(days=ANALYSIS_MONTHS * 30)
    start_date = start_dt.strftime("%Y-%m-%d")
    end_date = end_dt.strftime("%Y-%m-%d")

    print(f"\n📅 分析期間: {start_date} 〜 {end_date}（約{ANALYSIS_MONTHS}ヶ月）")

    # ---- 銘柄リストの取得 ----
    print("\n📥 銘柄リスト取得中...")
    if use_sample:
        tickers_df = _fallback_tickers()
    else:
        tickers_df = fetch_jpx_tickers()
    print(f"   {len(tickers_df)} 銘柄を対象に分析します")

    # ---- バックテスト実行 ----
    print("\n🔍 バックテスト実行中（しばらくお待ちください）...")
    bt_df = run_backtest(
        tickers_df,
        start_date,
        end_date,
        min_volume=DEFAULT_MIN_VOLUME,
        hit_threshold=DEFAULT_HIT_THRESHOLD,
    )

    if bt_df.empty:
        print("❌ バックテスト結果が空です。期間を変更してください。")
        return

    total_signals = len(bt_df)
    print(f"\n✅ 取得完了: {total_signals} シグナル")

    # ---- 5日以内プラス列の計算 ----
    if "pos_within_5d" not in bt_df.columns:
        print("⚠️ pos_within_5d 列が見つかりません。手動計算します。")
        # フォワードリターンの最大値が正なら True
        ret_cols_5d = [c for c in ["ret_1d", "ret_2d", "ret_3d", "ret_4d", "ret_5d"] if c in bt_df.columns]
        if ret_cols_5d:
            bt_df["pos_within_5d"] = bt_df[ret_cols_5d].max(axis=1).apply(lambda x: 1 if x > 0 else 0)

    if "pos_within_3d" not in bt_df.columns:
        ret_cols_3d = [c for c in ["ret_1d", "ret_2d", "ret_3d"] if c in bt_df.columns]
        if ret_cols_3d:
            bt_df["pos_within_3d"] = bt_df[ret_cols_3d].max(axis=1).apply(lambda x: 1 if x > 0 else 0)

    # ---- 分析実行 ----
    print("\n📈 特徴量分析を実行中...")

    results = {}

    # 全体の基準勝率
    valid_5d = bt_df[bt_df["pos_within_5d"].notna()]
    overall_win_rate_5d = valid_5d["pos_within_5d"].mean() * 100 if len(valid_5d) > 0 else 0
    valid_3d = bt_df[bt_df["pos_within_3d"].notna()] if "pos_within_3d" in bt_df.columns else pd.DataFrame()
    overall_win_rate_3d = valid_3d["pos_within_3d"].mean() * 100 if len(valid_3d) > 0 else 0

    results["overall"] = {
        "total_signals": total_signals,
        "win_rate_5d": round(overall_win_rate_5d, 1),
        "win_rate_3d": round(overall_win_rate_3d, 1),
        "period": f"{start_date} 〜 {end_date}",
    }

    # ---- (1) ビン分割型の特徴量分析 ----
    bin_results = {}
    for feat_name, feat_cfg in FEATURE_BINS.items():
        col = feat_cfg["column"]
        if col not in bt_df.columns:
            continue

        df_valid = bt_df[[col, "pos_within_5d"]].dropna()
        if len(df_valid) < MIN_SAMPLE_SIZE:
            continue

        df_valid["_bin"] = pd.cut(
            df_valid[col],
            bins=feat_cfg["bins"],
            labels=feat_cfg["labels"],
            right=False,
        )

        grouped = df_valid.groupby("_bin", observed=True).agg(
            count=("pos_within_5d", "count"),
            win_count=("pos_within_5d", "sum"),
        )
        grouped["win_rate"] = (grouped["win_count"] / grouped["count"] * 100).round(1)
        grouped = grouped[grouped["count"] >= MIN_SAMPLE_SIZE]

        if len(grouped) > 0:
            bin_results[feat_name] = grouped.reset_index().to_dict("records")

    results["bin_features"] = bin_results

    # ---- (2) フラグ型の特徴量分析 ----
    flag_results = {}
    for feat_name, col in FLAG_FEATURES.items():
        if col not in bt_df.columns:
            continue

        df_valid = bt_df[[col, "pos_within_5d"]].dropna()
        for flag_val, label in [(True, "あり"), (False, "なし")]:
            subset = df_valid[df_valid[col] == flag_val]
            if len(subset) >= MIN_SAMPLE_SIZE:
                wr = subset["pos_within_5d"].mean() * 100
                if feat_name not in flag_results:
                    flag_results[feat_name] = []
                flag_results[feat_name].append({
                    "condition": label,
                    "count": len(subset),
                    "win_rate": round(wr, 1),
                })

    results["flag_features"] = flag_results

    # ---- (3) 曜日分析 ----
    weekday_results = []
    if "day_of_week" in bt_df.columns:
        for dow, name in WEEKDAY_NAMES.items():
            subset = bt_df[(bt_df["day_of_week"] == dow) & bt_df["pos_within_5d"].notna()]
            if len(subset) >= MIN_SAMPLE_SIZE:
                wr = subset["pos_within_5d"].mean() * 100
                weekday_results.append({
                    "day": name,
                    "count": len(subset),
                    "win_rate": round(wr, 1),
                })
    results["weekday"] = weekday_results

    # ---- (4) 条件の組み合わせ分析（上位パターン発見） ----
    print("🔎 最適な条件の組み合わせを探索中...")

    # 各条件をブール列として定義する
    combo_conditions = {}

    if "rsi" in bt_df.columns:
        combo_conditions["RSI 50-65"] = (bt_df["rsi"] >= 50) & (bt_df["rsi"] <= 65)
        combo_conditions["RSI 30-50"] = (bt_df["rsi"] >= 30) & (bt_df["rsi"] < 50)

    if "volume_ratio" in bt_df.columns:
        combo_conditions["出来高比≥1.2"] = bt_df["volume_ratio"] >= 1.2
        combo_conditions["出来高比≥1.5"] = bt_df["volume_ratio"] >= 1.5

    if "day_change_pct" in bt_df.columns:
        combo_conditions["当日陽線"] = bt_df["day_change_pct"] > 0
        combo_conditions["当日陰線"] = bt_df["day_change_pct"] < 0

    if "prev_day_change_pct" in bt_df.columns:
        combo_conditions["前日陰線"] = bt_df["prev_day_change_pct"] < 0
        combo_conditions["前日陽線"] = bt_df["prev_day_change_pct"] > 0

    for feat_name, col in FLAG_FEATURES.items():
        if col in bt_df.columns:
            combo_conditions[feat_name] = bt_df[col] == True

    if "day_of_week" in bt_df.columns:
        combo_conditions["火〜木曜"] = bt_df["day_of_week"].isin([1, 2, 3])

    # 2〜3個の組み合わせで探索する
    combo_results = []
    condition_names = list(combo_conditions.keys())

    for r in range(2, min(4, len(condition_names) + 1)):
        for combo in combinations(condition_names, r):
            mask = pd.Series(True, index=bt_df.index)
            for c in combo:
                mask &= combo_conditions[c]

            subset = bt_df[mask & bt_df["pos_within_5d"].notna()]
            if len(subset) >= MIN_SAMPLE_SIZE:
                wr = subset["pos_within_5d"].mean() * 100
                combo_results.append({
                    "conditions": " + ".join(combo),
                    "count": len(subset),
                    "win_rate": round(wr, 1),
                    "n_conditions": r,
                })

    # 勝率順にソートする
    combo_results.sort(key=lambda x: (-x["win_rate"], -x["count"]))
    results["combos"] = combo_results[:30]  # 上位30件

    # ---- (5) 翌日+2%達成の条件組み合わせ分析 ----
    print("🔎 翌日+2%達成の最適条件を探索中...")
    combo_results_2pct = []
    if "ret_1d" in bt_df.columns:
        # 翌日+2%達成フラグを作成する
        bt_df["_next_day_2pct"] = (bt_df["ret_1d"] >= 2.0).astype(int)
        overall_2pct_rate = bt_df["_next_day_2pct"].mean() * 100
        results["overall"]["next_day_2pct_rate"] = round(overall_2pct_rate, 1)

        for r in range(2, min(4, len(condition_names) + 1)):
            for combo in combinations(condition_names, r):
                mask = pd.Series(True, index=bt_df.index)
                for c in combo:
                    mask &= combo_conditions[c]

                subset = bt_df[mask & bt_df["ret_1d"].notna()]
                if len(subset) >= MIN_SAMPLE_SIZE:
                    wr = subset["_next_day_2pct"].mean() * 100
                    combo_results_2pct.append({
                        "conditions": " + ".join(combo),
                        "count": len(subset),
                        "win_rate": round(wr, 1),
                        "n_conditions": r,
                    })

        combo_results_2pct.sort(key=lambda x: (-x["win_rate"], -x["count"]))
    results["combos_2pct"] = combo_results_2pct[:30]

    # ---- HTMLレポート生成 ----
    print("\n📝 レポートを生成中...")
    html = _generate_html_report(results)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    report_path = os.path.join(OUTPUT_DIR, "feature_analysis_report.html")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n✅ レポートを保存しました: {report_path}")
    print(f"   ブラウザで開いてください: file://{os.path.abspath(report_path)}")

    return results


def _generate_html_report(results: dict) -> str:
    """分析結果をHTMLレポートに変換する。"""

    overall = results["overall"]

    # ---- ビン分割型テーブルを生成する ----
    bin_tables = ""
    for feat_name, records in results.get("bin_features", {}).items():
        rows = ""
        for r in records:
            wr = r["win_rate"]
            bar_width = min(wr, 100)
            bar_color = "#10b981" if wr >= overall["win_rate_5d"] else "#ef4444"
            diff = wr - overall["win_rate_5d"]
            diff_str = f'<span style="color:{bar_color};font-weight:bold">{diff:+.1f}%</span>'
            rows += f"""
            <tr>
                <td>{r['_bin']}</td>
                <td style="text-align:right">{r['count']}</td>
                <td style="text-align:right;font-weight:bold">{wr:.1f}%</td>
                <td style="text-align:right">{diff_str}</td>
                <td>
                    <div style="background:#1f2937;border-radius:4px;height:20px;width:200px;position:relative">
                        <div style="background:{bar_color};height:100%;width:{bar_width}%;border-radius:4px;opacity:0.8"></div>
                    </div>
                </td>
            </tr>"""

        bin_tables += f"""
        <div class="card">
            <h3>📊 {feat_name}</h3>
            <table>
                <thead><tr>
                    <th>範囲</th><th>件数</th><th>5日以内勝率</th><th>全体比</th><th>分布</th>
                </tr></thead>
                <tbody>{rows}</tbody>
            </table>
        </div>"""

    # ---- フラグ型テーブルを生成する ----
    flag_table_rows = ""
    for feat_name, records in results.get("flag_features", {}).items():
        for r in records:
            wr = r["win_rate"]
            bar_color = "#10b981" if wr >= overall["win_rate_5d"] else "#ef4444"
            diff = wr - overall["win_rate_5d"]
            diff_str = f'<span style="color:{bar_color};font-weight:bold">{diff:+.1f}%</span>'
            flag_table_rows += f"""
            <tr>
                <td>{feat_name}</td>
                <td>{r['condition']}</td>
                <td style="text-align:right">{r['count']}</td>
                <td style="text-align:right;font-weight:bold">{wr:.1f}%</td>
                <td style="text-align:right">{diff_str}</td>
            </tr>"""

    # ---- 曜日テーブルを生成する ----
    weekday_rows = ""
    for r in results.get("weekday", []):
        wr = r["win_rate"]
        bar_color = "#10b981" if wr >= overall["win_rate_5d"] else "#ef4444"
        diff = wr - overall["win_rate_5d"]
        diff_str = f'<span style="color:{bar_color};font-weight:bold">{diff:+.1f}%</span>'
        weekday_rows += f"""
        <tr>
            <td>{r['day']}</td>
            <td style="text-align:right">{r['count']}</td>
            <td style="text-align:right;font-weight:bold">{wr:.1f}%</td>
            <td style="text-align:right">{diff_str}</td>
        </tr>"""

    # ---- 組み合わせテーブルを生成する ----
    combo_rows = ""
    for i, r in enumerate(results.get("combos", []), 1):
        wr = r["win_rate"]
        bar_color = "#10b981" if wr >= overall["win_rate_5d"] else "#ef4444"
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}"
        combo_rows += f"""
        <tr>
            <td style="text-align:center">{medal}</td>
            <td>{r['conditions']}</td>
            <td style="text-align:right">{r['count']}</td>
            <td style="text-align:right;font-weight:bold;color:{bar_color}">{wr:.1f}%</td>
        </tr>"""

    # ---- 翌日+2%達成の組み合わせテーブルを生成する ----
    combo_2pct_rows = ""
    overall_2pct = overall.get("next_day_2pct_rate", 0)
    for i, r in enumerate(results.get("combos_2pct", []), 1):
        wr = r["win_rate"]
        bar_color = "#10b981" if wr >= overall_2pct else "#ef4444"
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}"
        combo_2pct_rows += f"""
        <tr>
            <td style="text-align:center">{medal}</td>
            <td>{r['conditions']}</td>
            <td style="text-align:right">{r['count']}</td>
            <td style="text-align:right;font-weight:bold;color:{bar_color}">{wr:.1f}%</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>特徴量分析レポート</title>
<style>
    :root {{
        --bg: #0f172a;
        --card-bg: #1e293b;
        --text: #e2e8f0;
        --muted: #94a3b8;
        --accent: #38bdf8;
        --green: #10b981;
        --red: #ef4444;
        --border: #334155;
    }}
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{
        background: var(--bg);
        color: var(--text);
        font-family: 'Segoe UI', 'Hiragino Sans', sans-serif;
        line-height: 1.6;
        padding: 2rem;
    }}
    h1 {{
        text-align: center;
        font-size: 1.8rem;
        margin-bottom: 0.5rem;
        color: var(--accent);
    }}
    .subtitle {{
        text-align: center;
        color: var(--muted);
        margin-bottom: 2rem;
    }}
    .kpi-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: 1rem;
        margin-bottom: 2rem;
    }}
    .kpi {{
        background: var(--card-bg);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 1.5rem;
        text-align: center;
    }}
    .kpi .label {{ color: var(--muted); font-size: 0.85rem; }}
    .kpi .value {{ font-size: 2rem; font-weight: bold; color: var(--accent); margin-top: 4px; }}
    .card {{
        background: var(--card-bg);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1.5rem;
    }}
    .card h3 {{
        color: var(--accent);
        margin-bottom: 1rem;
        font-size: 1.1rem;
    }}
    table {{
        width: 100%;
        border-collapse: collapse;
        font-size: 0.9rem;
    }}
    th {{
        background: #0f172a;
        color: var(--muted);
        text-align: left;
        padding: 8px 12px;
        border-bottom: 2px solid var(--border);
        font-weight: 600;
    }}
    td {{
        padding: 8px 12px;
        border-bottom: 1px solid var(--border);
    }}
    tr:hover {{
        background: rgba(56, 189, 248, 0.05);
    }}
    .section-title {{
        font-size: 1.3rem;
        color: var(--accent);
        margin: 2rem 0 1rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid var(--border);
    }}
    .note {{
        color: var(--muted);
        font-size: 0.85rem;
        margin-top: 0.5rem;
    }}
</style>
</head>
<body>

<h1>📊 特徴量分析レポート</h1>
<p class="subtitle">{overall['period']} ・ {overall['total_signals']}シグナル</p>
<div style="text-align:center;margin-bottom:1.5rem">
    <a href="http://localhost:8501" target="_blank"
       style="color:#38bdf8;text-decoration:none;font-size:0.9rem;
              border:1px solid #334155;border-radius:8px;padding:6px 16px;
              display:inline-block;transition:background 0.2s"
       onmouseover="this.style.background='#1e293b'"
       onmouseout="this.style.background='transparent'">
       📊 スクリーニング画面に戻る →
    </a>
</div>

<div class="kpi-grid">
    <div class="kpi">
        <div class="label">分析シグナル数</div>
        <div class="value">{overall['total_signals']:,}</div>
    </div>
    <div class="kpi">
        <div class="label">全体 5日以内勝率</div>
        <div class="value" style="color:var(--green)">{overall['win_rate_5d']:.1f}%</div>
    </div>
    <div class="kpi">
        <div class="label">全体 3日以内勝率</div>
        <div class="value" style="color:var(--green)">{overall['win_rate_3d']:.1f}%</div>
    </div>
</div>

<h2 class="section-title">📈 特徴量ごとの5日以内勝率</h2>
<p class="note">※ 各特徴量の範囲別に勝率を集計し、全体勝率との差を表示しています。緑は全体より高い、赤は全体より低い範囲です。</p>
{bin_tables}

<h2 class="section-title">🏷️ フラグ型条件</h2>
<div class="card">
    <table>
        <thead><tr>
            <th>条件</th><th>状態</th><th>件数</th><th>5日以内勝率</th><th>全体比</th>
        </tr></thead>
        <tbody>{flag_table_rows}</tbody>
    </table>
</div>

<h2 class="section-title">📅 曜日別の勝率</h2>
<div class="card">
    <table>
        <thead><tr>
            <th>曜日</th><th>件数</th><th>5日以内勝率</th><th>全体比</th>
        </tr></thead>
        <tbody>{weekday_rows}</tbody>
    </table>
</div>

<h2 class="section-title">🏆 5日以内プラス 最強の条件組み合わせ TOP30</h2>
<p class="note">※ 2〜3条件のあらゆる組み合わせを調査し、サンプル数{MIN_SAMPLE_SIZE}件以上で勝率が高い順に表示しています。</p>
<div class="card">
    <table>
        <thead><tr>
            <th style="width:40px">順位</th><th>条件の組み合わせ</th><th>件数</th><th>5日以内勝率</th>
        </tr></thead>
        <tbody>{combo_rows}</tbody>
    </table>
</div>

<h2 class="section-title">🚀 翌日+2%達成 最強の条件組み合わせ TOP30</h2>
<p class="note">※ 翌日の終値が+2%以上上昇する確率が高い組み合わせです。全体の翌日+2%達成率: {overall.get('next_day_2pct_rate', 'N/A')}%</p>
<div class="card">
    <table>
        <thead><tr>
            <th style="width:40px">順位</th><th>条件の組み合わせ</th><th>件数</th><th>翌日+2%達成率</th>
        </tr></thead>
        <tbody>{combo_2pct_rows}</tbody>
    </table>
</div>

<p class="note" style="text-align:center; margin-top:2rem">
    生成日時: {datetime.now().strftime('%Y年%m月%d日 %H:%M')} ・ 
    基準: SMA5&gt;SMA20&gt;SMA60, 出来高≥{DEFAULT_MIN_VOLUME:,}株
</p>

</body>
</html>"""

    return html


# =============================================================
# エントリーポイント
# =============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="特徴量自動分析システム")
    parser.add_argument("--sample", action="store_true",
                        help="サンプル50銘柄で高速実行する（テスト用）")
    args = parser.parse_args()
    run_analysis(use_sample=args.sample)
