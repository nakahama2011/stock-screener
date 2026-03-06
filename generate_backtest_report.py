"""
レベル1 バックテスト結果 HTMLレポート生成スクリプト

backtester.py が出力した CSV と サマリーJSON を読み込み、
検証結果をリッチなHTMLダッシュボードとして出力する。
"""

import json
import math
import os
from datetime import datetime

import pandas as pd


# =============================================================
# 定数
# =============================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "results")
DEFAULT_CSV = os.path.join(OUTPUT_DIR, "backtest_latest.csv")
DEFAULT_JSON = os.path.join(OUTPUT_DIR, "backtest_latest_summary.json")
OUTPUT_HTML = os.path.join(OUTPUT_DIR, "backtest_report.html")

# レベル1レポートへの相対パス
REPORT_L1_REL = "report.html"


# =============================================================
# HTMLテンプレート（CSS + 骨格）
# =============================================================
HTML_HEAD = """<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>バックテスト結果 | レベル1検証</title>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Noto+Sans+JP:wght@300;400;500;700&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg-primary: #0a0e17;
      --bg-secondary: #111827;
      --bg-card: #1a2332;
      --bg-card-hover: #1f2b3d;
      --border: #2a3a4e;
      --text-primary: #e8edf5;
      --text-secondary: #8899aa;
      --text-muted: #5a6a7a;
      --blue: #3b82f6;
      --cyan: #06b6d4;
      --green: #10b981;
      --yellow: #f59e0b;
      --red: #ef4444;
      --grad: linear-gradient(135deg, #3b82f6, #06b6d4);
    }
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: 'Inter', 'Noto Sans JP', sans-serif;
      background: var(--bg-primary);
      color: var(--text-primary);
      min-height: 100vh;
      line-height: 1.6;
    }

    /* ヘッダー */
    .header {
      background: var(--bg-secondary);
      border-bottom: 1px solid var(--border);
      padding: 1.2rem 2rem;
      position: sticky; top: 0; z-index: 100;
      backdrop-filter: blur(12px);
    }
    .header-inner {
      max-width: 1500px; margin: 0 auto;
      display: flex; justify-content: space-between; align-items: center;
    }
    .header h1 {
      font-size: 1.4rem; font-weight: 700;
      background: var(--grad);
      -webkit-background-clip: text; -webkit-text-fill-color: transparent;
      background-clip: text;
    }
    .header-right { display: flex; gap: 1rem; align-items: center; }
    .badge {
      background: rgba(6,182,212,0.15); border: 1px solid rgba(6,182,212,0.35);
      color: var(--cyan); padding: 0.2rem 0.6rem; border-radius: 6px;
      font-size: 0.78rem; font-weight: 700;
    }
    .btn-link {
      display: inline-flex; align-items: center; gap: 0.35rem;
      padding: 0.4rem 0.9rem; background: rgba(59,130,246,0.12);
      border: 1px solid rgba(59,130,246,0.35); color: var(--blue);
      border-radius: 8px; font-size: 0.82rem; font-weight: 600;
      cursor: pointer; text-decoration: none; transition: all 0.2s;
    }
    .btn-link:hover { background: rgba(59,130,246,0.25); }

    /* メイン */
    .main { max-width: 1500px; margin: 0 auto; padding: 2rem; }

    /* 条件サマリーバー */
    .condition-bar {
      background: var(--bg-card); border: 1px solid var(--border);
      border-radius: 10px; padding: 0.9rem 1.5rem; margin-bottom: 1.5rem;
      display: flex; gap: 1.5rem; flex-wrap: wrap; align-items: center;
    }
    .condition-bar .label { color: var(--text-muted); font-size: 0.8rem; }
    .condition-bar .val { color: var(--text-primary); font-weight: 600; font-size: 0.9rem; }
    .cond-sep { color: var(--border); }

    /* KPIカード */
    .kpi-grid {
      display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 1rem; margin-bottom: 2rem;
    }
    .kpi-card {
      background: var(--bg-card); border: 1px solid var(--border);
      border-radius: 12px; padding: 1.25rem 1.5rem;
      transition: all 0.3s; position: relative; overflow: hidden;
    }
    .kpi-card::before {
      content: ''; position: absolute; top: 0; left: 0; right: 0; height: 3px;
      background: var(--grad); opacity: 0; transition: opacity 0.3s;
    }
    .kpi-card:hover { background: var(--bg-card-hover); box-shadow: 0 0 30px rgba(59,130,246,0.12); transform: translateY(-2px); }
    .kpi-card:hover::before { opacity: 1; }
    .kpi-label { color: var(--text-secondary); font-size: 0.78rem; font-weight: 500; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.4rem; }
    .kpi-value { font-size: 1.8rem; font-weight: 700; background: var(--grad); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text; }
    .kpi-sub { color: var(--text-muted); font-size: 0.75rem; margin-top: 0.2rem; }
    .kpi-value.green { background: linear-gradient(135deg, #10b981, #06b6d4); -webkit-background-clip: text; background-clip: text; }
    .kpi-value.yellow { background: linear-gradient(135deg, #f59e0b, #f97316); -webkit-background-clip: text; background-clip: text; }

    /* セクションタイトル */
    .section-title {
      font-size: 1rem; font-weight: 700; color: var(--text-primary);
      margin-bottom: 1rem; display: flex; align-items: center; gap: 0.5rem;
    }
    .section-title::after {
      content: ''; flex: 1; height: 1px; background: var(--border);
    }

    /* リターン統計テーブル（横並び） */
    .ret-stats-grid {
      display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 1rem; margin-bottom: 2rem;
    }
    .ret-stat-card {
      background: var(--bg-card); border: 1px solid var(--border);
      border-radius: 10px; padding: 1rem 1.2rem;
    }
    .ret-stat-title { color: var(--cyan); font-size: 0.85rem; font-weight: 700; margin-bottom: 0.6rem; }
    .ret-stat-row { display: flex; justify-content: space-between; font-size: 0.82rem; margin: 0.2rem 0; }
    .ret-stat-row .k { color: var(--text-secondary); }
    .ret-stat-row .v { color: var(--text-primary); font-weight: 600; }
    .ret-stat-row .v.pos { color: var(--green); }
    .ret-stat-row .v.neg { color: var(--red); }

    /* 上位銘柄テーブル */
    .top-ticker-table { width: 100%; border-collapse: collapse; }
    .top-ticker-table th {
      background: var(--bg-secondary); padding: 0.7rem 1rem;
      font-size: 0.75rem; font-weight: 600; color: var(--text-secondary);
      text-transform: uppercase; letter-spacing: 0.05em;
      border-bottom: 1px solid var(--border); text-align: left;
    }
    .top-ticker-table td { padding: 0.7rem 1rem; font-size: 0.87rem; border-bottom: 1px solid rgba(42,58,78,0.4); }
    .top-ticker-table tr:hover td { background: var(--bg-card-hover); }
    .rank { color: var(--text-muted); text-align: center; font-weight: 700; }
    .rank.top3 { color: var(--yellow); }
    .code-cell { color: var(--blue); font-weight: 700; font-family: monospace; }
    .hit-bar-wrap { display: flex; align-items: center; gap: 0.5rem; }
    .hit-bar { height: 6px; border-radius: 3px; background: var(--grad); min-width: 4px; }

    /* 詳細テーブル */
    .table-wrap {
      background: var(--bg-card); border: 1px solid var(--border);
      border-radius: 12px; overflow: hidden; margin-bottom: 2rem;
    }
    .table-toolbar {
      padding: 0.9rem 1.2rem; border-bottom: 1px solid var(--border);
      display: flex; justify-content: space-between; align-items: center; gap: 1rem; flex-wrap: wrap;
    }
    .table-toolbar h2 { font-size: 1rem; font-weight: 600; }
    .toolbar-right { display: flex; gap: 0.75rem; align-items: center; }
    .search-box {
      background: var(--bg-secondary); border: 1px solid var(--border);
      border-radius: 8px; padding: 0.45rem 0.9rem; color: var(--text-primary);
      font-size: 0.82rem; width: 200px; outline: none; transition: border-color 0.3s;
    }
    .search-box:focus { border-color: var(--blue); }
    .search-box::placeholder { color: var(--text-muted); }
    .filter-select {
      background: var(--bg-secondary); border: 1px solid var(--border);
      border-radius: 8px; padding: 0.45rem 0.7rem; color: var(--text-primary);
      font-size: 0.82rem; outline: none; cursor: pointer;
    }
    .detail-table { width: 100%; border-collapse: collapse; }
    .detail-table th {
      background: var(--bg-secondary); padding: 0.7rem 0.8rem;
      font-size: 0.72rem; font-weight: 600; color: var(--text-secondary);
      text-transform: uppercase; letter-spacing: 0.04em;
      border-bottom: 1px solid var(--border); cursor: pointer; user-select: none;
      white-space: nowrap; text-align: right;
    }
    .detail-table th:first-child,
    .detail-table th:nth-child(2),
    .detail-table th:nth-child(3) { text-align: left; }
    .detail-table th:hover { color: var(--blue); }
    .detail-table th .si { margin-left: 0.2rem; opacity: 0.3; }
    .detail-table th.sorted .si { opacity: 1; color: var(--blue); }
    .detail-table td {
      padding: 0.65rem 0.8rem; font-size: 0.82rem;
      border-bottom: 1px solid rgba(42,58,78,0.3); text-align: right; white-space: nowrap;
    }
    .detail-table td:first-child,
    .detail-table td:nth-child(2),
    .detail-table td:nth-child(3) { text-align: left; }
    .detail-table tr:hover td { background: var(--bg-card-hover); }
    .ret-pos { color: var(--green); font-weight: 600; }
    .ret-neg { color: var(--red); font-weight: 600; }
    .hit-badge {
      display: inline-block; padding: 0.1rem 0.4rem;
      border-radius: 4px; font-size: 0.72rem; font-weight: 700;
    }
    .hit-1 { background: rgba(16,185,129,0.2); color: var(--green); }
    .hit-0 { background: rgba(239,68,68,0.15); color: var(--red); }
    .hit-na { color: var(--text-muted); }

    /* ページネーション */
    .pagination {
      display: flex; justify-content: center; align-items: center; gap: 0.5rem;
      padding: 1rem; border-top: 1px solid var(--border);
    }
    .page-btn {
      background: var(--bg-secondary); border: 1px solid var(--border);
      color: var(--text-secondary); padding: 0.35rem 0.7rem; border-radius: 6px;
      cursor: pointer; font-size: 0.8rem; transition: all 0.2s;
    }
    .page-btn:hover, .page-btn.active {
      background: var(--blue); border-color: var(--blue); color: #fff;
    }
    .page-info { color: var(--text-muted); font-size: 0.8rem; }

    /* フッター */
    .footer { text-align: center; padding: 2rem; color: var(--text-muted); font-size: 0.78rem; }

    @keyframes fadeIn { from { opacity: 0; transform: translateY(4px); } to { opacity: 1; transform: translateY(0); } }
    tbody tr { animation: fadeIn 0.2s ease; }

    /* 日付別集計テーブル */
    .date-summary-table { width: 100%; border-collapse: collapse; }
    .date-summary-table th {
      background: var(--bg-secondary);
      padding: 0.65rem 0.9rem;
      font-size: 0.73rem; font-weight: 700; color: var(--text-secondary);
      text-transform: uppercase; letter-spacing: 0.05em;
      border-bottom: 1px solid var(--border); white-space: nowrap; text-align: center;
    }
    .date-summary-table th:first-child { text-align: left; width: 120px; }
    .date-summary-table td {
      padding: 0.7rem 0.9rem; font-size: 0.88rem;
      border-bottom: 1px solid rgba(42,58,78,0.4);
      text-align: center; white-space: nowrap;
    }
    .date-summary-table td:first-child { text-align: left; font-weight: 600; font-family: monospace; color: var(--cyan); }
    .date-summary-table tr:hover td { background: var(--bg-card-hover); }
    .hm-cell {
      display: inline-block; border-radius: 6px;
      padding: 0.2rem 0.7rem; font-weight: 700; font-size: 0.85rem;
      min-width: 70px;
    }
    /* ミニ棒グラフ */
    .mini-chart-wrap {
      background: var(--bg-card); border: 1px solid var(--border);
      border-radius: 12px; padding: 1.2rem 1.5rem; margin-bottom: 2rem;
      overflow-x: auto;
    }
    .mini-chart {
      display: flex; align-items: flex-end; gap: 6px; height: 80px;
      min-width: max-content;
    }
    .mini-bar-col { display: flex; flex-direction: column; align-items: center; gap: 3px; }
    .mini-bar {
      width: 42px; border-radius: 3px 3px 0 0;
      transition: opacity 0.2s;
    }
    .mini-bar:hover { opacity: 0.75; }
    .mini-bar-label { font-size: 0.62rem; color: var(--text-muted); white-space: nowrap; }
    .mini-bar-val { font-size: 0.65rem; font-weight: 700; }

    @media (max-width: 768px) {
      .main { padding: 1rem; }
      .header-inner { flex-direction: column; gap: 0.7rem; align-items: flex-start; }
      .kpi-value { font-size: 1.4rem; }
    }
  </style>
</head>
<body>
"""


def _fmt_ret(val, na="—") -> str:
    """騰落率を色付きHTMLで返す。"""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return f'<span class="ret-na" style="color:var(--text-muted)">{na}</span>'
    cls = "ret-pos" if val >= 0 else "ret-neg"
    sign = "+" if val >= 0 else ""
    return f'<span class="{cls}">{sign}{val:.2f}%</span>'


def _fmt_hit(val) -> str:
    """達成フラグを色付きバッジで返す。"""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return '<span class="hit-na">—</span>'
    if val == 1:
        return '<span class="hit-badge hit-1">✓ 達成</span>'
    return '<span class="hit-badge hit-0">✗</span>'


def generate_report(
    csv_path: str = None,
    json_path: str = None,
) -> str:
    """
    バックテスト結果のHTMLダッシュボードを生成する。

    Args:
        csv_path: バックテスト結果CSVのパス（省略時は latest を使用）
        json_path: サマリーJSONのパス（省略時は latest を使用）

    Returns:
        生成された HTML ファイルのパス
    """
    if csv_path is None:
        csv_path = DEFAULT_CSV
    if json_path is None:
        json_path = DEFAULT_JSON

    if not os.path.exists(csv_path):
        print(f"❌ CSVが見つかりません: {csv_path}")
        print("   先に backtester.py を実行してください。")
        return ""
    if not os.path.exists(json_path):
        print(f"❌ サマリーJSONが見つかりません: {json_path}")
        return ""

    # ---- データ読み込み ----
    df = pd.read_csv(csv_path)
    with open(json_path, "r", encoding="utf-8") as f:
        summary = json.load(f)

    generated_at = summary.get("generated_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    hit_threshold = summary.get("hit_threshold_pct", 2.0)
    hit_col = f"hit_{hit_threshold:.0f}pct_1d"

    # 日付範囲を取得
    dates = sorted(df["date"].unique()) if "date" in df.columns else []
    date_from = dates[0] if dates else "—"
    date_to = dates[-1] if dates else "—"

    # ---- KPIカードHTML ----
    def _kpi(label, value, sub="", cls=""):
        return f"""
        <div class="kpi-card">
          <div class="kpi-label">{label}</div>
          <div class="kpi-value {cls}">{value}</div>
          {"<div class='kpi-sub'>" + sub + "</div>" if sub else ""}
        </div>"""

    avg_ret = summary.get("avg_ret_1d")
    win_rate = summary.get("win_rate_1d")
    hit_rate = summary.get("hit_rate_1d")

    avg_ret_str = f"{avg_ret:+.3f}%" if avg_ret is not None else "N/A"
    win_rate_str = f"{win_rate:.1f}%" if win_rate is not None else "N/A"
    hit_rate_str = f"{hit_rate:.1f}%" if hit_rate is not None else "N/A"

    avg_ret_cls = "green" if avg_ret and avg_ret >= 0 else ""

    kpi_html = "".join([
        _kpi("検証日数", f"{summary.get('n_dates', 0):,}日"),
        _kpi("総シグナル数", f"{summary.get('n_signals', 0):,}件",
             f"{summary.get('n_tickers', 0)}銘柄"),
        _kpi("翌日平均騰落率", avg_ret_str, "全シグナル平均", avg_ret_cls),
        _kpi("翌日勝率", win_rate_str, "プラスで終わった割合"),
        _kpi(f"翌日+{hit_threshold:.0f}%達成率", hit_rate_str,
             "狙い通りの爆発率", "yellow"),
    ])

    # ---- 日数別リターン統計 ----
    rs = summary.get("ret_stats_by_day", {})
    hr = summary.get("hit_rates_by_day", {})
    day_labels = {"ret_1d": "翌日（1日後）", "ret_2d": "2日後", "ret_3d": "3日後", "ret_5d": "5日後"}

    ret_stat_cards = []
    for col, label in day_labels.items():
        stat = rs.get(col, {})
        n_str = col.replace("ret_", "").replace("d", "")
        hit_r = hr.get(f"{n_str}d", None)
        hit_r_str = f"{hit_r:.1f}%" if hit_r is not None else "—"

        mean = stat.get("mean")
        mean_cls = "pos" if mean and mean >= 0 else "neg"
        mean_str = f"+{mean:.3f}%" if mean and mean >= 0 else (f"{mean:.3f}%" if mean else "—")

        card = f"""
        <div class="ret-stat-card">
          <div class="ret-stat-title">📅 {label}</div>
          <div class="ret-stat-row"><span class="k">平均リターン</span><span class="v {mean_cls}">{mean_str}</span></div>
          <div class="ret-stat-row"><span class="k">中央値</span><span class="v">{stat.get('median', '—')}</span></div>
          <div class="ret-stat-row"><span class="k">勝率</span><span class="v">{stat.get('win_rate', '—')}%</span></div>
          <div class="ret-stat-row"><span class="k">最大</span><span class="v pos">+{stat.get('max', '—')}%</span></div>
          <div class="ret-stat-row"><span class="k">最小</span><span class="v neg">{stat.get('min', '—')}%</span></div>
          <div class="ret-stat-row"><span class="k">+{hit_threshold:.0f}%達成率</span><span class="v">{hit_r_str}</span></div>
        </div>"""
        ret_stat_cards.append(card)

    ret_stats_html = "\n".join(ret_stat_cards)

    # ---- 日付別集計ビュー ----
    def _heatmap_cell(value: float, low: float, high: float, suffix: str = "%") -> str:
        """
        値をヒートマップカラーで色付けたセルを返す。

        low値のとき赤、high値のとき緑となるからのグラデーション。
        """
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return f'<span class="hm-cell" style="background:rgba(255,255,255,0.04);color:var(--text-muted)">—</span>'
        span = high - low if high != low else 1.0
        ratio = max(0.0, min(1.0, (value - low) / span))
        # 0.0=赤系 (239,68,68)  0.5=黄 (251,191,36)  1.0=緑 (16,185,129)
        if ratio < 0.5:
            r2 = ratio * 2  # 0→1
            r = int(239 + (251 - 239) * r2)
            g = int(68  + (191 - 68) * r2)
            b = int(68  + (36  - 68) * r2)
        else:
            r2 = (ratio - 0.5) * 2  # 0→1
            r = int(251 + (16  - 251) * r2)
            g = int(191 + (185 - 191) * r2)
            b = int(36  + (129 - 36) * r2)
        bg  = f"rgba({r},{g},{b},0.22)"
        col = f"rgb({r},{g},{b})"
        sign = "+" if value > 0 else ""
        return f'<span class="hm-cell" style="background:{bg};color:{col}">{sign}{value:.1f}{suffix}</span>'

    # 日付から一度集計する
    date_groups = df.groupby("date")
    date_rows_data = []
    for date_val, grp in date_groups:
        n = len(grp)
        # ret_1d 勝率
        r1_valid = grp["ret_1d"].dropna()
        win1 = (r1_valid > 0).sum() / len(r1_valid) * 100 if len(r1_valid) > 0 else float("nan")
        avg1 = r1_valid.mean() if len(r1_valid) > 0 else float("nan")
        # ret_2d 勝率
        r2_valid = grp["ret_2d"].dropna()
        win2 = (r2_valid > 0).sum() / len(r2_valid) * 100 if len(r2_valid) > 0 else float("nan")
        avg2 = r2_valid.mean() if len(r2_valid) > 0 else float("nan")
        # hit 達成率
        if hit_col in grp.columns:
            hit_valid = grp[hit_col].dropna()
            hit_r_day = hit_valid.mean() * 100 if len(hit_valid) > 0 else float("nan")
        else:
            hit_r_day = float("nan")
        date_rows_data.append({
            "date": str(date_val),
            "n": n,
            "win1": win1, "avg1": avg1,
            "win2": win2, "avg2": avg2,
            "hit": hit_r_day,
        })

    # ヒートマップ傾斉の基準値を決める
    all_win1 = [d["win1"] for d in date_rows_data if not math.isnan(d["win1"])]
    all_avg1 = [d["avg1"] for d in date_rows_data if not math.isnan(d["avg1"])]
    all_win2 = [d["win2"] for d in date_rows_data if not math.isnan(d["win2"])]
    all_avg2 = [d["avg2"] for d in date_rows_data if not math.isnan(d["avg2"])]
    all_hit  = [d["hit"]  for d in date_rows_data if not math.isnan(d["hit"])]

    win_low, win_high = (min(all_win1), max(all_win1)) if all_win1 else (0, 100)
    avg_low, avg_high = (min(all_avg1 + all_avg2), max(all_avg1 + all_avg2)) if all_avg1 else (-5, 5)
    hit_low, hit_high = (min(all_hit), max(all_hit)) if all_hit else (0, 100)

    # ミニ棒グラフHTML
    max_abs_avg1 = max(abs(avg_low), abs(avg_high)) if all_avg1 else 5
    mini_bars_html = ""
    for d in date_rows_data:
        v = d["avg1"]
        if math.isnan(v):
            continue
        height_px = int(abs(v) / max_abs_avg1 * 64) if max_abs_avg1 > 0 else 4
        height_px = max(height_px, 4)
        color = "#10b981" if v >= 0 else "#ef4444"
        sign = "+" if v >= 0 else ""
        mini_bars_html += (
            f"<div class='mini-bar-col'>"
            f"  <span class='mini-bar-val' style='color:{color}; margin-bottom:{64-height_px}px'>{sign}{v:.1f}%</span>"
            f"  <div class='mini-bar' style='height:{height_px}px;background:{color};' title='{d['date']} 翌日平均: {sign}{v:.1f}%'></div>"
            f"  <span class='mini-bar-label'>{d['date'][5:]}</span>"
            f"</div>"
        )

    # 日付別集計テーブルHTML
    date_table_rows = ""
    for d in date_rows_data:
        date_table_rows += (
            f"<tr>"
            f"<td>{d['date']}</td>"
            f"<td>{d['n']}件</td>"
            f"<td>{_heatmap_cell(d['win1'], win_low, win_high)}</td>"
            f"<td>{_heatmap_cell(d['avg1'], avg_low, avg_high)}</td>"
            f"<td>{_heatmap_cell(d['win2'], win_low, win_high)}</td>"
            f"<td>{_heatmap_cell(d['avg2'], avg_low, avg_high)}</td>"
            f"<td>{_heatmap_cell(d['hit'], hit_low, hit_high)}</td>"
            f"</tr>"
        )

    date_summary_html = f"""
    <div class="mini-chart-wrap">
      <div style="font-size:0.78rem;color:var(--text-secondary);margin-bottom:0.7rem;font-weight:600;">
        📊 日付別 翌日平均騰落率（棒グラフ）
      </div>
      <div class="mini-chart">{mini_bars_html}</div>
    </div>
    <div class="table-wrap" style="margin-bottom:2rem">
      <table class="date-summary-table">
        <thead>
          <tr>
            <th>日付</th>
            <th>候補数</th>
            <th>翌日 勝率</th>
            <th>翌日 平均</th>
            <th>2日後 勝率</th>
            <th>2日後 平均</th>
            <th>+{hit_threshold:.0f}% 達成率</th>
          </tr>
        </thead>
        <tbody>{date_table_rows}</tbody>
      </table>
    </div>"""

    # ---- 上位銘柄テーブル ----
    top_tickers = summary.get("top_tickers", [])
    max_hits = top_tickers[0]["hit_count"] if top_tickers else 1

    top_rows = []
    for i, t in enumerate(top_tickers[:20], 1):
        rank_cls = "top3" if i <= 3 else ""
        bar_pct = t["hit_count"] / max_hits * 100
        avg_r = t.get("avg_ret", 0)
        avg_cls = "ret-pos" if avg_r >= 0 else "ret-neg"
        sign = "+" if avg_r >= 0 else ""
        top_rows.append(f"""
        <tr>
          <td class="rank {rank_cls}">{i}</td>
          <td class="code-cell">{t['ticker']}</td>
          <td>{t.get('name', '')}</td>
          <td>
            <div class="hit-bar-wrap">
              <span style="font-weight:700;color:var(--green)">{t['hit_count']}</span>
              <div class="hit-bar" style="width:{bar_pct:.0f}px"></div>
            </div>
          </td>
          <td style="text-align:right">{t.get('signal_count', '—')}</td>
          <td style="text-align:right" class="{avg_cls}">{sign}{avg_r:.3f}%</td>
        </tr>""")

    top_table_html = f"""
    <table class="top-ticker-table">
      <thead>
        <tr>
          <th style="width:40px">#</th>
          <th>コード</th>
          <th>銘柄名</th>
          <th>+{hit_threshold:.0f}%達成回数</th>
          <th style="text-align:right">シグナル総数</th>
          <th style="text-align:right">平均翌日リターン</th>
        </tr>
      </thead>
      <tbody>
        {"".join(top_rows) if top_rows else '<tr><td colspan="6" style="text-align:center;color:var(--text-muted);padding:2rem">データなし</td></tr>'}
      </tbody>
    </table>"""

    # ---- 詳細テーブル（全シグナル行） ----
    detail_rows = []
    for _, row in df.iterrows():
        ret1 = row.get("ret_1d")
        ret2 = row.get("ret_2d")
        ret3 = row.get("ret_3d")
        ret5 = row.get("ret_5d")
        hit1 = row.get(hit_col)

        # NaN → None 変換
        def _val(v):
            if v is None:
                return None
            try:
                return None if math.isnan(float(v)) else float(v)
            except (TypeError, ValueError):
                return None

        ret1, ret2, ret3, ret5 = _val(ret1), _val(ret2), _val(ret3), _val(ret5)
        hit1 = _val(hit1)

        detail_rows.append(f"""
        <tr>
          <td>{row.get('date', '—')}</td>
          <td class="code-cell">{row.get('ticker', '—')}</td>
          <td>{row.get('name', '—')}</td>
          <td>{row.get('close', '—'):,.1f}</td>
          <td>{row.get('volume', 0):,.0f}</td>
          <td>{row.get('volume_ratio', '—'):.2f}x</td>
          <td>{_fmt_ret(ret1)}</td>
          <td>{_fmt_ret(ret2)}</td>
          <td>{_fmt_ret(ret3)}</td>
          <td>{_fmt_ret(ret5)}</td>
          <td>{_fmt_hit(hit1)}</td>
        </tr>""")

    detail_table_html = f"""
    <table class="detail-table" id="detailTable">
      <thead>
        <tr>
          <th data-sort="date" data-type="string" style="text-align:left">日付 <span class="si">⇅</span></th>
          <th data-sort="ticker" data-type="number" style="text-align:left">コード <span class="si">⇅</span></th>
          <th data-sort="name" data-type="string" style="text-align:left">銘柄名 <span class="si">⇅</span></th>
          <th data-sort="close" data-type="number">終値 <span class="si">⇅</span></th>
          <th data-sort="volume" data-type="number">出来高 <span class="si">⇅</span></th>
          <th data-sort="vol_ratio" data-type="number">出来高比 <span class="si">⇅</span></th>
          <th data-sort="ret1" data-type="number">翌日 <span class="si">⇅</span></th>
          <th data-sort="ret2" data-type="number">2日後 <span class="si">⇅</span></th>
          <th data-sort="ret3" data-type="number">3日後 <span class="si">⇅</span></th>
          <th data-sort="ret5" data-type="number">5日後 <span class="si">⇅</span></th>
          <th data-sort="hit1" data-type="number">+{hit_threshold:.0f}%達成 <span class="si">⇅</span></th>
        </tr>
      </thead>
      <tbody id="detailBody">
        {"".join(detail_rows)}
      </tbody>
    </table>"""

    # ---- 全体HTMLを組み立て ----
    html = HTML_HEAD + f"""
  <header class="header">
    <div class="header-inner">
      <h1>🧪 バックテスト結果ダッシュボード</h1>
      <div class="header-right">
        <button class="btn-link" onclick="window.open('{REPORT_L1_REL}', '_blank')">← レベル1 スクリーニング結果</button>
        <span class="badge">LEVEL 1 検証</span>
      </div>
    </div>
  </header>

  <main class="main">

    <!-- 条件バー -->
    <div class="condition-bar">
      <span class="label">検証期間</span>
      <span class="val">{date_from} 〜 {date_to}</span>
      <span class="cond-sep">|</span>
      <span class="label">スクリーニング条件</span>
      <span class="val">SMA5 &gt; SMA20 &gt; SMA60 &nbsp;&amp;&amp;&nbsp; 出来高 ≥ {summary.get('conditions', {}).get('min_volume', 500000) if 'conditions' in summary else 500000:,}株</span>
      <span class="cond-sep">|</span>
      <span class="label">達成閾値</span>
      <span class="val">+{hit_threshold:.0f}%</span>
      <span class="cond-sep">|</span>
      <span class="label">生成日時</span>
      <span class="val">{generated_at}</span>
    </div>

    <!-- KPIカード -->
    <div class="kpi-grid">
      {kpi_html}
    </div>

    <!-- 日付別集計 -->
    <div class="section-title">📅 日付別パーセンテージ一覧</div>
    {date_summary_html}

    <!-- 日数別リターン統計 -->
    <div class="section-title">📈 日数別リターン統計</div>
    <div class="ret-stats-grid">
      {ret_stats_html}
    </div>

    <!-- 上位銘柄 -->
    <div class="section-title">🏆 上位銘柄ランキング（翌日+{hit_threshold:.0f}%達成回数）</div>
    <div class="table-wrap" style="margin-bottom:2rem">
      {top_table_html}
    </div>

    <!-- 詳細テーブル -->
    <div class="table-wrap">
      <div class="table-toolbar">
        <h2>📋 全シグナル詳細（{len(df):,}件）</h2>
        <div class="toolbar-right">
          <select class="filter-select" id="hitFilter">
            <option value="all">すべて表示</option>
            <option value="hit">達成のみ</option>
            <option value="miss">未達成のみ</option>
            <option value="nodata">データなし</option>
          </select>
          <input type="text" class="search-box" id="searchBox" placeholder="🔍 銘柄・日付で検索...">
        </div>
      </div>
      {detail_table_html}
      <div class="pagination" id="pagination"></div>
    </div>

  </main>

  <footer class="footer">
    <p>⚠️ 本ツールは投資助言を目的としたものではありません。投資判断は自己責任でお願いいたします。</p>
    <p style="margin-top:0.4rem">Generated at {generated_at}</p>
  </footer>

  <script>
    // ============================================================
    // 全行データをキャッシュ（フィルタ・ページネーション用）
    // ============================================================
    const allRows = Array.from(document.querySelectorAll('#detailBody tr'));
    const PAGE_SIZE = 100;
    let filteredRows = [...allRows];
    let currentPage = 1;
    let sortState = {{}};

    function applyFiltersAndRender() {{
      const query = document.getElementById('searchBox').value.toLowerCase();
      const hitFilter = document.getElementById('hitFilter').value;

      filteredRows = allRows.filter(row => {{
        // テキスト検索
        if (query && !row.textContent.toLowerCase().includes(query)) return false;

        // 達成フラグフィルタ
        if (hitFilter !== 'all') {{
          const hitCell = row.cells[10];
          const text = hitCell ? hitCell.textContent.trim() : '';
          if (hitFilter === 'hit' && !text.includes('達成')) return false;
          if (hitFilter === 'miss' && !text.includes('✗')) return false;
          if (hitFilter === 'nodata' && text !== '—') return false;
        }}

        return true;
      }});

      currentPage = 1;
      renderPage();
    }}

    function renderPage() {{
      const tbody = document.getElementById('detailBody');
      const start = (currentPage - 1) * PAGE_SIZE;
      const end = start + PAGE_SIZE;

      allRows.forEach(r => r.style.display = 'none');
      filteredRows.slice(start, end).forEach(r => r.style.display = '');

      renderPagination();
    }}

    function renderPagination() {{
      const total = filteredRows.length;
      const totalPages = Math.ceil(total / PAGE_SIZE);
      const pg = document.getElementById('pagination');

      if (totalPages <= 1) {{ pg.innerHTML = `<span class="page-info">全 ${{total}}件</span>`; return; }}

      let html = `<span class="page-info">${{(currentPage-1)*PAGE_SIZE+1}}–${{Math.min(currentPage*PAGE_SIZE,total)}} / ${{total}}件</span>`;

      // 前へ
      html += `<button class="page-btn" onclick="goPage(${{currentPage-1}})" ${{currentPage===1?'disabled':''}}>‹</button>`;

      // ページ番号
      const pages = getPageNumbers(currentPage, totalPages);
      pages.forEach(p => {{
        if (p === '...') html += `<span class="page-info">…</span>`;
        else html += `<button class="page-btn ${{p===currentPage?'active':''}}" onclick="goPage(${{p}})">${{p}}</button>`;
      }});

      // 次へ
      html += `<button class="page-btn" onclick="goPage(${{currentPage+1}})" ${{currentPage===totalPages?'disabled':''}}>›</button>`;

      pg.innerHTML = html;
    }}

    function getPageNumbers(current, total) {{
      if (total <= 7) return Array.from({{length: total}}, (_, i) => i + 1);
      const pages = [];
      if (current <= 4) {{
        for (let i = 1; i <= 5; i++) pages.push(i);
        pages.push('...'); pages.push(total);
      }} else if (current >= total - 3) {{
        pages.push(1); pages.push('...');
        for (let i = total - 4; i <= total; i++) pages.push(i);
      }} else {{
        pages.push(1); pages.push('...');
        for (let i = current - 1; i <= current + 1; i++) pages.push(i);
        pages.push('...'); pages.push(total);
      }}
      return pages;
    }}

    function goPage(p) {{
      const total = Math.ceil(filteredRows.length / PAGE_SIZE);
      if (p < 1 || p > total) return;
      currentPage = p;
      renderPage();
      window.scrollTo({{top: document.querySelector('.table-wrap').offsetTop - 80, behavior: 'smooth'}});
    }}

    // ============================================================
    // イベント
    // ============================================================
    document.getElementById('searchBox').addEventListener('input', applyFiltersAndRender);
    document.getElementById('hitFilter').addEventListener('change', applyFiltersAndRender);

    // ソート
    document.querySelectorAll('.detail-table th[data-sort]').forEach(th => {{
      th.addEventListener('click', function() {{
        const key = this.dataset.sort;
        const type = this.dataset.type || 'string';
        sortState[key] = sortState[key] === 'asc' ? 'desc' : 'asc';
        const dir = sortState[key];

        document.querySelectorAll('.detail-table th').forEach(h => h.classList.remove('sorted'));
        this.classList.add('sorted');
        this.querySelector('.si').textContent = dir === 'asc' ? '▲' : '▼';

        const colIndex = Array.from(this.parentElement.children).indexOf(this);

        allRows.sort((a, b) => {{
          const va = a.cells[colIndex]?.textContent.trim() || '';
          const vb = b.cells[colIndex]?.textContent.trim() || '';

          if (type === 'number') {{
            const na = parseFloat(va.replace(/[^0-9.\-]/g, '')) || -Infinity;
            const nb = parseFloat(vb.replace(/[^0-9.\-]/g, '')) || -Infinity;
            return dir === 'asc' ? na - nb : nb - na;
          }}
          return dir === 'asc' ? va.localeCompare(vb, 'ja') : vb.localeCompare(va, 'ja');
        }});

        const tbody = document.getElementById('detailBody');
        allRows.forEach(r => tbody.appendChild(r));

        applyFiltersAndRender();
      }});
    }});

    // 初期描画
    applyFiltersAndRender();
  </script>
</body>
</html>
"""

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"✅ バックテストレポートを生成しました: {OUTPUT_HTML}")
    return OUTPUT_HTML


# =============================================================
# エントリーポイント
# =============================================================
if __name__ == "__main__":
    path = generate_report()
    if path:
        print("\n🌐 ブラウザで開きます...")
        import webbrowser
        webbrowser.open(f"file://{path}")
