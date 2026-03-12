"""
🔄 底打ち反転スクリーナー

長期下落→底打ち→上昇転換しそうな日本株を
TradingView Screener APIでリアルタイムにスクリーニングする。

既存の SMA5>SMA20>SMA60 スクリーニングとは根本的に条件が異なるため、
独立したページとして実装。
"""

import io
import json
import math
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import streamlit as st

# TradingView Screener API
try:
    from tradingview_screener import Query, col as tv_col
    TV_API_AVAILABLE = True
except ImportError:
    TV_API_AVAILABLE = False


# =====================================================
# ページ設定
# =====================================================
st.set_page_config(
    page_title="🔄 底打ち反転スクリーナー",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded",
)


# =====================================================
# 定数
# =====================================================
SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "results")
JPX_CSV_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"


# =====================================================
# カスタムCSS
# =====================================================
st.markdown("""
<style>
  html, body, [class*="css"] {
    font-family: 'Inter', 'Noto Sans JP', -apple-system, sans-serif;
  }
  .stApp {
    background-color: #f0f2f6 !important;
  }
  section[data-testid="stSidebar"] {
    background-color: #e4e7ed !important;
  }
  [data-testid="metric-container"] {
    background: #1a2332;
    border: 1px solid #2a3a4e;
    border-radius: 10px;
    padding: 1rem !important;
  }
  .app-header {
    background: linear-gradient(135deg, rgba(168,85,247,0.15), rgba(59,130,246,0.08));
    border: 1px solid rgba(168,85,247,0.25);
    border-radius: 12px;
    padding: 1.2rem 1.5rem;
    margin-bottom: 1.5rem;
  }
  .app-header h1 { margin: 0; font-size: 1.6rem; }
  .app-header p { margin: 0.3rem 0 0; color: #8899aa; font-size: 0.9rem; }
  .badge-reversal {
    display: inline-block;
    background: rgba(168,85,247,0.2);
    border: 1px solid rgba(168,85,247,0.4);
    color: #a855f7;
    padding: 0.15rem 0.6rem;
    border-radius: 6px;
    font-size: 0.75rem;
    font-weight: 700;
    margin-left: 0.5rem;
  }
  .condition-box {
    background: rgba(168,85,247,0.06);
    border: 1px solid rgba(168,85,247,0.2);
    border-radius: 10px;
    padding: 1rem 1.2rem;
    margin-bottom: 1rem;
    font-size: 0.88rem;
    line-height: 1.6;
  }
  .condition-box h4 { margin: 0 0 0.4rem; font-size: 0.95rem; }
</style>
""", unsafe_allow_html=True)

# =====================================================
# ヘッダー
# =====================================================
st.markdown("""
<div class="app-header">
  <h1>🔄 底打ち反転スクリーナー <span class="badge-reversal">REVERSAL</span></h1>
  <p>長期下落→底打ち→上昇転換しそうな銘柄をリアルタイムでスクリーニング（TradingView API）</p>
</div>
""", unsafe_allow_html=True)

# 条件の説明
st.markdown("""
<div class="condition-box">
  <h4>📋 スクリーニング条件</h4>
  <strong>📉 長期下落:</strong> 3ヶ月パフォーマンス < 0 かつ 終値 < SMA200<br>
  <strong>📊 底打ちシグナル:</strong> RSI 30〜50 かつ MACD > Signal（ゴールデンクロス）<br>
  <strong>📈 短期上昇転換:</strong> SMA5 > SMA20 かつ 終値 > SMA5<br>
  <strong>📦 出来高:</strong> ≥ 50万株
</div>
""", unsafe_allow_html=True)


# =====================================================
# JPX銘柄名マッピング（キャッシュ付き）
# =====================================================
@st.cache_data(ttl=86400, show_spinner=False)
def _fetch_jpx_name_map() -> Dict[int, str]:
    """JPXの上場銘柄一覧から {銘柄コード: 日本語銘柄名} のマッピングを取得する。"""
    try:
        resp = requests.get(JPX_CSV_URL, timeout=30)
        resp.raise_for_status()
        df = pd.read_excel(io.BytesIO(resp.content))
        df = df.rename(columns={"コード": "code", "銘柄名": "name"})
        df = df[pd.to_numeric(df["code"], errors="coerce").notna()]
        df["code"] = df["code"].astype(int)
        return dict(zip(df["code"], df["name"]))
    except Exception:
        return {}


def _safe_float(val) -> Optional[float]:
    """NaN安全な浮動小数点変換"""
    if val is None:
        return None
    try:
        f = float(val)
        if f != f:
            return None
        return f
    except (ValueError, TypeError):
        return None


# =====================================================
# サイドバー（条件カスタマイズ）
# =====================================================
with st.sidebar:
    st.markdown("## ⚙️ スクリーニング設定")
    st.markdown("---")

    st.markdown("### 📉 長期下落条件")
    perf_3m_threshold = st.slider(
        "3ヶ月パフォーマンス上限（%）",
        min_value=-50.0,
        max_value=0.0,
        value=0.0,
        step=5.0,
        key="perf_3m_thr",
        help="これ以下の3ヶ月パフォーマンスの銘柄のみ表示（0 = マイナスならOK）",
    )

    st.markdown("---")
    st.markdown("### 📊 テクニカル条件")

    rsi_range = st.slider(
        "RSI範囲",
        min_value=10.0,
        max_value=70.0,
        value=(30.0, 50.0),
        step=5.0,
        key="rsi_range",
        help="底打ち判定のRSI範囲（デフォルト: 30〜50）",
    )

    st.markdown("---")
    st.markdown("### 📦 出来高条件")

    min_volume = st.number_input(
        "最低出来高（株）",
        min_value=100_000,
        max_value=10_000_000,
        value=500_000,
        step=100_000,
        format="%d",
        key="br_min_vol",
    )

    st.markdown("---")

    st.info(
        f"**適用条件**\n"
        f"- Perf.3M < {perf_3m_threshold:.0f}%\n"
        f"- Close < SMA200\n"
        f"- RSI: {rsi_range[0]:.0f}〜{rsi_range[1]:.0f}\n"
        f"- MACD > Signal\n"
        f"- SMA5 > SMA20, Close > SMA5\n"
        f"- 出来高 ≥ {min_volume:,}株"
    )

    st.markdown("---")

    # 実行ボタン
    run_button = st.button(
        "🔍 底打ち反転スクリーニング実行",
        type="primary",
        use_container_width=True,
    )


# =====================================================
# スクリーニング実行関数
# =====================================================
@st.cache_data(ttl=300, show_spinner=False)
def run_bottom_reversal(
    perf_threshold: float,
    rsi_min: float,
    rsi_max: float,
    min_vol: int,
) -> List[Dict[str, Any]]:
    """
    TradingView Screener APIで底打ち反転銘柄をスクリーニングする。

    Args:
        perf_threshold: 3ヶ月パフォーマンスの上限（%単位, 例: 0 or -10）
        rsi_min: RSI下限
        rsi_max: RSI上限
        min_vol: 最低出来高

    Returns:
        候補銘柄のリスト
    """
    if not TV_API_AVAILABLE:
        return []

    # TradingView APIのPerf.3Mはパーセント値で返される（例: -18.1 = -18.1%）
    # perf_thresholdもパーセント値で指定
    (count, df) = (Query()
        .set_markets('japan')
        .select(
            'name', 'description', 'close', 'volume',
            'SMA5', 'SMA20', 'SMA60', 'SMA200',
            'RSI', 'MACD.macd', 'MACD.signal',
            'Perf.1M', 'Perf.3M', 'Perf.6M',
            'High.3M', 'Low.3M', 'High.6M', 'Low.6M',
            'relative_volume_10d_calc',
            'change',
        )
        .where(
            # 長期下落の証拠
            tv_col('Perf.3M') < perf_threshold,
            tv_col('close') < tv_col('SMA200'),
            # 底打ちシグナル
            tv_col('RSI') > rsi_min,
            tv_col('RSI') < rsi_max,
            tv_col('MACD.macd') > tv_col('MACD.signal'),
            # 短期上昇転換
            tv_col('SMA5') > tv_col('SMA20'),
            tv_col('close') > tv_col('SMA5'),
            # 出来高
            tv_col('volume') > min_vol,
        )
        .order_by('volume', ascending=False)
        .limit(500)
        .get_scanner_data())

    if df.empty:
        return []

    # JPX銘柄名マッピング
    jpx_names = _fetch_jpx_name_map()
    today_str = datetime.now().strftime("%Y-%m-%d")
    candidates = []

    for _, row in df.iterrows():
        ticker_str = str(row.get("ticker", ""))
        code_str = ticker_str.split(":")[-1] if ":" in ticker_str else ticker_str
        try:
            code = int(code_str)
        except ValueError:
            continue

        close_val = float(row.get("close", 0))
        sma5_val = float(row.get("SMA5", 0))
        sma20_val = float(row.get("SMA20", 0))
        sma60_val = float(row.get("SMA60", 0))
        sma200_val = float(row.get("SMA200", 0))
        volume_val = int(row.get("volume", 0))
        rsi_val = float(row.get("RSI", 0))
        macd_val = float(row.get("MACD.macd", 0))
        macd_signal_val = float(row.get("MACD.signal", 0))
        perf_1m = _safe_float(row.get("Perf.1M"))
        perf_3m = _safe_float(row.get("Perf.3M"))
        perf_6m = _safe_float(row.get("Perf.6M"))
        high_3m = _safe_float(row.get("High.3M"))
        low_3m = _safe_float(row.get("Low.3M"))
        rel_vol = _safe_float(row.get("relative_volume_10d_calc"))
        change_pct = _safe_float(row.get("change"))

        # 底からの反発度（3ヶ月レンジ内の位置 0〜100%）
        reversal_position = None
        if high_3m is not None and low_3m is not None and high_3m > low_3m:
            reversal_position = round(
                (close_val - low_3m) / (high_3m - low_3m) * 100, 1
            )

        # SMA200からの乖離率（%）
        sma200_deviation = round(
            (close_val - sma200_val) / sma200_val * 100, 2
        ) if sma200_val > 0 else None

        candidate = {
            "code": code,
            "name": jpx_names.get(code, str(row.get("description", row.get("name", "")))),
            "close": round(close_val, 1),
            "sma5": round(sma5_val, 1),
            "sma20": round(sma20_val, 1),
            "sma60": round(sma60_val, 1),
            "sma200": round(sma200_val, 1),
            "sma200_dev": sma200_deviation,
            "rsi": round(rsi_val, 1),
            "macd": round(macd_val, 4),
            "macd_signal": round(macd_signal_val, 4),
            "perf_1m": round(perf_1m, 2) if perf_1m is not None else None,
            "perf_3m": round(perf_3m, 2) if perf_3m is not None else None,
            "perf_6m": round(perf_6m, 2) if perf_6m is not None else None,
            "reversal_pos": reversal_position,
            "volume": volume_val,
            "rel_vol": round(rel_vol, 2) if rel_vol is not None else None,
            "change": round(change_pct, 2) if change_pct is not None else None,
        }
        candidates.append(candidate)

    candidates.sort(key=lambda x: x["volume"], reverse=True)
    return candidates


# =====================================================
# 結果テーブルのHTML生成
# =====================================================
def _build_result_table(candidates: List[Dict[str, Any]]) -> str:
    """候補銘柄リストからスタイル付きHTMLテーブルを生成する。"""

    # ヘッダー定義
    columns = [
        ("銘柄コード", "left"),
        ("銘柄名", "left"),
        ("終値", "right"),
        ("RSI", "right"),
        ("MACD", "right"),
        ("Signal", "right"),
        ("3M騰落", "right"),
        ("6M騰落", "right"),
        ("SMA200乖離", "right"),
        ("反発度", "right"),
        ("当日変動", "right"),
        ("出来高", "right"),
        ("相対出来高", "right"),
    ]

    # ヘッダーHTML
    th_cells = ""
    for name, align in columns:
        th_cells += f'<th style="text-align:{align}">{name}</th>'

    # 行HTML
    rows_html = ""
    tv_base = "https://jp.tradingview.com/chart/M3vhlCeS/?symbol=TSE%3A"

    for c in candidates:
        code = c["code"]
        tv_url = f"{tv_base}{code}"

        # 銘柄コードセル
        code_cell = (
            f'<td class="code-cell">'
            f'<a class="code-link" href="{tv_url}" target="_blank">{code}</a>'
            f'</td>'
        )

        # 銘柄名セル
        name_cell = f'<td class="name-cell">{c["name"]}</td>'

        # 終値
        close_cell = f'<td>{c["close"]:,.1f}</td>'

        # RSI（色分け）
        rsi = c["rsi"]
        if rsi <= 35:
            rsi_style = "background:rgba(16,185,129,0.2);color:#10b981;font-weight:bold"
        elif rsi <= 45:
            rsi_style = "background:rgba(16,185,129,0.1);color:#10b981"
        else:
            rsi_style = ""
        rsi_cell = f'<td style="{rsi_style}">{rsi:.1f}</td>'

        # MACD / Signal
        macd_cell = f'<td>{c["macd"]:.4f}</td>'
        signal_cell = f'<td>{c["macd_signal"]:.4f}</td>'

        # パフォーマンス（色分け）
        def _perf_cell(val):
            if val is None:
                return '<td>—</td>'
            intensity = min(int(abs(val) / 30 * 70), 70)
            if val > 0:
                style = f"background:rgba(16,185,129,0.{intensity:02d});color:#10b981;font-weight:bold"
            else:
                style = f"background:rgba(239,68,68,0.{intensity:02d});color:#ef4444;font-weight:bold"
            return f'<td style="{style}">{val:+.1f}%</td>'

        perf_3m_cell = _perf_cell(c["perf_3m"])
        perf_6m_cell = _perf_cell(c["perf_6m"])

        # SMA200乖離
        dev = c.get("sma200_dev")
        if dev is not None:
            dev_intensity = min(int(abs(dev) / 30 * 70), 70)
            dev_style = f"background:rgba(239,68,68,0.{dev_intensity:02d});color:#ef4444;font-weight:bold"
            dev_cell = f'<td style="{dev_style}">{dev:+.1f}%</td>'
        else:
            dev_cell = '<td>—</td>'

        # 反発度（重要指標）
        rev = c.get("reversal_pos")
        if rev is not None:
            if rev < 25:
                rev_style = "background:rgba(168,85,247,0.2);color:#a855f7;font-weight:bold"
            elif rev < 50:
                rev_style = "background:rgba(168,85,247,0.12);color:#a855f7"
            else:
                rev_style = "color:#64748b"
            rev_cell = f'<td style="{rev_style}">{rev:.1f}%</td>'
        else:
            rev_cell = '<td>—</td>'

        # 当日変動
        chg = c.get("change")
        if chg is not None:
            chg_intensity = min(int(abs(chg) / 5 * 80), 70)
            if chg > 0:
                chg_style = f"background:rgba(16,185,129,0.{chg_intensity:02d});color:#10b981;font-weight:bold"
            elif chg < 0:
                chg_style = f"background:rgba(239,68,68,0.{chg_intensity:02d});color:#ef4444;font-weight:bold"
            else:
                chg_style = ""
            chg_cell = f'<td style="{chg_style}">{chg:+.2f}%</td>'
        else:
            chg_cell = '<td>—</td>'

        # 出来高
        vol_cell = f'<td>{c["volume"]:,}</td>'

        # 相対出来高
        rel = c.get("rel_vol")
        if rel is not None:
            rel_style = "font-weight:bold;color:#f59e0b" if rel >= 1.5 else ""
            rel_cell = f'<td style="{rel_style}">{rel:.2f}</td>'
        else:
            rel_cell = '<td>—</td>'

        cells = (code_cell + name_cell + close_cell + rsi_cell +
                 macd_cell + signal_cell +
                 perf_3m_cell + perf_6m_cell + dev_cell + rev_cell +
                 chg_cell + vol_cell + rel_cell)
        rows_html += f"<tr>{cells}</tr>\n"

    n_rows = len(candidates)
    table_height = min(max(n_rows * 42 + 60, 200), 700)

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,'Noto Sans JP',sans-serif;background:transparent;overflow-x:auto}}
table{{border-collapse:collapse;width:100%;font-size:12px}}
th{{
  position:sticky;top:0;z-index:10;
  background:#1a2332;color:#8899aa;font-weight:600;
  padding:8px 10px;border-bottom:1px solid #2a3a4e;
  white-space:nowrap;cursor:pointer;user-select:none
}}
th:hover{{background:#1e2d42!important}}
td{{
  padding:7px 10px;text-align:right;border-bottom:1px solid rgba(255,255,255,0.04);
  color:#111;white-space:nowrap
}}
td.name-cell{{text-align:left;color:#111;max-width:180px;overflow:hidden;text-overflow:ellipsis}}
td.code-cell{{text-align:left}}
tr:hover td{{background:rgba(255,255,255,0.03)}}
.code-link{{
  display:inline-block;
  color:#a855f7;font-weight:700;text-decoration:none;
  background:rgba(168,85,247,0.1);
  border:1px solid rgba(168,85,247,0.4);
  border-radius:12px;padding:2px 10px;
  transition:background .15s,border-color .15s
}}
.code-link:hover{{background:rgba(168,85,247,0.25);border-color:#a855f7}}
th[data-sort="asc"]::after{{content:" ▲";font-size:9px;color:#a855f7}}
th[data-sort="desc"]::after{{content:" ▼";font-size:9px;color:#a855f7}}
.tbl-wrap{{height:{table_height}px;overflow-y:auto;overflow-x:auto}}
</style>
</head>
<body>
<div class="tbl-wrap">
  <table>
    <thead><tr>{th_cells}</tr></thead>
    <tbody>{rows_html}</tbody>
  </table>
</div>
<script>
(function(){{
  const table = document.querySelector('table');
  const tbody = table.querySelector('tbody');
  const ths = table.querySelectorAll('th');
  let sortColIdx = -1, sortAsc = true;
  ths.forEach(function(th, i){{
    th.addEventListener('click', function(){{
      if(sortColIdx === i){{ sortAsc = !sortAsc; }}
      else {{ sortColIdx = i; sortAsc = true; }}
      ths.forEach(function(t){{ t.dataset.sort = ''; }});
      th.dataset.sort = sortAsc ? 'asc' : 'desc';
      const rows = Array.from(tbody.querySelectorAll('tr'));
      rows.sort(function(a, b){{
        const aRaw = (a.cells[i]?.innerText || '').trim();
        const bRaw = (b.cells[i]?.innerText || '').trim();
        const aNum = parseFloat(aRaw.replace(/[,%+—]/g,''));
        const bNum = parseFloat(bRaw.replace(/[,%+—]/g,''));
        if(aRaw==='—' && bRaw!=='—') return 1;
        if(bRaw==='—' && aRaw!=='—') return -1;
        const cmp = (!isNaN(aNum)&&!isNaN(bNum)) ? aNum-bNum : aRaw.localeCompare(bRaw,'ja');
        return sortAsc ? cmp : -cmp;
      }});
      rows.forEach(function(r){{ tbody.appendChild(r); }});
    }});
  }});
}})();
</script>
</body>
</html>"""
    return html, table_height


# =====================================================
# 実行ロジック
# =====================================================
if run_button:
    if not TV_API_AVAILABLE:
        st.error("❌ `tradingview-screener` パッケージがインストールされていません。")
    else:
        with st.spinner("⏳ TradingView APIでスクリーニング中..."):
            try:
                candidates = run_bottom_reversal(
                    perf_threshold=perf_3m_threshold,
                    rsi_min=rsi_range[0],
                    rsi_max=rsi_range[1],
                    min_vol=min_volume,
                )
                st.session_state["br_candidates"] = candidates
                st.session_state["br_run_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            except Exception as e:
                st.error(f"❌ スクリーニング失敗: {e}")
                candidates = []


# =====================================================
# 結果表示
# =====================================================
if "br_candidates" in st.session_state:
    candidates = st.session_state["br_candidates"]
    run_at = st.session_state.get("br_run_at", "")

    if not candidates:
        st.warning("⚠️ 条件に合致する銘柄がありませんでした。条件を緩めてみてください。")
    else:
        st.markdown(f"## 📋 スクリーニング結果（{run_at} 実行）")

        # ---- KPI サマリー ----
        n = len(candidates)
        avg_rsi = sum(c["rsi"] for c in candidates) / n
        avg_rev = [c["reversal_pos"] for c in candidates if c.get("reversal_pos") is not None]
        avg_dev = [c["sma200_dev"] for c in candidates if c.get("sma200_dev") is not None]
        avg_perf_3m = [c["perf_3m"] for c in candidates if c.get("perf_3m") is not None]

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("検出銘柄数", f"{n}件")
        with col2:
            st.metric("平均RSI", f"{avg_rsi:.1f}")
        with col3:
            if avg_rev:
                st.metric("平均反発度", f"{sum(avg_rev)/len(avg_rev):.1f}%")
            else:
                st.metric("平均反発度", "N/A")
        with col4:
            if avg_perf_3m:
                st.metric("平均3M騰落", f"{sum(avg_perf_3m)/len(avg_perf_3m):.1f}%")
            else:
                st.metric("平均3M騰落", "N/A")

        # ---- テーブル表示 ----
        table_html, table_height = _build_result_table(candidates)
        st.components.v1.html(table_html, height=table_height + 4, scrolling=False)

        # ---- 解説 ----
        st.markdown("---")
        st.markdown("""
        **📝 指標の読み方**
        - **反発度**: (現在値 - 3ヶ月安値) / (3ヶ月高値 - 3ヶ月安値) × 100。低いほど底に近い初動段階
        - **SMA200乖離**: 200日移動平均線からの乖離率。マイナスが大きいほど長期トレンドから離れている
        - **RSI**: 30付近は売られすぎからの回復初期、50に近づくほど上昇力が増している
        - **MACD > Signal**: ゴールデンクロスが発生しており、上昇モメンタムへの転換を示唆
        """)

else:
    # ---- 未実行の案内 ----
    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("""
        ### 📋 使い方
        1. **左サイドバーで条件を調整**（デフォルトのままでもOK）
        2. **「スクリーニング実行」ボタンを押す**
        3. 結果がリアルタイムで表示されます

        **既存のSMA順行配列スクリーニングとの違い:**
        - 順行配列: 上昇トレンド中の銘柄を検出
        - **底打ち反転: 下落→反転の初動を検出**
        """)
    with col2:
        st.markdown("""
        ### ⚡ 特徴
        - **実行時間:** 約1秒（TradingView APIで即時取得）
        - **データ:** リアルタイム（市場営業中は最新データ）
        - **条件カスタマイズ:** サイドバーでRSI範囲・出来高等を調整可能

        **📌 ヒント:** 反発度が低い（20%以下）銘柄は底に近い
        初動段階で、リスク・リターンが高い銘柄です。
        """)

    st.markdown("---")
    st.caption("⚠️ 本ツールは投資助言を目的としたものではありません。")
