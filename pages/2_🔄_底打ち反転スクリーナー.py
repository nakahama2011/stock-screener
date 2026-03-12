"""
🔄 底打ち反転スクリーナー

長期下落→底打ち→上昇転換しそうな日本株をスクリーニングする。

モード:
  - 今日: TradingView Screener API でリアルタイム取得（約1秒）
  - 過去日付: backtester.py のインフラを利用してバックテスト
"""

import io
import json
import math
import os
import sys
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import streamlit as st

warnings.filterwarnings("ignore")

# backtester.py のインフラを再利用する
SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRIPT_DIR)
from backtester import (
    fetch_jpx_tickers,
    fetch_ticker_history,
    calc_forward_returns,
    MAX_WORKERS,
)

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
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "results")
JPX_CSV_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
# SMA60 + RSI + MACD 計算に必要な最小バッファ（120営業日）
HISTORY_BUFFER_DAYS = 120


# =====================================================
# カスタムCSS
# =====================================================
st.markdown("""
<style>
  html, body, [class*="css"] {
    font-family: 'Inter', 'Noto Sans JP', -apple-system, sans-serif;
  }
  .stApp { background-color: #f0f2f6 !important; }
  section[data-testid="stSidebar"] { background-color: #e4e7ed !important; }
  [data-testid="metric-container"] {
    background: #1a2332; border: 1px solid #2a3a4e;
    border-radius: 10px; padding: 1rem !important;
  }
  .app-header {
    background: linear-gradient(135deg, rgba(168,85,247,0.15), rgba(59,130,246,0.08));
    border: 1px solid rgba(168,85,247,0.25);
    border-radius: 12px; padding: 1.2rem 1.5rem; margin-bottom: 1.5rem;
  }
  .app-header h1 { margin: 0; font-size: 1.6rem; }
  .app-header p { margin: 0.3rem 0 0; color: #8899aa; font-size: 0.9rem; }
  .badge-reversal {
    display: inline-block; background: rgba(168,85,247,0.2);
    border: 1px solid rgba(168,85,247,0.4); color: #a855f7;
    padding: 0.15rem 0.6rem; border-radius: 6px;
    font-size: 0.75rem; font-weight: 700; margin-left: 0.5rem;
  }
  .condition-box {
    background: rgba(168,85,247,0.06); border: 1px solid rgba(168,85,247,0.2);
    border-radius: 10px; padding: 1rem 1.2rem; margin-bottom: 1rem;
    font-size: 0.88rem; line-height: 1.6;
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
  <p>長期下落→底打ち→上昇転換しそうな銘柄をスクリーニング</p>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div class="condition-box">
  <h4>📋 スクリーニング条件</h4>
  <strong>📉 長期下落:</strong> 3ヶ月パフォーマンス < 0 かつ 終値 < SMA60<br>
  <strong>📊 底打ちシグナル:</strong> RSI 30〜50 かつ MACD > Signal（ゴールデンクロス）<br>
  <strong>📈 短期上昇転換:</strong> SMA5 > SMA20 かつ 終値 > SMA5<br>
  <strong>📦 出来高:</strong> ≥ 50万株
</div>
""", unsafe_allow_html=True)


# =====================================================
# ユーティリティ
# =====================================================
@st.cache_data(ttl=86400, show_spinner=False)
def _fetch_jpx_name_map() -> Dict[int, str]:
    """JPXの上場銘柄一覧から銘柄名マッピングを取得する。"""
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
        return None if f != f else f
    except (ValueError, TypeError):
        return None


# =====================================================
# サイドバー
# =====================================================
with st.sidebar:
    st.markdown("## ⚙️ スクリーニング設定")
    st.markdown("---")

    # --- 日付選択 ---
    st.markdown("### 📅 検証日付")
    date_mode = st.radio(
        "モード選択",
        options=["今日（リアルタイム）", "過去日付を指定"],
        key="br_date_mode",
        help="今日=TradingView API（約1秒）、過去日付=yfinance（数分）",
    )

    selected_date = date.today()
    is_today = True

    if date_mode == "過去日付を指定":
        is_today = False
        quick_dates = []
        d = date.today() - timedelta(days=1)
        while len(quick_dates) < 5:
            if d.weekday() < 5:
                quick_dates.append(d)
            d -= timedelta(days=1)
        quick_labels = [
            f"{d.strftime('%m/%d')}（{['月','火','水','木','金','土','日'][d.weekday()]}）"
            for d in quick_dates
        ]
        quick_choice = st.selectbox(
            "クイック選択", ["カレンダーで指定"] + quick_labels, index=0, key="br_quick")
        if quick_choice == "カレンダーで指定":
            selected_date = st.date_input(
                "カレンダーで選択", value=date.today() - timedelta(days=1),
                min_value=date(2024, 1, 1), max_value=date.today(), key="br_cal")
        else:
            idx = quick_labels.index(quick_choice)
            selected_date = quick_dates[idx]
            st.info(f"選択中: **{selected_date.strftime('%Y年%m月%d日')}**")

    st.markdown("---")

    st.markdown("### 📉 長期下落条件")
    perf_3m_threshold = st.slider(
        "3ヶ月パフォーマンス上限（%）", -50.0, 0.0, 0.0, 5.0,
        key="perf_3m_thr",
        help="これ以下の3ヶ月パフォーマンスの銘柄のみ表示",
    )

    st.markdown("---")
    st.markdown("### 📊 テクニカル条件")
    rsi_range = st.slider(
        "RSI範囲", 10.0, 70.0, (30.0, 50.0), 5.0, key="rsi_range",
        help="底打ち判定のRSI範囲",
    )

    st.markdown("---")
    st.markdown("### 📦 出来高条件")
    min_volume = st.number_input(
        "最低出来高（株）", 100_000, 10_000_000, 500_000, 100_000,
        format="%d", key="br_min_vol",
    )

    st.markdown("---")
    mode_label = "⚡ TradingView API（即時）" if is_today else f"📅 yfinance（{selected_date.strftime('%Y/%m/%d')}）"
    st.info(
        f"**適用条件**\n"
        f"- モード: {mode_label}\n"
        f"- Perf.3M < {perf_3m_threshold:.0f}%\n"
        f"- Close < SMA60\n"
        f"- RSI: {rsi_range[0]:.0f}〜{rsi_range[1]:.0f}\n"
        f"- MACD > Signal\n"
        f"- SMA5 > SMA20, Close > SMA5\n"
        f"- 出来高 ≥ {min_volume:,}株"
    )
    st.markdown("---")

    button_label = "🔍 底打ち反転スクリーニング実行" if is_today else f"🔍 {selected_date.strftime('%Y/%m/%d')} でスクリーニング実行"
    run_button = st.button(button_label, type="primary", use_container_width=True)


# =====================================================
# TradingView API（今日用）
# =====================================================
@st.cache_data(ttl=300, show_spinner=False)
def run_today(perf_thr: float, rsi_min: float, rsi_max: float, min_vol: int) -> List[Dict]:
    """TradingView APIで今日の底打ち反転銘柄を取得する。"""
    if not TV_API_AVAILABLE:
        return []
    (_, df) = (Query()
        .set_markets('japan')
        .select('name','description','close','volume',
                'SMA5','SMA20','SMA60','SMA200',
                'RSI','MACD.macd','MACD.signal',
                'Perf.3M','Perf.6M','High.3M','Low.3M',
                'relative_volume_10d_calc','change')
        .where(
            tv_col('Perf.3M') < perf_thr,
            tv_col('close') < tv_col('SMA200'),
            tv_col('RSI') > rsi_min, tv_col('RSI') < rsi_max,
            tv_col('MACD.macd') > tv_col('MACD.signal'),
            tv_col('SMA5') > tv_col('SMA20'),
            tv_col('close') > tv_col('SMA5'),
            tv_col('volume') > min_vol,
        )
        .order_by('volume', ascending=False)
        .limit(500)
        .get_scanner_data())
    if df.empty:
        return []
    jpx = _fetch_jpx_name_map()
    cands = []
    for _, r in df.iterrows():
        ts = str(r.get("ticker",""))
        cs = ts.split(":")[-1] if ":" in ts else ts
        try: code = int(cs)
        except ValueError: continue
        cl = float(r.get("close",0)); s200 = float(r.get("SMA200",0))
        h3 = _safe_float(r.get("High.3M")); l3 = _safe_float(r.get("Low.3M"))
        rp = round((cl-l3)/(h3-l3)*100,1) if h3 and l3 and h3>l3 else None
        sd = round((cl-s200)/s200*100,2) if s200>0 else None
        cands.append({
            "code": code,
            "name": jpx.get(code, str(r.get("description",r.get("name","")))),
            "close": round(cl,1),
            "rsi": round(float(r.get("RSI",0)),1),
            "macd": round(float(r.get("MACD.macd",0)),4),
            "macd_signal": round(float(r.get("MACD.signal",0)),4),
            "perf_3m": round(_safe_float(r.get("Perf.3M")) or 0,2),
            "sma200_dev": sd, "reversal_pos": rp,
            "change": round(_safe_float(r.get("change")) or 0,2) if _safe_float(r.get("change")) else None,
            "volume": int(r.get("volume",0)),
            "rel_vol": round(_safe_float(r.get("relative_volume_10d_calc")) or 0,2) if _safe_float(r.get("relative_volume_10d_calc")) else None,
            "ret_1d": None, "ret_2d": None, "ret_3d": None, "ret_5d": None,
        })
    cands.sort(key=lambda x: x["volume"], reverse=True)
    return cands


# =====================================================
# バックテスト（過去日付用） — backtester.py インフラ再利用
# =====================================================
def _screen_reversal_at_date(
    df: pd.DataFrame,
    as_of: pd.Timestamp,
    min_vol: int,
    rsi_min: float,
    rsi_max: float,
    perf_thr: float,
) -> Optional[Dict]:
    """
    指定日時点で底打ち反転条件を判定する。
    backtester.py の screen_at_date と同じ構造で未来データ混入を防止する。

    SMA200の代わりにSMA60を使用（データ量の制約を回避）。
    """
    past_df = df[df.index <= as_of].copy()

    # SMA60計算に最低60行、RSI/MACDに追加30行は必要
    if len(past_df) < 60:
        return None

    past_df["SMA5"] = past_df["Close"].rolling(5).mean()
    past_df["SMA20"] = past_df["Close"].rolling(20).mean()
    past_df["SMA60"] = past_df["Close"].rolling(60).mean()

    latest = past_df.iloc[-1]
    for col_name in ["SMA5", "SMA20", "SMA60", "Volume"]:
        if pd.isna(latest[col_name]):
            return None

    close = float(latest["Close"])
    sma5 = float(latest["SMA5"])
    sma20 = float(latest["SMA20"])
    sma60 = float(latest["SMA60"])
    volume = int(latest["Volume"])

    # 条件1: 出来高
    if volume < min_vol:
        return None
    # 条件2: 終値 < SMA60（長期的に弱い）
    if close >= sma60:
        return None
    # 条件3: SMA5 > SMA20（短期上昇転換）
    if not (sma5 > sma20):
        return None
    # 条件4: 終値 > SMA5
    if close <= sma5:
        return None

    # 条件5: 3ヶ月パフォーマンス
    perf_window = min(60, len(past_df) - 1)
    if perf_window < 40:
        return None
    close_3m_ago = float(past_df.iloc[-(perf_window + 1)]["Close"])
    perf_3m = (close - close_3m_ago) / close_3m_ago * 100 if close_3m_ago > 0 else 0
    if perf_3m >= perf_thr:
        return None

    # 条件6: RSI(14)
    delta = past_df["Close"].diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1/14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/14, adjust=False).mean()
    rs = avg_gain.iloc[-1] / avg_loss.iloc[-1] if avg_loss.iloc[-1] != 0 else float("inf")
    rsi = 100 - 100 / (1 + rs)
    if not (rsi_min <= rsi <= rsi_max):
        return None

    # 条件7: MACD > Signal
    ema12 = past_df["Close"].ewm(span=12, adjust=False).mean()
    ema26 = past_df["Close"].ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    macd_val = float(macd_line.iloc[-1])
    signal_val = float(signal_line.iloc[-1])
    if macd_val <= signal_val:
        return None

    # SMA60乖離率
    sma60_dev = round((close - sma60) / sma60 * 100, 2)

    # 反発度（3ヶ月レンジ）
    w = past_df.tail(perf_window)
    h3 = float(w["High"].max()) if "High" in w.columns else float(w["Close"].max())
    l3 = float(w["Low"].min()) if "Low" in w.columns else float(w["Close"].min())
    rp = round((close - l3) / (h3 - l3) * 100, 1) if h3 > l3 else None

    # 当日変動
    change = None
    if len(past_df) >= 2:
        pc = float(past_df.iloc[-2]["Close"])
        if pc > 0:
            change = round((close - pc) / pc * 100, 2)

    return {
        "close": round(close, 1),
        "rsi": round(rsi, 1),
        "macd": round(macd_val, 4),
        "macd_signal": round(signal_val, 4),
        "perf_3m": round(perf_3m, 2),
        "sma200_dev": sma60_dev,  # UIラベルは共通だがバックテスト時はSMA60乖離
        "reversal_pos": rp,
        "change": change,
        "volume": volume,
    }


@st.cache_data(ttl=3600, show_spinner=False)
def run_backtest(
    as_of_str: str, perf_thr: float, rsi_min: float, rsi_max: float, min_vol: int,
) -> List[Dict]:
    """過去日付で底打ち反転銘柄をバックテストする。backtester.pyのインフラを使用。"""
    as_of_dt = datetime.strptime(as_of_str, "%Y-%m-%d")
    fetch_start = (as_of_dt - timedelta(days=HISTORY_BUFFER_DAYS)).strftime("%Y-%m-%d")
    fetch_end = (as_of_dt + timedelta(days=14)).strftime("%Y-%m-%d")

    tickers_df = fetch_jpx_tickers()
    jpx_names = _fetch_jpx_name_map()
    if tickers_df.empty:
        return []

    # データ取得（backtester.pyの fetch_ticker_history を使用）
    all_data = {}
    progress = st.progress(0, text="📥 株価データを取得中...")
    total = len(tickers_df)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for _, row in tickers_df.iterrows():
            code = int(row["code"])
            name = str(row.get("name", ""))
            f = executor.submit(fetch_ticker_history, code, name, fetch_start, fetch_end)
            futures[f] = (code, name)

        done = 0
        for f in as_completed(futures):
            done += 1
            if done % 100 == 0:
                progress.progress(min(done / total, 1.0), text=f"📥 {done}/{total} 銘柄のデータ取得中...")
            code, name = futures[f]
            try:
                res = f.result()
                if res is not None:
                    _, _, df = res
                    all_data[code] = (name, df)
            except Exception:
                pass

    progress.progress(1.0, text=f"✅ {len(all_data)}銘柄のデータ取得完了")

    # スクリーニング
    signal_ts = pd.Timestamp(as_of_dt)
    candidates = []
    for code, (name, df) in all_data.items():
        result = _screen_reversal_at_date(df, signal_ts, min_vol, rsi_min, rsi_max, perf_thr)
        if result is None:
            continue

        # 翌日以降のリターン（backtester.pyのcalc_forward_returnsを利用）
        fwd = calc_forward_returns(df, signal_ts)

        candidate = {
            "code": code,
            "name": jpx_names.get(code, name),
            **result,
            "rel_vol": None,
            "ret_1d": round(fwd.get(1), 2) if fwd.get(1) is not None else None,
            "ret_2d": round(fwd.get(2), 2) if fwd.get(2) is not None else None,
            "ret_3d": round(fwd.get(3), 2) if fwd.get(3) is not None else None,
            "ret_5d": round(fwd.get(5), 2) if fwd.get(5) is not None else None,
        }
        candidates.append(candidate)

    candidates.sort(key=lambda x: x["volume"], reverse=True)
    return candidates


# =====================================================
# テーブル生成
# =====================================================
def _build_table(cands: List[Dict], show_returns: bool = False) -> Tuple[str, int]:
    """HTMLテーブルを生成する。"""
    cols = [
        ("銘柄コード","left"),("銘柄名","left"),("終値","right"),
        ("RSI","right"),("MACD","right"),("Signal","right"),
        ("3M騰落","right"),("SMA乖離","right"),("反発度","right"),
        ("当日変動","right"),("出来高","right"),
    ]
    if show_returns:
        cols += [("翌日","right"),("2日後","right"),("3日後","right"),("5日後","right")]

    th = "".join(f'<th style="text-align:{a}">{n}</th>' for n,a in cols)
    rows = ""
    tv = "https://jp.tradingview.com/chart/M3vhlCeS/?symbol=TSE%3A"

    def _pc(v, s=30):
        if v is None: return '<td>—</td>'
        i = min(int(abs(v)/s*70),70)
        c = "#10b981" if v>0 else "#ef4444"
        b = "16,185,129" if v>0 else "239,68,68"
        return f'<td style="background:rgba({b},0.{i:02d});color:{c};font-weight:bold">{v:+.1f}%</td>'

    for c in cands:
        cd = c["code"]
        cc = f'<td class="code-cell"><a class="code-link" href="{tv}{cd}" target="_blank">{cd}</a></td>'
        nc = f'<td class="name-cell">{c["name"]}</td>'
        cl = f'<td>{c["close"]:,.1f}</td>'
        rsi = c["rsi"]
        rs = "background:rgba(16,185,129,0.2);color:#10b981;font-weight:bold" if rsi<=35 else "background:rgba(16,185,129,0.1);color:#10b981" if rsi<=45 else ""
        rc = f'<td style="{rs}">{rsi:.1f}</td>'
        mc = f'<td>{c["macd"]:.4f}</td>'
        sc = f'<td>{c["macd_signal"]:.4f}</td>'
        p3 = _pc(c.get("perf_3m"))
        dv = _pc(c.get("sma200_dev"))
        rv = c.get("reversal_pos")
        if rv is not None:
            rvs = "background:rgba(168,85,247,0.2);color:#a855f7;font-weight:bold" if rv<25 else "background:rgba(168,85,247,0.12);color:#a855f7" if rv<50 else "color:#64748b"
            rvc = f'<td style="{rvs}">{rv:.1f}%</td>'
        else:
            rvc = '<td>—</td>'
        chg = _pc(c.get("change"), 5)
        vc = f'<td>{c["volume"]:,}</td>'
        cells = cc+nc+cl+rc+mc+sc+p3+dv+rvc+chg+vc
        if show_returns:
            for k in ["ret_1d","ret_2d","ret_3d","ret_5d"]:
                cells += _pc(c.get(k), 3)
        rows += f"<tr>{cells}</tr>\n"

    n = len(cands)
    h = min(max(n*42+60, 200), 700)
    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,'Noto Sans JP',sans-serif;background:transparent;overflow-x:auto}}
table{{border-collapse:collapse;width:100%;font-size:12px}}
th{{position:sticky;top:0;z-index:10;background:#1a2332;color:#8899aa;font-weight:600;padding:8px 10px;border-bottom:1px solid #2a3a4e;white-space:nowrap;cursor:pointer;user-select:none}}
th:hover{{background:#1e2d42!important}}
td{{padding:7px 10px;text-align:right;border-bottom:1px solid rgba(255,255,255,0.04);color:#111;white-space:nowrap}}
td.name-cell{{text-align:left;color:#111;max-width:180px;overflow:hidden;text-overflow:ellipsis}}
td.code-cell{{text-align:left}}
tr:hover td{{background:rgba(255,255,255,0.03)}}
.code-link{{display:inline-block;color:#a855f7;font-weight:700;text-decoration:none;background:rgba(168,85,247,0.1);border:1px solid rgba(168,85,247,0.4);border-radius:12px;padding:2px 10px;transition:background .15s}}
.code-link:hover{{background:rgba(168,85,247,0.25);border-color:#a855f7}}
th[data-sort="asc"]::after{{content:" ▲";font-size:9px;color:#a855f7}}
th[data-sort="desc"]::after{{content:" ▼";font-size:9px;color:#a855f7}}
.tbl-wrap{{height:{h}px;overflow-y:auto;overflow-x:auto}}
</style></head><body>
<div class="tbl-wrap"><table><thead><tr>{th}</tr></thead><tbody>{rows}</tbody></table></div>
<script>
(function(){{const t=document.querySelector('table'),b=t.querySelector('tbody'),hs=t.querySelectorAll('th');let si=-1,sa=true;
hs.forEach(function(h,i){{h.addEventListener('click',function(){{
if(si===i){{sa=!sa}}else{{si=i;sa=true}}
hs.forEach(function(x){{x.dataset.sort=''}});h.dataset.sort=sa?'asc':'desc';
const rs=Array.from(b.querySelectorAll('tr'));
rs.sort(function(a,c){{const aR=(a.cells[i]?.innerText||'').trim(),bR=(c.cells[i]?.innerText||'').trim();
const aN=parseFloat(aR.replace(/[,%+—]/g,'')),bN=parseFloat(bR.replace(/[,%+—]/g,''));
if(aR==='—'&&bR!=='—')return 1;if(bR==='—'&&aR!=='—')return-1;
const d=(!isNaN(aN)&&!isNaN(bN))?aN-bN:aR.localeCompare(bR,'ja');return sa?d:-d}});
rs.forEach(function(r){{b.appendChild(r)}})}})}})}})();
</script></body></html>"""
    return html, h


# =====================================================
# 実行ロジック
# =====================================================
if run_button:
    if is_today:
        if not TV_API_AVAILABLE:
            st.error("❌ `tradingview-screener` パッケージがインストールされていません。")
        else:
            with st.spinner("⏳ TradingView APIでスクリーニング中..."):
                try:
                    cands = run_today(perf_3m_threshold, rsi_range[0], rsi_range[1], min_volume)
                    st.session_state["br_cands"] = cands
                    st.session_state["br_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    st.session_state["br_bt"] = False
                except Exception as e:
                    st.error(f"❌ スクリーニング失敗: {e}")
    else:
        as_of_str = selected_date.strftime("%Y-%m-%d")
        with st.spinner(f"⏳ {as_of_str} のデータでバックテスト中..."):
            try:
                cands = run_backtest(as_of_str, perf_3m_threshold, rsi_range[0], rsi_range[1], min_volume)
                st.session_state["br_cands"] = cands
                st.session_state["br_at"] = f"{as_of_str}（バックテスト）"
                st.session_state["br_bt"] = True
            except Exception as e:
                st.error(f"❌ バックテスト失敗: {e}")


# =====================================================
# 結果表示
# =====================================================
if "br_cands" in st.session_state:
    cands = st.session_state["br_cands"]
    run_at = st.session_state.get("br_at", "")
    is_bt = st.session_state.get("br_bt", False)

    if not cands:
        st.warning("⚠️ 条件に合致する銘柄がありませんでした。条件を緩めてみてください。")
    else:
        st.markdown(f"## 📋 スクリーニング結果（{run_at}）")

        n = len(cands)
        avg_rsi = sum(c["rsi"] for c in cands) / n
        avg_rev = [c["reversal_pos"] for c in cands if c.get("reversal_pos") is not None]
        avg_p3 = [c["perf_3m"] for c in cands if c.get("perf_3m") is not None]

        if is_bt:
            ret1 = [c["ret_1d"] for c in cands if c.get("ret_1d") is not None]
            c1,c2,c3,c4,c5 = st.columns(5)
            with c1: st.metric("検出銘柄数", f"{n}件")
            with c2: st.metric("平均RSI", f"{avg_rsi:.1f}")
            with c3:
                st.metric("平均反発度", f"{sum(avg_rev)/len(avg_rev):.1f}%" if avg_rev else "N/A")
            with c4:
                if ret1:
                    wr = sum(1 for r in ret1 if r>0)/len(ret1)*100
                    st.metric("翌日勝率", f"{wr:.1f}%（{len(ret1)}件）")
                else:
                    st.metric("翌日勝率", "N/A")
            with c5:
                if ret1:
                    st.metric("平均翌日リターン", f"{sum(ret1)/len(ret1):+.2f}%")
                else:
                    st.metric("平均翌日リターン", "N/A")
        else:
            c1,c2,c3,c4 = st.columns(4)
            with c1: st.metric("検出銘柄数", f"{n}件")
            with c2: st.metric("平均RSI", f"{avg_rsi:.1f}")
            with c3:
                st.metric("平均反発度", f"{sum(avg_rev)/len(avg_rev):.1f}%" if avg_rev else "N/A")
            with c4:
                st.metric("平均3M騰落", f"{sum(avg_p3)/len(avg_p3):.1f}%" if avg_p3 else "N/A")

        tbl, th = _build_table(cands, show_returns=is_bt)
        st.components.v1.html(tbl, height=th+4, scrolling=False)

        st.markdown("---")
        st.markdown("""
        **📝 指標の読み方**
        - **反発度**: (現在値 - 3ヶ月安値)/(3ヶ月高値 - 3ヶ月安値)×100。低いほど底に近い
        - **SMA乖離**: 中長期移動平均線からの乖離率。マイナスが大きいほど弱い
        - **RSI**: 30付近は売られすぎからの回復初期、50付近は上昇力が増している
        - **MACD > Signal**: ゴールデンクロスで上昇モメンタムへの転換を示唆
        """)

else:
    st.markdown("---")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("""
        ### 📋 使い方
        1. **モード選択:** 「今日」or「過去日付を指定」
        2. **条件を調整**（デフォルトのままでもOK）
        3. **「スクリーニング実行」ボタンを押す**

        **2つのモード:**
        - 🟢 **今日:** TradingView API（約1秒）
        - 🟡 **過去日付:** yfinance バックテスト（数分）
          → 翌日〜5日後のリターンも表示
        """)
    with c2:
        st.markdown("""
        ### ⚡ 特徴
        - **今日モード:** リアルタイムデータで即時取得
        - **過去日付モード:** 過去のシグナルを検証し、
          **翌日以降のリターン**で仮説を検証可能

        **📌 ヒント:** 反発度が低い（20%以下）銘柄は
        底に近い初動段階で、リスク・リターンが高い銘柄です。
        """)
    st.markdown("---")
    st.caption("⚠️ 本ツールは投資助言を目的としたものではありません。")
