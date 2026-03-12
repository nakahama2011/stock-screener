"""
🔄 底打ち反転スクリーナー

長期下落→底打ち→上昇転換しそうな日本株をスクリーニングする。

モード:
  - 今日: TradingView Screener API でリアルタイム取得（約1秒）
  - 過去日付: TradingView事前フィルタ + yf.download一括取得でバックテスト
"""

import io
import math
import os
import sys
import warnings
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

# TradingView Screener API
try:
    from tradingview_screener import Query, col as tv_col
    TV_API_AVAILABLE = True
except ImportError:
    TV_API_AVAILABLE = False

# yfinance
try:
    import yfinance as yf
    YF_AVAILABLE = True
except ImportError:
    YF_AVAILABLE = False


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
HISTORY_BUFFER_DAYS = 150  # SMA60 + RSI + MACD用バッファ
RETURN_DAYS = [5, 10, 15, 20, 30]


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
  <strong>📊 底打ちシグナル:</strong> RSI範囲内 かつ MACD > Signal（オプション）<br>
  <strong>📈 短期上昇転換:</strong> SMA5 > SMA20<br>
  <strong>📦 出来高:</strong> ≥ 指定値
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
    if val is None:
        return None
    try:
        f = float(val)
        return None if f != f else f
    except (ValueError, TypeError):
        return None


# =====================================================
# TradingView事前フィルタ（出来高の大きい銘柄のみ取得）
# =====================================================
@st.cache_data(ttl=3600, show_spinner=False)
def _get_volume_universe(min_vol: int) -> List[int]:
    """TradingView APIで出来高が閾値以上の銘柄コードリストを取得する（事前フィルタ用）。"""
    if not TV_API_AVAILABLE:
        return []
    try:
        (_, df) = (Query()
            .set_markets('japan')
            .select('name', 'close', 'volume')
            .where(tv_col('volume') > min_vol)
            .order_by('volume', ascending=False)
            .limit(2000)
            .get_scanner_data())
        codes = []
        for _, r in df.iterrows():
            ts = str(r.get("ticker", ""))
            cs = ts.split(":")[-1] if ":" in ts else ts
            try:
                codes.append(int(cs))
            except ValueError:
                pass
        return codes
    except Exception:
        return []


# =====================================================
# yf.download一括取得（高速バックテスト用）
# =====================================================
@st.cache_data(ttl=3600, show_spinner=False)
def _batch_download(codes: Tuple[int, ...], start: str, end: str) -> Dict[int, pd.DataFrame]:
    """yf.downloadで複数銘柄を一括取得する（個別取得の10倍以上高速）。"""
    if not YF_AVAILABLE or not codes:
        return {}

    symbols = [f"{c}.T" for c in codes]
    # バッチダウンロード（yfinanceが内部で最適化する）
    try:
        raw = yf.download(
            symbols,
            start=start,
            end=end,
            group_by='ticker',
            threads=True,
            progress=False,
        )
    except Exception:
        return {}

    result = {}
    for code in codes:
        sym = f"{code}.T"
        try:
            if len(symbols) == 1:
                # 1銘柄の場合はマルチインデックスにならない
                df = raw.copy()
            else:
                df = raw[sym].copy()
            df = df.dropna(subset=["Close"])
            if df.index.tz is not None:
                df.index = df.index.tz_localize(None)
            if len(df) >= 30:
                result[code] = df
        except Exception:
            pass

    return result


# =====================================================
# サイドバー
# =====================================================
with st.sidebar:
    st.markdown("## ⚙️ スクリーニング設定")
    st.markdown("---")

    st.markdown("### 📅 検証日付")
    date_mode = st.radio(
        "モード選択",
        options=["今日（リアルタイム）", "過去日付を指定"],
        key="br_date_mode",
        help="今日=TradingView API（約1秒）、過去日付=yfinance（約30秒〜1分）",
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
        key="perf_3m_thr")

    st.markdown("---")
    st.markdown("### 📊 テクニカル条件")
    rsi_range = st.slider(
        "RSI範囲", 10.0, 70.0, (25.0, 55.0), 5.0, key="rsi_range",
        help="デフォルト: 25〜55（広めに設定）")

    require_macd = st.checkbox(
        "MACD > Signal を必須にする",
        value=False,
        key="req_macd",
        help="オフにすると条件が緩和され、より多くの銘柄が検出されます",
    )

    st.markdown("---")
    st.markdown("### 📦 出来高条件")
    min_volume = st.number_input(
        "最低出来高（株）", 100_000, 10_000_000, 500_000, 100_000,
        format="%d", key="br_min_vol")

    st.markdown("---")
    macd_str = "必須" if require_macd else "不問"
    mode_label = "⚡ TradingView API" if is_today else f"📅 {selected_date.strftime('%Y/%m/%d')}"
    st.info(
        f"**適用条件**\n"
        f"- モード: {mode_label}\n"
        f"- Perf.3M < {perf_3m_threshold:.0f}%\n"
        f"- Close < SMA60\n"
        f"- RSI: {rsi_range[0]:.0f}〜{rsi_range[1]:.0f}\n"
        f"- MACD GC: {macd_str}\n"
        f"- SMA5 > SMA20\n"
        f"- 出来高 ≥ {min_volume:,}株"
    )
    st.markdown("---")

    button_label = "🔍 スクリーニング実行" if is_today else f"🔍 {selected_date.strftime('%Y/%m/%d')} で実行"
    run_button = st.button(button_label, type="primary", use_container_width=True)


# =====================================================
# TradingView API（今日用）
# =====================================================
@st.cache_data(ttl=300, show_spinner=False)
def run_today(perf_thr: float, rsi_min: float, rsi_max: float,
              min_vol: int, req_macd: bool) -> List[Dict]:
    """TradingView APIで今日の底打ち反転銘柄を取得する。"""
    if not TV_API_AVAILABLE:
        return []
    filters = [
        tv_col('Perf.3M') < perf_thr,
        tv_col('close') < tv_col('SMA200'),
        tv_col('RSI') > rsi_min, tv_col('RSI') < rsi_max,
        tv_col('SMA5') > tv_col('SMA20'),
        tv_col('close') > tv_col('SMA5'),
        tv_col('volume') > min_vol,
    ]
    if req_macd:
        filters.append(tv_col('MACD.macd') > tv_col('MACD.signal'))

    (_, df) = (Query()
        .set_markets('japan')
        .select('name','description','close','volume','change')
        .where(*filters)
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
        cands.append({
            "code": code,
            "name": jpx.get(code, str(r.get("description", r.get("name","")))),
            "prev_volume": None,
            "volume": int(r.get("volume", 0)),
            **{f"ret_{n}d": None for n in RETURN_DAYS},
        })
    cands.sort(key=lambda x: x["volume"], reverse=True)
    return cands


# =====================================================
# バックテスト（過去日付用） — 高速版
# =====================================================
def _screen_reversal(
    df: pd.DataFrame, as_of: pd.Timestamp,
    min_vol: int, rsi_min: float, rsi_max: float,
    perf_thr: float, req_macd: bool,
) -> Optional[Dict]:
    """指定日時点で底打ち反転条件を判定する。"""
    past = df[df.index <= as_of].copy()
    if len(past) < 60:
        return None

    past["SMA5"] = past["Close"].rolling(5).mean()
    past["SMA20"] = past["Close"].rolling(20).mean()
    past["SMA60"] = past["Close"].rolling(60).mean()

    latest = past.iloc[-1]
    for c in ["SMA5", "SMA20", "SMA60", "Volume"]:
        if pd.isna(latest[c]):
            return None

    close = float(latest["Close"])
    sma5 = float(latest["SMA5"])
    sma20 = float(latest["SMA20"])
    sma60 = float(latest["SMA60"])
    volume = int(latest["Volume"])

    # 条件判定
    if volume < min_vol: return None
    if close >= sma60: return None
    if not (sma5 > sma20): return None

    # 3ヶ月パフォーマンス
    pw = min(60, len(past) - 1)
    if pw < 40: return None
    c3m = float(past.iloc[-(pw+1)]["Close"])
    p3m = (close - c3m) / c3m * 100 if c3m > 0 else 0
    if p3m >= perf_thr: return None

    # RSI(14)
    delta = past["Close"].diff()
    ag = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
    al = (-delta).clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
    rs = ag.iloc[-1] / al.iloc[-1] if al.iloc[-1] != 0 else float("inf")
    rsi = 100 - 100 / (1 + rs)
    if not (rsi_min <= rsi <= rsi_max): return None

    # MACD（オプション）
    if req_macd:
        ema12 = past["Close"].ewm(span=12, adjust=False).mean()
        ema26 = past["Close"].ewm(span=26, adjust=False).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9, adjust=False).mean()
        if float(macd.iloc[-1]) <= float(signal.iloc[-1]):
            return None

    prev_vol = int(past.iloc[-2]["Volume"]) if len(past) >= 2 else None

    return {"volume": volume, "prev_volume": prev_vol}


def _calc_returns(df: pd.DataFrame, signal_date: pd.Timestamp) -> Dict[str, Optional[float]]:
    """シグナル日終値からの累積リターンを計算する。"""
    past = df[df.index <= signal_date]
    if past.empty:
        return {f"ret_{n}d": None for n in RETURN_DAYS}
    base = float(past.iloc[-1]["Close"])
    future = df[df.index > signal_date]
    result = {}
    for n in RETURN_DAYS:
        if len(future) >= n and base > 0:
            result[f"ret_{n}d"] = round((float(future.iloc[n-1]["Close"]) - base) / base * 100, 2)
        else:
            result[f"ret_{n}d"] = None
    return result


@st.cache_data(ttl=3600, show_spinner=False)
def run_backtest(
    as_of_str: str, perf_thr: float, rsi_min: float, rsi_max: float,
    min_vol: int, req_macd: bool, universe_codes: Tuple[int, ...],
) -> List[Dict]:
    """高速バックテスト: TradingView事前フィルタ + yf.download一括取得。"""
    as_of_dt = datetime.strptime(as_of_str, "%Y-%m-%d")
    fetch_start = (as_of_dt - timedelta(days=HISTORY_BUFFER_DAYS)).strftime("%Y-%m-%d")
    fetch_end = (as_of_dt + timedelta(days=50)).strftime("%Y-%m-%d")

    jpx_names = _fetch_jpx_name_map()

    # yf.downloadで一括取得（高速）
    all_data = _batch_download(universe_codes, fetch_start, fetch_end)

    signal_ts = pd.Timestamp(as_of_dt)
    candidates = []
    for code, df in all_data.items():
        result = _screen_reversal(df, signal_ts, min_vol, rsi_min, rsi_max, perf_thr, req_macd)
        if result is None:
            continue
        fwd = _calc_returns(df, signal_ts)
        candidates.append({
            "code": code,
            "name": jpx_names.get(code, str(code)),
            "prev_volume": result["prev_volume"],
            "volume": result["volume"],
            **fwd,
        })

    candidates.sort(key=lambda x: x["volume"], reverse=True)
    return candidates


# =====================================================
# テーブル生成
# =====================================================
def _build_table(cands: List[Dict], show_returns: bool = False) -> Tuple[str, int]:
    """HTMLテーブルを生成する。"""
    cols = [
        ("銘柄コード", "left"),
        ("銘柄名", "left"),
        ("前日出来高", "right"),
        ("当日出来高", "right"),
    ]
    if show_returns:
        for n in RETURN_DAYS:
            cols.append((f"{n}日後", "right"))

    th = "".join(f'<th style="text-align:{a}">{n}</th>' for n, a in cols)
    rows = ""
    tv = "https://jp.tradingview.com/chart/M3vhlCeS/?symbol=TSE%3A"

    def _pc(v):
        if v is None: return '<td>—</td>'
        i = min(int(abs(v) / 10 * 70), 70)
        if v > 0:
            s = f"background:rgba(16,185,129,0.{i:02d});color:#10b981;font-weight:bold"
        elif v < 0:
            s = f"background:rgba(239,68,68,0.{i:02d});color:#ef4444;font-weight:bold"
        else:
            s = ""
        return f'<td style="{s}">{v:+.2f}%</td>'

    for c in cands:
        cd = c["code"]
        cc = f'<td class="code-cell"><a class="code-link" href="{tv}{cd}" target="_blank">{cd}</a></td>'
        nc = f'<td class="name-cell">{c["name"]}</td>'
        pv = c.get("prev_volume")
        pvc = f'<td>{pv:,}</td>' if pv is not None else '<td>—</td>'
        vc = f'<td>{c["volume"]:,}</td>'
        cells = cc + nc + pvc + vc
        if show_returns:
            for n in RETURN_DAYS:
                cells += _pc(c.get(f"ret_{n}d"))
        rows += f"<tr>{cells}</tr>\n"

    n_rows = len(cands)
    h = min(max(n_rows * 42 + 60, 200), 700)

    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,'Noto Sans JP',sans-serif;background:transparent;overflow-x:auto}}
table{{border-collapse:collapse;width:100%;font-size:13px}}
th{{position:sticky;top:0;z-index:10;background:#1a2332;color:#8899aa;font-weight:600;padding:8px 12px;border-bottom:1px solid #2a3a4e;white-space:nowrap;cursor:pointer;user-select:none}}
th:hover{{background:#1e2d42!important}}
td{{padding:8px 12px;text-align:right;border-bottom:1px solid rgba(255,255,255,0.04);color:#111;white-space:nowrap}}
td.name-cell{{text-align:left;color:#111;max-width:200px;overflow:hidden;text-overflow:ellipsis}}
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
                    cands = run_today(perf_3m_threshold, rsi_range[0], rsi_range[1],
                                     min_volume, require_macd)
                    st.session_state["br_cands"] = cands
                    st.session_state["br_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    st.session_state["br_bt"] = False
                except Exception as e:
                    st.error(f"❌ スクリーニング失敗: {e}")
    else:
        if not TV_API_AVAILABLE:
            st.error("❌ TradingView事前フィルタに `tradingview-screener` が必要です。")
        elif not YF_AVAILABLE:
            st.error("❌ `yfinance` パッケージがインストールされていません。")
        else:
            as_of_str = selected_date.strftime("%Y-%m-%d")

            # Step 1: TradingView APIで出来高の大きい銘柄を事前フィルタ（約1秒）
            with st.spinner("⏳ Step 1/2: TradingView APIで候補銘柄を絞り込み中..."):
                universe = _get_volume_universe(min_volume)
                if not universe:
                    st.error("❌ TradingView APIからの銘柄リスト取得に失敗しました。")
                else:
                    st.info(f"📋 出来高{min_volume:,}以上の銘柄: **{len(universe)}社**（全3800+社から絞り込み済み）")

            if universe:
                # Step 2: yf.downloadで一括取得 + スクリーニング
                with st.spinner(f"⏳ Step 2/2: {len(universe)}銘柄のデータを一括取得・分析中..."):
                    try:
                        cands = run_backtest(
                            as_of_str, perf_3m_threshold, rsi_range[0], rsi_range[1],
                            min_volume, require_macd, tuple(universe),
                        )
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
        st.warning("⚠️ 条件に合致する銘柄がありませんでした。以下を試してみてください：\n"
                   "- RSI範囲を広げる（例: 20〜60）\n"
                   "- 「MACD > Signal を必須にする」をオフにする\n"
                   "- 出来高の閾値を下げる")
    else:
        st.markdown(f"## 📋 スクリーニング結果（{run_at}）")
        n = len(cands)

        if is_bt:
            kpi_cols = st.columns(len(RETURN_DAYS) + 1)
            with kpi_cols[0]:
                st.metric("検出銘柄数", f"{n}件")
            for i, nd in enumerate(RETURN_DAYS):
                with kpi_cols[i + 1]:
                    rets = [c[f"ret_{nd}d"] for c in cands if c.get(f"ret_{nd}d") is not None]
                    if rets:
                        wr = sum(1 for r in rets if r > 0) / len(rets) * 100
                        avg_r = sum(rets) / len(rets)
                        st.metric(f"{nd}日後勝率", f"{wr:.0f}%", delta=f"平均{avg_r:+.1f}%")
                    else:
                        st.metric(f"{nd}日後勝率", "N/A")
        else:
            c1, c2 = st.columns(2)
            with c1: st.metric("検出銘柄数", f"{n}件")
            with c2: st.caption("💡 過去日付で5〜30日後のリターンを確認できます")

        tbl, th = _build_table(cands, show_returns=is_bt)
        st.components.v1.html(tbl, height=th + 4, scrolling=False)

        if is_bt:
            st.markdown("---")
            st.markdown("""
            **📝 リターンの見方**
            - 各「N日後」はシグナル日の**終値で購入**した場合のN営業日後の累積リターン（%）
            - 🟢 プラス = 利益  🔴 マイナス = 損失
            """)
else:
    st.markdown("---")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("""
        ### 📋 使い方
        1. **モード選択:** 「今日」or「過去日付を指定」
        2. **条件を調整**（デフォルトは緩め設定）
        3. **「スクリーニング実行」ボタンを押す**

        **2つのモード:**
        - 🟢 **今日:** TradingView API（約1秒）
        - 🟡 **過去日付:** yfinance バックテスト（約30秒〜1分）
        """)
    with c2:
        st.markdown("""
        ### ⚡ 高速化の仕組み
        1. TradingView APIで出来高の大きい銘柄に事前絞込
        2. yf.downloadで一括ダウンロード（個別取得の10倍高速）

        **📌 銘柄が少ない場合:**
        - RSI範囲を広げる（25〜55 → 20〜60）
        - 「MACD GC必須」をオフにする
        - 出来高の閾値を下げる
        """)
    st.markdown("---")
    st.caption("⚠️ 本ツールは投資助言を目的としたものではありません。")
