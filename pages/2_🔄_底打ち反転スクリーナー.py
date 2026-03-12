"""
🔄 底打ち反転スクリーナー

長期下落→底打ち→上昇転換しそうな日本株をスクリーニングする。

モード:
  - 今日: TradingView Screener API でリアルタイム取得（約1秒）
  - 過去日付: yfinance で株価データを取得しバックテスト（数分）
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

# TradingView Screener API
try:
    from tradingview_screener import Query, col as tv_col
    TV_API_AVAILABLE = True
except ImportError:
    TV_API_AVAILABLE = False

# yfinance（過去日付バックテスト用）
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
SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "results")
JPX_CSV_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
HISTORY_BUFFER_DAYS = 250  # SMA200計算用バッファ
MAX_WORKERS = 8


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
  <p>長期下落→底打ち→上昇転換しそうな銘柄をスクリーニング</p>
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


@st.cache_data(ttl=86400, show_spinner=False)
def _fetch_jpx_tickers() -> pd.DataFrame:
    """JPX上場銘柄一覧（内国普通株）を取得する。"""
    try:
        resp = requests.get(JPX_CSV_URL, timeout=30)
        resp.raise_for_status()
        df = pd.read_excel(io.BytesIO(resp.content))
        df = df.rename(columns={
            "コード": "code", "銘柄名": "name", "市場・商品区分": "market",
        })
        df = df[pd.to_numeric(df["code"], errors="coerce").notna()]
        df["code"] = df["code"].astype(int)
        stock_markets = [
            "プライム（内国株式）", "スタンダード（内国株式）", "グロース（内国株式）",
        ]
        df = df[df["market"].isin(stock_markets)]
        return df[["code", "name"]].reset_index(drop=True)
    except Exception:
        return pd.DataFrame(columns=["code", "name"])


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
# サイドバー（条件カスタマイズ + 日付選択）
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
        # クイック選択（最近5営業日）
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
            "クイック選択",
            options=["カレンダーで指定"] + quick_labels,
            index=0,
            key="br_quick_choice",
        )

        if quick_choice == "カレンダーで指定":
            selected_date = st.date_input(
                "カレンダーで選択",
                value=date.today() - timedelta(days=1),
                min_value=date(2024, 1, 1),
                max_value=date.today(),
                key="br_cal_date",
            )
        else:
            idx = quick_labels.index(quick_choice)
            selected_date = quick_dates[idx]
            st.info(f"選択中: **{selected_date.strftime('%Y年%m月%d日')}**")

    st.markdown("---")

    st.markdown("### 📉 長期下落条件")
    perf_3m_threshold = st.slider(
        "3ヶ月パフォーマンス上限（%）",
        min_value=-50.0,
        max_value=0.0,
        value=0.0,
        step=5.0,
        key="perf_3m_thr",
        help="これ以下の3ヶ月パフォーマンスの銘柄のみ表示",
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

    mode_label = "⚡ TradingView API（即時）" if is_today else f"📅 yfinance（{selected_date.strftime('%Y/%m/%d')}）"
    st.info(
        f"**適用条件**\n"
        f"- モード: {mode_label}\n"
        f"- Perf.3M < {perf_3m_threshold:.0f}%\n"
        f"- Close < SMA200\n"
        f"- RSI: {rsi_range[0]:.0f}〜{rsi_range[1]:.0f}\n"
        f"- MACD > Signal\n"
        f"- SMA5 > SMA20, Close > SMA5\n"
        f"- 出来高 ≥ {min_volume:,}株"
    )

    st.markdown("---")

    button_label = "🔍 底打ち反転スクリーニング実行" if is_today else f"🔍 {selected_date.strftime('%Y/%m/%d')} でスクリーニング実行"
    run_button = st.button(
        button_label,
        type="primary",
        use_container_width=True,
    )


# =====================================================
# TradingView API スクリーニング（今日用）
# =====================================================
@st.cache_data(ttl=300, show_spinner=False)
def run_bottom_reversal_today(
    perf_threshold: float,
    rsi_min: float,
    rsi_max: float,
    min_vol: int,
) -> List[Dict[str, Any]]:
    """TradingView Screener APIで今日の底打ち反転銘柄を取得する。"""
    if not TV_API_AVAILABLE:
        return []

    (count, df) = (Query()
        .set_markets('japan')
        .select(
            'name', 'description', 'close', 'volume',
            'SMA5', 'SMA20', 'SMA60', 'SMA200',
            'RSI', 'MACD.macd', 'MACD.signal',
            'Perf.1M', 'Perf.3M', 'Perf.6M',
            'High.3M', 'Low.3M',
            'relative_volume_10d_calc',
            'change',
        )
        .where(
            tv_col('Perf.3M') < perf_threshold,
            tv_col('close') < tv_col('SMA200'),
            tv_col('RSI') > rsi_min,
            tv_col('RSI') < rsi_max,
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

    jpx_names = _fetch_jpx_name_map()
    candidates = []

    for _, row in df.iterrows():
        ticker_str = str(row.get("ticker", ""))
        code_str = ticker_str.split(":")[-1] if ":" in ticker_str else ticker_str
        try:
            code = int(code_str)
        except ValueError:
            continue

        close_val = float(row.get("close", 0))
        sma200_val = float(row.get("SMA200", 0))
        high_3m = _safe_float(row.get("High.3M"))
        low_3m = _safe_float(row.get("Low.3M"))

        reversal_position = None
        if high_3m is not None and low_3m is not None and high_3m > low_3m:
            reversal_position = round((close_val - low_3m) / (high_3m - low_3m) * 100, 1)

        sma200_deviation = round((close_val - sma200_val) / sma200_val * 100, 2) if sma200_val > 0 else None

        candidate = {
            "code": code,
            "name": jpx_names.get(code, str(row.get("description", row.get("name", "")))),
            "close": round(close_val, 1),
            "rsi": round(float(row.get("RSI", 0)), 1),
            "macd": round(float(row.get("MACD.macd", 0)), 4),
            "macd_signal": round(float(row.get("MACD.signal", 0)), 4),
            "perf_3m": round(_safe_float(row.get("Perf.3M")) or 0, 2),
            "perf_6m": round(_safe_float(row.get("Perf.6M")) or 0, 2) if _safe_float(row.get("Perf.6M")) else None,
            "sma200_dev": sma200_deviation,
            "reversal_pos": reversal_position,
            "change": round(_safe_float(row.get("change")) or 0, 2) if _safe_float(row.get("change")) else None,
            "volume": int(row.get("volume", 0)),
            "rel_vol": round(_safe_float(row.get("relative_volume_10d_calc")) or 0, 2) if _safe_float(row.get("relative_volume_10d_calc")) else None,
            # 過去日付時のみ使用するリターン列（今日モードでは None）
            "ret_1d": None, "ret_2d": None, "ret_3d": None, "ret_5d": None,
        }
        candidates.append(candidate)

    candidates.sort(key=lambda x: x["volume"], reverse=True)
    return candidates


# =====================================================
# yfinance バックテスト（過去日付用）
# =====================================================
def _fetch_single_ticker(code: int, name: str, start: str, end: str) -> Optional[Tuple[int, str, pd.DataFrame]]:
    """1銘柄の日足データを取得する。"""
    symbol = f"{code}.T"
    try:
        df = yf.Ticker(symbol).history(start=start, end=end)
        if df.empty or len(df) < 60:
            return None
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        return (code, name, df)
    except Exception:
        return None


def _screen_bottom_reversal_at_date(
    df: pd.DataFrame,
    as_of: pd.Timestamp,
    min_vol: int,
    rsi_min: float,
    rsi_max: float,
    perf_threshold: float,
) -> Optional[Dict[str, Any]]:
    """
    指定日時点（as_of 以前のデータのみ）で底打ち反転条件を判定する。

    条件:
      1. 3ヶ月パフォーマンス < perf_threshold%
      2. 終値 < SMA200
      3. RSI rsi_min〜rsi_max
      4. MACD > Signal
      5. SMA5 > SMA20
      6. 終値 > SMA5
      7. 出来高 > min_vol
    """
    past_df = df[df.index <= as_of].copy()
    if len(past_df) < 200:
        return None

    # 移動平均線
    past_df["SMA5"] = past_df["Close"].rolling(5).mean()
    past_df["SMA20"] = past_df["Close"].rolling(20).mean()
    past_df["SMA200"] = past_df["Close"].rolling(200).mean()

    latest = past_df.iloc[-1]
    for col_name in ["SMA5", "SMA20", "SMA200", "Volume"]:
        if pd.isna(latest[col_name]):
            return None

    close = float(latest["Close"])
    sma5 = float(latest["SMA5"])
    sma20 = float(latest["SMA20"])
    sma200 = float(latest["SMA200"])
    volume = int(latest["Volume"])

    # 条件判定
    # 1. 出来高
    if volume < min_vol:
        return None
    # 2. 終値 < SMA200（長期的に弱い）
    if close >= sma200:
        return None
    # 3. SMA5 > SMA20（短期上昇転換）
    if not (sma5 > sma20):
        return None
    # 4. 終値 > SMA5
    if close <= sma5:
        return None

    # 5. 3ヶ月パフォーマンス（約60営業日前からの変動率）
    perf_window = 60
    if len(past_df) >= perf_window:
        close_3m_ago = float(past_df.iloc[-perf_window]["Close"])
        if close_3m_ago > 0:
            perf_3m = (close - close_3m_ago) / close_3m_ago * 100
        else:
            perf_3m = 0
    else:
        return None
    if perf_3m >= perf_threshold:
        return None

    # 6. RSI(14)
    delta = past_df["Close"].diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1/14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/14, adjust=False).mean()
    rs = avg_gain.iloc[-1] / avg_loss.iloc[-1] if avg_loss.iloc[-1] != 0 else float("inf")
    rsi = 100 - 100 / (1 + rs)
    if not (rsi_min <= rsi <= rsi_max):
        return None

    # 7. MACD > Signal
    ema12 = past_df["Close"].ewm(span=12, adjust=False).mean()
    ema26 = past_df["Close"].ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    macd_val = float(macd_line.iloc[-1])
    signal_val = float(signal_line.iloc[-1])
    if macd_val <= signal_val:
        return None

    # SMA200乖離率
    sma200_dev = round((close - sma200) / sma200 * 100, 2)

    # 3ヶ月レンジからの反発度
    window_3m = past_df.tail(60)
    high_3m = float(window_3m["High"].max()) if "High" in window_3m.columns else float(window_3m["Close"].max())
    low_3m = float(window_3m["Low"].min()) if "Low" in window_3m.columns else float(window_3m["Close"].min())
    reversal_pos = None
    if high_3m > low_3m:
        reversal_pos = round((close - low_3m) / (high_3m - low_3m) * 100, 1)

    # 当日の前日比
    change = None
    if len(past_df) >= 2:
        prev_close = float(past_df.iloc[-2]["Close"])
        if prev_close > 0:
            change = round((close - prev_close) / prev_close * 100, 2)

    return {
        "close": round(close, 1),
        "rsi": round(rsi, 1),
        "macd": round(macd_val, 4),
        "macd_signal": round(signal_val, 4),
        "perf_3m": round(perf_3m, 2),
        "sma200_dev": sma200_dev,
        "reversal_pos": reversal_pos,
        "change": change,
        "volume": volume,
    }


def _calc_forward_returns(df: pd.DataFrame, signal_date: pd.Timestamp) -> Dict[str, Optional[float]]:
    """シグナル発生日の翌日以降のリターンを計算する。"""
    past_df = df[df.index <= signal_date]
    if past_df.empty:
        return {"ret_1d": None, "ret_2d": None, "ret_3d": None, "ret_5d": None}
    base_close = float(past_df.iloc[-1]["Close"])
    future_df = df[df.index > signal_date]

    result = {}
    for n, key in [(1, "ret_1d"), (2, "ret_2d"), (3, "ret_3d"), (5, "ret_5d")]:
        if len(future_df) >= n and base_close > 0:
            fwd_close = float(future_df.iloc[n - 1]["Close"])
            result[key] = round((fwd_close - base_close) / base_close * 100, 2)
        else:
            result[key] = None
    return result


@st.cache_data(ttl=3600, show_spinner=False)
def run_bottom_reversal_backtest(
    as_of_date_str: str,
    perf_threshold: float,
    rsi_min: float,
    rsi_max: float,
    min_vol: int,
) -> List[Dict[str, Any]]:
    """過去日付で底打ち反転銘柄をバックテストする。"""
    if not YF_AVAILABLE:
        return []

    as_of_dt = datetime.strptime(as_of_date_str, "%Y-%m-%d")
    fetch_start = (as_of_dt - timedelta(days=HISTORY_BUFFER_DAYS)).strftime("%Y-%m-%d")
    fetch_end = (as_of_dt + timedelta(days=14)).strftime("%Y-%m-%d")

    tickers_df = _fetch_jpx_tickers()
    jpx_names = _fetch_jpx_name_map()

    if tickers_df.empty:
        return []

    # 並列データ取得
    all_data: Dict[int, Tuple[str, pd.DataFrame]] = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for _, row in tickers_df.iterrows():
            code = int(row["code"])
            name = str(row.get("name", ""))
            f = executor.submit(_fetch_single_ticker, code, name, fetch_start, fetch_end)
            futures[f] = (code, name)

        for f in as_completed(futures):
            code, name = futures[f]
            try:
                res = f.result()
                if res is not None:
                    _, _, df = res
                    all_data[code] = (name, df)
            except Exception:
                pass

    # スクリーニング判定
    signal_ts = pd.Timestamp(as_of_dt)
    candidates = []

    for code, (name, df) in all_data.items():
        screen_result = _screen_bottom_reversal_at_date(
            df, signal_ts, min_vol, rsi_min, rsi_max, perf_threshold,
        )
        if screen_result is None:
            continue

        fwd = _calc_forward_returns(df, signal_ts)

        candidate = {
            "code": code,
            "name": jpx_names.get(code, name),
            **screen_result,
            "perf_6m": None,  # yfinanceでは6ヶ月パフォーマンスを省略
            "reversal_pos": screen_result.get("reversal_pos"),
            "rel_vol": None,
            "ret_1d": fwd.get("ret_1d"),
            "ret_2d": fwd.get("ret_2d"),
            "ret_3d": fwd.get("ret_3d"),
            "ret_5d": fwd.get("ret_5d"),
        }
        candidates.append(candidate)

    candidates.sort(key=lambda x: x["volume"], reverse=True)
    return candidates


# =====================================================
# 結果テーブルのHTML生成
# =====================================================
def _build_result_table(candidates: List[Dict[str, Any]], show_returns: bool = False) -> Tuple[str, int]:
    """候補銘柄リストからスタイル付きHTMLテーブルを生成する。"""

    columns = [
        ("銘柄コード", "left"),
        ("銘柄名", "left"),
        ("終値", "right"),
        ("RSI", "right"),
        ("MACD", "right"),
        ("Signal", "right"),
        ("3M騰落", "right"),
        ("SMA200乖離", "right"),
        ("反発度", "right"),
        ("当日変動", "right"),
        ("出来高", "right"),
    ]

    # 過去日付の場合、リターン列を追加
    if show_returns:
        columns += [
            ("翌日", "right"),
            ("2日後", "right"),
            ("3日後", "right"),
            ("5日後", "right"),
        ]

    th_cells = ""
    for name, align in columns:
        th_cells += f'<th style="text-align:{align}">{name}</th>'

    rows_html = ""
    tv_base = "https://jp.tradingview.com/chart/M3vhlCeS/?symbol=TSE%3A"

    for c in candidates:
        code = c["code"]
        tv_url = f"{tv_base}{code}"

        code_cell = f'<td class="code-cell"><a class="code-link" href="{tv_url}" target="_blank">{code}</a></td>'
        name_cell = f'<td class="name-cell">{c["name"]}</td>'
        close_cell = f'<td>{c["close"]:,.1f}</td>'

        # RSI
        rsi = c["rsi"]
        rsi_style = ""
        if rsi <= 35:
            rsi_style = "background:rgba(16,185,129,0.2);color:#10b981;font-weight:bold"
        elif rsi <= 45:
            rsi_style = "background:rgba(16,185,129,0.1);color:#10b981"
        rsi_cell = f'<td style="{rsi_style}">{rsi:.1f}</td>'

        macd_cell = f'<td>{c["macd"]:.4f}</td>'
        signal_cell = f'<td>{c["macd_signal"]:.4f}</td>'

        def _pct_cell(val, scale=30):
            if val is None:
                return '<td>—</td>'
            intensity = min(int(abs(val) / scale * 70), 70)
            color = "#10b981" if val > 0 else "#ef4444"
            bg = "16,185,129" if val > 0 else "239,68,68"
            style = f"background:rgba({bg},0.{intensity:02d});color:{color};font-weight:bold"
            return f'<td style="{style}">{val:+.1f}%</td>'

        perf_3m_cell = _pct_cell(c.get("perf_3m"))

        dev = c.get("sma200_dev")
        dev_cell = _pct_cell(dev) if dev is not None else '<td>—</td>'

        rev = c.get("reversal_pos")
        if rev is not None:
            rev_style = "background:rgba(168,85,247,0.2);color:#a855f7;font-weight:bold" if rev < 25 else "background:rgba(168,85,247,0.12);color:#a855f7" if rev < 50 else "color:#64748b"
            rev_cell = f'<td style="{rev_style}">{rev:.1f}%</td>'
        else:
            rev_cell = '<td>—</td>'

        chg = c.get("change")
        chg_cell = _pct_cell(chg, scale=5) if chg is not None else '<td>—</td>'

        vol_cell = f'<td>{c["volume"]:,}</td>'

        cells = code_cell + name_cell + close_cell + rsi_cell + macd_cell + signal_cell + perf_3m_cell + dev_cell + rev_cell + chg_cell + vol_cell

        # リターン列
        if show_returns:
            for key in ["ret_1d", "ret_2d", "ret_3d", "ret_5d"]:
                cells += _pct_cell(c.get(key), scale=3)

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
th{{position:sticky;top:0;z-index:10;background:#1a2332;color:#8899aa;font-weight:600;padding:8px 10px;border-bottom:1px solid #2a3a4e;white-space:nowrap;cursor:pointer;user-select:none}}
th:hover{{background:#1e2d42!important}}
td{{padding:7px 10px;text-align:right;border-bottom:1px solid rgba(255,255,255,0.04);color:#111;white-space:nowrap}}
td.name-cell{{text-align:left;color:#111;max-width:180px;overflow:hidden;text-overflow:ellipsis}}
td.code-cell{{text-align:left}}
tr:hover td{{background:rgba(255,255,255,0.03)}}
.code-link{{display:inline-block;color:#a855f7;font-weight:700;text-decoration:none;background:rgba(168,85,247,0.1);border:1px solid rgba(168,85,247,0.4);border-radius:12px;padding:2px 10px;transition:background .15s,border-color .15s}}
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
  const table=document.querySelector('table'),tbody=table.querySelector('tbody'),ths=table.querySelectorAll('th');
  let sortColIdx=-1,sortAsc=true;
  ths.forEach(function(th,i){{
    th.addEventListener('click',function(){{
      if(sortColIdx===i){{sortAsc=!sortAsc}}else{{sortColIdx=i;sortAsc=true}}
      ths.forEach(function(t){{t.dataset.sort=''}});
      th.dataset.sort=sortAsc?'asc':'desc';
      const rows=Array.from(tbody.querySelectorAll('tr'));
      rows.sort(function(a,b){{
        const aR=(a.cells[i]?.innerText||'').trim(),bR=(b.cells[i]?.innerText||'').trim();
        const aN=parseFloat(aR.replace(/[,%+—]/g,'')),bN=parseFloat(bR.replace(/[,%+—]/g,''));
        if(aR==='—'&&bR!=='—')return 1;if(bR==='—'&&aR!=='—')return-1;
        const c=(!isNaN(aN)&&!isNaN(bN))?aN-bN:aR.localeCompare(bR,'ja');
        return sortAsc?c:-c;
      }});
      rows.forEach(function(r){{tbody.appendChild(r)}});
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
    if is_today:
        if not TV_API_AVAILABLE:
            st.error("❌ `tradingview-screener` パッケージがインストールされていません。")
        else:
            with st.spinner("⏳ TradingView APIでスクリーニング中..."):
                try:
                    candidates = run_bottom_reversal_today(
                        perf_threshold=perf_3m_threshold,
                        rsi_min=rsi_range[0],
                        rsi_max=rsi_range[1],
                        min_vol=min_volume,
                    )
                    st.session_state["br_candidates"] = candidates
                    st.session_state["br_run_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    st.session_state["br_is_backtest"] = False
                except Exception as e:
                    st.error(f"❌ スクリーニング失敗: {e}")
    else:
        if not YF_AVAILABLE:
            st.error("❌ `yfinance` パッケージがインストールされていません。")
        else:
            as_of_str = selected_date.strftime("%Y-%m-%d")
            with st.spinner(f"⏳ {as_of_str} のデータを取得・分析中...（全銘柄を検索するため数分かかります）"):
                try:
                    candidates = run_bottom_reversal_backtest(
                        as_of_date_str=as_of_str,
                        perf_threshold=perf_3m_threshold,
                        rsi_min=rsi_range[0],
                        rsi_max=rsi_range[1],
                        min_vol=min_volume,
                    )
                    st.session_state["br_candidates"] = candidates
                    st.session_state["br_run_at"] = f"{as_of_str}（バックテスト）"
                    st.session_state["br_is_backtest"] = True
                except Exception as e:
                    st.error(f"❌ バックテスト失敗: {e}")


# =====================================================
# 結果表示
# =====================================================
if "br_candidates" in st.session_state:
    candidates = st.session_state["br_candidates"]
    run_at = st.session_state.get("br_run_at", "")
    is_backtest = st.session_state.get("br_is_backtest", False)

    if not candidates:
        st.warning("⚠️ 条件に合致する銘柄がありませんでした。条件を緩めてみてください。")
    else:
        st.markdown(f"## 📋 スクリーニング結果（{run_at}）")

        # ---- KPI サマリー ----
        n = len(candidates)
        avg_rsi = sum(c["rsi"] for c in candidates) / n
        avg_rev = [c["reversal_pos"] for c in candidates if c.get("reversal_pos") is not None]
        avg_perf_3m = [c["perf_3m"] for c in candidates if c.get("perf_3m") is not None]

        if is_backtest:
            # バックテスト時: リターンのKPIも表示
            ret_1d = [c["ret_1d"] for c in candidates if c.get("ret_1d") is not None]
            col1, col2, col3, col4, col5 = st.columns(5)
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
                if ret_1d:
                    win_rate = sum(1 for r in ret_1d if r > 0) / len(ret_1d) * 100
                    st.metric("翌日勝率", f"{win_rate:.1f}%（{len(ret_1d)}件）")
                else:
                    st.metric("翌日勝率", "N/A")
            with col5:
                if ret_1d:
                    avg_ret = sum(ret_1d) / len(ret_1d)
                    st.metric("平均翌日リターン", f"{avg_ret:+.2f}%")
                else:
                    st.metric("平均翌日リターン", "N/A")
        else:
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
        table_html, table_height = _build_result_table(candidates, show_returns=is_backtest)
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
        1. **モード選択:** 「今日」or「過去日付を指定」
        2. **条件を調整**（サイドバー）
        3. **「スクリーニング実行」ボタンを押す**

        **2つのモード:**
        - 🟢 **今日:** TradingView API（約1秒）
        - 🟡 **過去日付:** yfinance バックテスト（数分）
          → 翌日〜5日後のリターンも表示
        """)
    with col2:
        st.markdown("""
        ### ⚡ 特徴
        - **今日モード:** リアルタイムデータで即時取得
        - **過去日付モード:** 過去のシグナルを検証し、
          **翌日以降のリターン**で実際の成績を確認可能

        **📌 ヒント:** 反発度が低い（20%以下）銘柄は底に近い
        初動段階で、リスク・リターンが高い銘柄です。
        """)

    st.markdown("---")
    st.caption("⚠️ 本ツールは投資助言を目的としたものではありません。")
