"""
米国株バックテストエンジン

指定した期間・条件（SMA5>SMA20>SMA60, 出来高100万以上）で
米国株の過去シグナルを再現し、翌日〜5日後のリターンを検証する。

使い方:
    # 期間バックテスト
    python3 us_backtester.py --start 2025-01-01 --end 2025-12-31

    # 特定日1日だけ確認
    python3 us_backtester.py --date 2025-10-15

    # 少ない銘柄で動作確認（開発用）
    python3 us_backtester.py --start 2025-01-06 --end 2025-01-10 --sample
"""

import argparse
import json
import math
import os
import sys
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import yfinance as yf

warnings.filterwarnings("ignore")


# =============================================================
# 定数
# =============================================================
# WikipediaのS&P 500銘柄一覧ページURL
SP500_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

# スクリーニング条件のデフォルト値
DEFAULT_MIN_VOLUME = 1_000_000   # 最低出来高: 100万株
DEFAULT_HIT_THRESHOLD = 2.0     # 達成フラグのリターン閾値 (%)
FORWARD_DAYS = [1, 2, 3, 4, 5]  # 先読み日数

# 並列実行のワーカー数
MAX_WORKERS = 8

# バッファ（SMA60計算用に検証開始日より前から取得する日数）
HISTORY_BUFFER_DAYS = 120

# 出力先
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "us_results")


# =============================================================
# 銘柄一覧取得
# =============================================================
def fetch_sp500_tickers() -> pd.DataFrame:
    """
    WikipediaからS&P 500銘柄一覧を取得する。
    取得失敗時はフォールバックリストを返す。

    Returns:
        pd.DataFrame: columns=[symbol, name, sector]
    """
    print("📥 S&P 500 銘柄一覧を取得中...")
    try:
        # WikipediaはUser-Agentがないとブロックするため、requestsで取得する
        import io as _io
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36"
        }
        resp = requests.get(SP500_WIKI_URL, headers=headers, timeout=30)
        resp.raise_for_status()
        tables = pd.read_html(_io.StringIO(resp.text))
        df = tables[0]
        df = df.rename(columns={
            "Symbol": "symbol",
            "Security": "name",
            "GICS Sector": "sector",
        })
        # 一部の銘柄コードに「.」が含まれる場合がある（例: BRK.B → BRK-B）
        df["symbol"] = df["symbol"].str.replace(".", "-", regex=False)
        print(f"  ✅ {len(df)}銘柄を取得しました")
        return df[["symbol", "name", "sector"]].reset_index(drop=True)
    except Exception as e:
        print(f"  ⚠️ S&P 500取得失敗（{e}）→ フォールバックリストを使用")
        return _fallback_tickers()


def _fallback_tickers() -> pd.DataFrame:
    """フォールバック用の代表的な米国株銘柄リスト（動作確認用）。"""
    tickers = [
        ("AAPL", "Apple Inc."), ("MSFT", "Microsoft Corp."),
        ("AMZN", "Amazon.com Inc."), ("GOOGL", "Alphabet Inc. (Class A)"),
        ("META", "Meta Platforms Inc."), ("TSLA", "Tesla Inc."),
        ("NVDA", "NVIDIA Corp."), ("JPM", "JPMorgan Chase & Co."),
        ("V", "Visa Inc."), ("JNJ", "Johnson & Johnson"),
        ("WMT", "Walmart Inc."), ("PG", "Procter & Gamble Co."),
        ("MA", "Mastercard Inc."), ("UNH", "UnitedHealth Group Inc."),
        ("HD", "The Home Depot Inc."), ("DIS", "The Walt Disney Co."),
        ("BAC", "Bank of America Corp."), ("ADBE", "Adobe Inc."),
        ("CRM", "Salesforce Inc."), ("NFLX", "Netflix Inc."),
        ("AMD", "Advanced Micro Devices Inc."), ("INTC", "Intel Corp."),
        ("PEP", "PepsiCo Inc."), ("KO", "The Coca-Cola Co."),
        ("COST", "Costco Wholesale Corp."), ("TMO", "Thermo Fisher Scientific Inc."),
        ("AVGO", "Broadcom Inc."), ("MRK", "Merck & Co. Inc."),
        ("ABBV", "AbbVie Inc."), ("LLY", "Eli Lilly and Co."),
        ("ACN", "Accenture plc"), ("MCD", "McDonald's Corp."),
        ("CSCO", "Cisco Systems Inc."), ("DHR", "Danaher Corp."),
        ("ABT", "Abbott Laboratories"), ("TXN", "Texas Instruments Inc."),
        ("QCOM", "Qualcomm Inc."), ("NEE", "NextEra Energy Inc."),
        ("LOW", "Lowe's Companies Inc."), ("PM", "Philip Morris International Inc."),
        ("UPS", "United Parcel Service Inc."), ("MS", "Morgan Stanley"),
        ("RTX", "RTX Corp."), ("GS", "Goldman Sachs Group Inc."),
        ("AMAT", "Applied Materials Inc."), ("ISRG", "Intuitive Surgical Inc."),
        ("CAT", "Caterpillar Inc."), ("DE", "Deere & Co."),
        ("AMGN", "Amgen Inc."), ("BLK", "BlackRock Inc."),
    ]
    return pd.DataFrame(tickers, columns=["symbol", "name"])


# =============================================================
# 株価履歴の一括取得
# =============================================================
def fetch_ticker_history(
    symbol: str,
    name: str,
    start_date: str,
    end_date: str,
) -> Optional[Tuple[str, str, pd.DataFrame]]:
    """
    単一銘柄の日足履歴を指定期間で取得する。

    Args:
        symbol: ティッカーシンボル（例: AAPL）
        name: 銘柄名
        start_date: 取得開始日（"YYYY-MM-DD"）※ SMA計算バッファ込み
        end_date: 取得終了日（"YYYY-MM-DD"）※ リターン計算のために数日先まで

    Returns:
        (symbol, name, DataFrame) or None（取得失敗・データ不足の場合）
    """
    try:
        ticker_obj = yf.Ticker(symbol)
        df = ticker_obj.history(start=start_date, end=end_date)
        if df.empty or len(df) < 30:
            return None
        # タイムゾーンがある場合は除外
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        else:
            df.index = pd.to_datetime(df.index)
        return (symbol, name, df)
    except Exception:
        return None


# =============================================================
# 特定日時点でのスクリーニング判定
# =============================================================
def screen_at_date(
    df: pd.DataFrame,
    as_of: pd.Timestamp,
    min_volume: int = DEFAULT_MIN_VOLUME,
    use_pullback: bool = False,
    near_high_pct: float = 0.0,
    near_high_days: int = 60,
) -> Optional[Dict[str, Any]]:
    """
    指定日時点（as_of 以前）のデータのみを使って条件を判定する。
    未来データの混入を防ぐため、df[df.index <= as_of] にフィルタしてから計算する。

    条件:
        1. SMA5 > SMA20 > SMA60（順行配列）
        2. 当日出来高 >= min_volume
        3. [任意] use_pullback=True のとき
           価格 < SMA5 かつ 価格 > SMA20（5MA下・20MA上のプルバック）
        4. [任意] near_high_pct > 0 のとき
           終値が直近N日高値からnear_high_pct%以内

    Returns:
        条件合致時は指標値の辞書、不合致時は None
    """
    # as_of 以前のデータのみを使用（未来データ漏洩防止）
    past_df = df[df.index <= as_of].copy()

    if len(past_df) < 30:
        return None

    # 移動平均線の計算（過去データのみで）
    past_df["SMA5"] = past_df["Close"].rolling(5).mean()
    past_df["SMA20"] = past_df["Close"].rolling(20).mean()
    past_df["SMA60"] = past_df["Close"].rolling(60).mean()
    past_df["VolMA20"] = past_df["Volume"].rolling(20).mean()

    latest = past_df.iloc[-1]

    # NaN チェック
    for c in ["SMA5", "SMA20", "SMA60", "Volume"]:
        if pd.isna(latest[c]):
            return None

    sma5 = float(latest["SMA5"])
    sma20 = float(latest["SMA20"])
    sma60 = float(latest["SMA60"])
    close = float(latest["Close"])
    volume = int(latest["Volume"])
    vol_ma20 = float(latest["VolMA20"]) if not pd.isna(latest["VolMA20"]) else 0

    # 基本条件判定（SMA順行配列 + 出来高）
    if not (sma5 > sma20 > sma60 and volume >= min_volume):
        return None

    # プルバック条件（オプション）
    if use_pullback and not (close < sma5 and close > sma20):
        return None

    # 直近高値条件（オプション）
    recent_high = None
    if near_high_pct > 0:
        window = past_df.tail(near_high_days)
        recent_high = float(window["High"].max()) if "High" in window.columns else float(window["Close"].max())
        distance_pct = (recent_high - close) / recent_high * 100
        if distance_pct > near_high_pct:
            return None

    volume_ratio = round(volume / vol_ma20, 2) if vol_ma20 > 0 else 0

    # 出来高増減率を計算する
    if len(past_df) >= 3:
        vol_yesterday = float(past_df.iloc[-2]["Volume"])
        vol_day_before = float(past_df.iloc[-3]["Volume"])
        if vol_day_before > 0:
            volume_change_pct = round((vol_yesterday - vol_day_before) / vol_day_before * 100, 1)
        else:
            volume_change_pct = None
    else:
        volume_change_pct = None

    # 当日の前日比騰落率を計算する
    if len(past_df) >= 2:
        prev_close = float(past_df.iloc[-2]["Close"])
        day_change_pct = round((close - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0.0
    else:
        prev_close = None
        day_change_pct = 0.0

    # 前日の前々日比騰落率
    if len(past_df) >= 3 and prev_close is not None:
        prev_prev_close = float(past_df.iloc[-3]["Close"])
        prev_day_change_pct = round((prev_close - prev_prev_close) / prev_prev_close * 100, 2) if prev_prev_close > 0 else None
    else:
        prev_prev_close = None
        prev_day_change_pct = None

    # 前々日の騰落率
    if len(past_df) >= 4 and prev_prev_close is not None:
        prev_prev_prev_close = float(past_df.iloc[-4]["Close"])
        prev_prev_day_change_pct = round((prev_prev_close - prev_prev_prev_close) / prev_prev_prev_close * 100, 2) if prev_prev_prev_close > 0 else None
    else:
        prev_prev_day_change_pct = None

    # RSI(14) を計算する（Wilder の指数平滑平均方式）
    rsi_period = 14
    rsi_val = None
    if len(past_df) >= rsi_period + 1:
        delta = past_df["Close"].diff()
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        avg_gain = gain.ewm(alpha=1 / rsi_period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / rsi_period, adjust=False).mean()
        rs = avg_gain.iloc[-1] / avg_loss.iloc[-1] if avg_loss.iloc[-1] != 0 else float("inf")
        rsi_val = round(100 - 100 / (1 + rs), 1)

    # ---- スコアリング用の追加指標 ----
    open_price  = float(latest["Open"])  if "Open"  in past_df.columns else close
    high_price  = float(latest["High"])  if "High"  in past_df.columns else close
    low_price   = float(latest["Low"])   if "Low"   in past_df.columns else close

    # 20日高値
    window20 = past_df.tail(20)
    hh20 = float(window20["High"].max()) if "High" in window20.columns else float(window20["Close"].max())

    # 当日出来高 / 前日出来高 の比率
    vol_today_vs_yday_pct: Optional[float] = None
    if len(past_df) >= 2:
        vol_prev = float(past_df.iloc[-2]["Volume"])
        if vol_prev > 0:
            vol_today_vs_yday_pct = round((volume / vol_prev - 1) * 100, 1)

    # 位置判定
    is_pullback = bool(close < sma5 and close > sma20)
    is_breakout = bool(close >= hh20)

    # 長大上ヒゲ判定
    price_range   = high_price - low_price
    upper_wick    = high_price - max(open_price, close)
    upper_wick_ratio = upper_wick / price_range if price_range > 0 else 0.0
    long_upper_wick = bool(upper_wick_ratio >= 0.55)

    # 高値圏終盤判定
    dist_to_hh20 = (hh20 - close) / close if close > 0 else 1.0
    is_high_zone = bool(dist_to_hh20 <= 0.03 and close > sma20)

    # 大陰線直後の判定
    big_bearish_yesterday = False
    if len(past_df) >= 2 and "Open" in past_df.columns:
        prev = past_df.iloc[-2]
        p_open  = float(prev["Open"])
        p_close = float(prev["Close"])
        p_high  = float(prev["High"])  if "High" in past_df.columns else p_close
        p_low   = float(prev["Low"])   if "Low"  in past_df.columns else p_close
        p_range = p_high - p_low
        p_body  = abs(p_close - p_open)
        if p_range > 0 and p_close < p_open:
            body10 = (past_df["Close"] - past_df["Open"]).abs().tail(10).mean()
            if (p_body / p_range >= 0.65) and (p_body >= 1.5 * body10):
                big_bearish_yesterday = True

    # 週足SMA20判定（日足SMA100で代替）
    weekly_sma20_ok = False
    if len(past_df) >= 100:
        sma100 = past_df["Close"].rolling(100).mean().iloc[-1]
        if not pd.isna(sma100):
            weekly_sma20_ok = bool(close > float(sma100))

    # ---- 初回SMA20タッチ判定 ----
    first_sma20_touch = False
    sma20_touch_count = 0
    trend_start_days_ago = 0

    if len(past_df) >= 20 and "SMA5" in past_df.columns and "SMA20" in past_df.columns and "SMA60" in past_df.columns:
        _sma5_s  = past_df["SMA5"]
        _sma20_s = past_df["SMA20"]
        _sma60_s = past_df["SMA60"]
        _low_s   = past_df["Low"] if "Low" in past_df.columns else past_df["Close"]

        trend_start_idx = len(past_df) - 1
        for k in range(len(past_df) - 2, -1, -1):
            s5  = _sma5_s.iloc[k]
            s20 = _sma20_s.iloc[k]
            s60 = _sma60_s.iloc[k]
            if pd.isna(s5) or pd.isna(s20) or pd.isna(s60):
                trend_start_idx = k + 1
                break
            if not (s5 > s20 > s60):
                trend_start_idx = k + 1
                break
            trend_start_idx = k

        trend_start_days_ago = len(past_df) - 1 - trend_start_idx

        _touch_threshold = 0.015
        _in_touch = False
        for k in range(trend_start_idx, len(past_df)):
            _s20_val = float(_sma20_s.iloc[k])
            _low_val = float(_low_s.iloc[k])
            if _s20_val > 0:
                _dist = (_low_val - _s20_val) / _s20_val
                if _dist <= _touch_threshold:
                    if not _in_touch:
                        sma20_touch_count += 1
                        _in_touch = True
                else:
                    _in_touch = False

        _today_dist = (low_price - sma20) / sma20 if sma20 > 0 else 999
        first_sma20_touch = bool(sma20_touch_count == 1 and _today_dist <= _touch_threshold)

    # 曜日（0=月曜, 4=金曜）
    day_of_week = int(as_of.weekday())

    return {
        "close": round(close, 2),
        "prev_prev_day_change_pct": prev_prev_day_change_pct,
        "prev_day_change_pct": prev_day_change_pct,
        "day_change_pct": day_change_pct,
        "rsi": rsi_val,
        "recent_high": round(recent_high, 2) if recent_high is not None else None,
        "sma5": round(sma5, 2),
        "sma20": round(sma20, 2),
        "sma60": round(sma60, 2),
        "volume": volume,
        "volume_ma20": round(vol_ma20, 0),
        "volume_ratio": volume_ratio,
        "volume_change_pct": volume_change_pct,
        # ---- スコアリング用追加フィールド ----
        "open_price":  round(open_price,  2),
        "high_price":  round(high_price,  2),
        "low_price":   round(low_price,   2),
        "hh20":        round(hh20,        2),
        "vol_today_vs_yday_pct": vol_today_vs_yday_pct,
        "is_pullback":           is_pullback,
        "is_breakout":           is_breakout,
        "long_upper_wick":       long_upper_wick,
        "is_high_zone":          is_high_zone,
        "big_bearish_yesterday": big_bearish_yesterday,
        "weekly_sma20_ok":       weekly_sma20_ok,
        "first_sma20_touch":     first_sma20_touch,
        "sma20_touch_count":     sma20_touch_count,
        "trend_start_days_ago":  trend_start_days_ago,
        "day_of_week":           day_of_week,
    }


# =============================================================
# N日後リターン計算
# =============================================================
def calc_forward_returns(
    df: pd.DataFrame,
    signal_date: pd.Timestamp,
    hit_threshold: float = DEFAULT_HIT_THRESHOLD,
) -> Dict[str, Any]:
    """
    シグナル発生日の翌日〜5日後のリターンを計算する。

    Args:
        df: 当該銘柄の日足DataFrame（signal_date以降のデータを含む）
        signal_date: シグナル発生日（スクリーニング判定日）
        hit_threshold: 達成フラグの閾値（%）

    Returns:
        各日数のリターン・達成フラグを含む辞書
    """
    past_df_for_base = df[df.index <= signal_date]
    if past_df_for_base.empty:
        return {f"ret_{n}d": None for n in FORWARD_DAYS} | \
               {f"hit_{hit_threshold:.0f}pct_{n}d": None for n in FORWARD_DAYS} | \
               {"next_close": None, "pos_within_3d": None, "pos_within_5d": None}
    base_close = float(past_df_for_base.iloc[-1]["Close"])

    future_df = df[df.index > signal_date].copy()

    result = {}
    for idx, n in enumerate(FORWARD_DAYS):
        key_ret = f"ret_{n}d"
        key_hit = f"hit_{hit_threshold:.0f}pct_{n}d"
        if len(future_df) >= n and base_close is not None and base_close > 0:
            fwd_close = float(future_df.iloc[n - 1]["Close"])
            if n == 1:
                prev_for_ret = base_close
            else:
                prev_for_ret = float(future_df.iloc[n - 2]["Close"])
            if prev_for_ret > 0:
                ret = round((fwd_close - prev_for_ret) / prev_for_ret * 100, 3)
            else:
                ret = None
            result[key_ret] = ret
            cum_ret = round((fwd_close - base_close) / base_close * 100, 3)
            result[key_hit] = 1 if cum_ret >= hit_threshold else 0
        else:
            result[key_ret] = None
            result[key_hit] = None

    if len(future_df) >= 1:
        result["next_close"] = round(float(future_df.iloc[0]["Close"]), 2)
    else:
        result["next_close"] = None

    # 3日以内・5日以内プラスの判定
    pos_within_3d = None
    if len(future_df) > 0 and base_close is not None and base_close > 0:
        days_to_check_3 = min(len(future_df), 3)
        window_3d = future_df.iloc[:days_to_check_3]
        max_close_3d = float(window_3d["Close"].max())
        if max_close_3d > base_close:
            pos_within_3d = 1
        elif len(future_df) >= 3:
            pos_within_3d = 0

    pos_within_5d = None
    if len(future_df) > 0 and base_close is not None and base_close > 0:
        days_to_check_5 = min(len(future_df), 5)
        window_5d = future_df.iloc[:days_to_check_5]
        max_close_5d = float(window_5d["Close"].max())
        if max_close_5d > base_close:
            pos_within_5d = 1
        elif len(future_df) >= 5:
            pos_within_5d = 0

    result["pos_within_3d"] = pos_within_3d
    result["pos_within_5d"] = pos_within_5d

    return result


# =============================================================
# 期間バックテスト本体
# =============================================================
def run_backtest(
    tickers_df: pd.DataFrame,
    start_date: str,
    end_date: str,
    min_volume: int = DEFAULT_MIN_VOLUME,
    hit_threshold: float = DEFAULT_HIT_THRESHOLD,
) -> pd.DataFrame:
    """
    指定期間のすべての営業日に対してスクリーニングを適用し、
    翌日〜5日後のリターンを含む結果テーブルを返す。

    Args:
        tickers_df: 銘柄リスト（symbol, name列を含む）
        start_date: 検証開始日
        end_date: 検証終了日
        min_volume: 最低出来高
        hit_threshold: 達成フラグの閾値（%）

    Returns:
        pd.DataFrame: 全シグナル行を含む結果テーブル
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    fetch_start = (start_dt - timedelta(days=HISTORY_BUFFER_DAYS)).strftime("%Y-%m-%d")
    fetch_end = (end_dt + timedelta(days=14)).strftime("%Y-%m-%d")

    total = len(tickers_df)
    print(f"\n📥 米国株 株価データ取得中（{total}銘柄 / {fetch_start} 〜 {fetch_end}）...")
    print("   ※ SMA60バッファ込みで取得するため少し時間がかかります")
    print()

    # ---- 並列でデータ取得 ----
    all_data: Dict[str, Tuple[str, pd.DataFrame]] = {}
    completed = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for _, row in tickers_df.iterrows():
            symbol = str(row["symbol"])
            name = str(row.get("name", ""))
            f = executor.submit(fetch_ticker_history, symbol, name, fetch_start, fetch_end)
            futures[f] = (symbol, name)

        for f in as_completed(futures):
            completed += 1
            symbol, name = futures[f]
            try:
                res = f.result()
                if res is not None:
                    _, _, df = res
                    all_data[symbol] = (name, df)
                if completed % 100 == 0 or completed == total:
                    pct = completed / total * 100
                    print(f"  ⏳ [{completed}/{total}] {pct:.0f}% 完了... "
                          f"（有効データ: {len(all_data)}件）")
            except Exception:
                errors += 1

    print(f"\n  ✅ データ取得完了（成功: {len(all_data)}件 / エラー: {errors}件）")

    # ---- 検証対象の営業日リストを生成 ----
    biz_days: set = set()
    for _, (_, df) in all_data.items():
        dates_in_range = df.index[
            (df.index >= start_dt) & (df.index <= end_dt)
        ]
        biz_days.update(dates_in_range.tolist())
    biz_days_sorted = sorted(biz_days)

    print(f"\n🔍 米国株バックテスト開始")
    print(f"   検証期間: {start_date} 〜 {end_date}")
    print(f"   営業日数: {len(biz_days_sorted)}日")
    print(f"   条件: SMA5>SMA20>SMA60, 出来高≥{min_volume:,}株")
    print()

    # ---- 各営業日 × 各銘柄で判定 ----
    rows = []
    total_days = len(biz_days_sorted)

    for day_idx, signal_date in enumerate(biz_days_sorted):
        signal_ts = pd.Timestamp(signal_date)
        day_hits = 0

        for symbol, (name, df) in all_data.items():
            screen_result = screen_at_date(df, signal_ts, min_volume)
            if screen_result is None:
                continue

            fwd_returns = calc_forward_returns(df, signal_ts, hit_threshold)

            row = {
                "date": signal_ts.strftime("%Y-%m-%d"),
                "ticker": symbol,
                "name": name,
                **screen_result,
                **fwd_returns,
            }
            rows.append(row)
            day_hits += 1

        if (day_idx + 1) % 10 == 0 or day_idx + 1 == total_days:
            print(f"  📅 [{day_idx + 1}/{total_days}] "
                  f"{signal_ts.strftime('%Y-%m-%d')} 完了 "
                  f"（本日のヒット: {day_hits}件）")

    print(f"\n✅ 米国株バックテスト完了！ 総シグナル数: {len(rows)}件")

    if not rows:
        return pd.DataFrame()

    result_df = pd.DataFrame(rows)

    # 列の順序を整理
    col_order = [
        "date", "ticker", "name",
        "close", "sma5", "sma20", "sma60",
        "volume", "volume_ma20", "volume_ratio",
        "next_close",
        "ret_1d", "ret_2d", "ret_3d", "ret_5d",
        f"hit_{hit_threshold:.0f}pct_1d",
        f"hit_{hit_threshold:.0f}pct_2d",
        f"hit_{hit_threshold:.0f}pct_3d",
        f"hit_{hit_threshold:.0f}pct_5d",
    ]
    existing_cols = [c for c in col_order if c in result_df.columns]
    extra_cols = [c for c in result_df.columns if c not in col_order]
    result_df = result_df[existing_cols + extra_cols]

    return result_df


# =============================================================
# 単日スクリーニング（過去日付指定）
# =============================================================
def run_single_date_screen(
    tickers_df: pd.DataFrame,
    as_of_date: str,
    min_volume: int = DEFAULT_MIN_VOLUME,
) -> pd.DataFrame:
    """
    特定の過去日付1日分のスクリーニングを実行する。
    """
    return run_backtest(
        tickers_df=tickers_df,
        start_date=as_of_date,
        end_date=as_of_date,
        min_volume=min_volume,
    )


# =============================================================
# 統計集計
# =============================================================
def summarize_results(
    df: pd.DataFrame,
    hit_threshold: float = DEFAULT_HIT_THRESHOLD,
) -> Dict[str, Any]:
    """
    バックテスト結果から統計サマリーを生成する。
    """
    if df.empty:
        return {}

    hit_col = f"hit_{hit_threshold:.0f}pct_1d"
    ret_col = "ret_1d"

    valid = df[df[ret_col].notna()].copy()

    n_signals = len(df)
    n_valid = len(valid)
    n_dates = df["date"].nunique()
    n_tickers = df["ticker"].nunique()

    avg_ret_1d = round(valid[ret_col].mean(), 3) if n_valid > 0 else None
    win_rate = round(
        (valid[ret_col] > 0).sum() / n_valid * 100, 1
    ) if n_valid > 0 else None

    hit_valid = valid[valid[hit_col].notna()]
    hit_rate_1d = round(
        hit_valid[hit_col].mean() * 100, 1
    ) if len(hit_valid) > 0 else None

    ret_stats = {}
    for n in FORWARD_DAYS:
        col_name = f"ret_{n}d"
        if col_name in df.columns:
            v = df[df[col_name].notna()][col_name]
            ret_stats[col_name] = {
                "mean": round(v.mean(), 3) if len(v) > 0 else None,
                "median": round(v.median(), 3) if len(v) > 0 else None,
                "win_rate": round((v > 0).sum() / len(v) * 100, 1) if len(v) > 0 else None,
                "max": round(v.max(), 3) if len(v) > 0 else None,
                "min": round(v.min(), 3) if len(v) > 0 else None,
            }

    hit_rates = {}
    for n in FORWARD_DAYS:
        hcol = f"hit_{hit_threshold:.0f}pct_{n}d"
        if hcol in df.columns:
            hv = df[df[hcol].notna()][hcol]
            hit_rates[f"{n}d"] = round(hv.mean() * 100, 1) if len(hv) > 0 else None

    top_tickers_list = []
    if hit_col in df.columns:
        top_tickers = (
            valid[valid[hit_col] == 1]
            .groupby(["ticker", "name"])
            .agg(
                hit_count=(hit_col, "sum"),
                avg_ret=(ret_col, "mean"),
                signal_count=("date", "count"),
            )
            .reset_index()
            .sort_values("hit_count", ascending=False)
            .head(20)
        )
        top_tickers["avg_ret"] = top_tickers["avg_ret"].round(3)
        top_tickers_list = top_tickers.to_dict(orient="records")

    return {
        "n_signals": n_signals,
        "n_valid": n_valid,
        "n_dates": n_dates,
        "n_tickers": n_tickers,
        "hit_threshold_pct": hit_threshold,
        "avg_ret_1d": avg_ret_1d,
        "win_rate_1d": win_rate,
        "hit_rate_1d": hit_rate_1d,
        "hit_rates_by_day": hit_rates,
        "ret_stats_by_day": ret_stats,
        "top_tickers": top_tickers_list,
    }


# =============================================================
# 結果の保存
# =============================================================
def save_backtest_results(
    df: pd.DataFrame,
    summary: Dict[str, Any],
    tag: str = "",
) -> Tuple[str, str]:
    """
    バックテスト結果をCSVとJSONで保存する。

    Returns:
        (csv_path, json_path) のタプル
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    now = datetime.now()
    suffix = f"_{tag}" if tag else ""
    csv_filename = f"us_backtest{suffix}_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    json_filename = f"us_backtest{suffix}_{now.strftime('%Y%m%d_%H%M%S')}_summary.json"

    csv_path = os.path.join(OUTPUT_DIR, csv_filename)
    json_path = os.path.join(OUTPUT_DIR, json_filename)

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    # 最新版もコピー
    latest_csv = os.path.join(OUTPUT_DIR, "us_backtest_latest.csv")
    latest_json = os.path.join(OUTPUT_DIR, "us_backtest_latest_summary.json")
    df.to_csv(latest_csv, index=False, encoding="utf-8-sig")
    with open(latest_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"\n💾 結果を保存しました:")
    print(f"   CSV: {csv_path}")
    print(f"   Summary: {json_path}")
    return csv_path, json_path


# =============================================================
# エントリーポイント
# =============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="米国株バックテストエンジン")
    parser.add_argument("--start", type=str, help="検証開始日 (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="検証終了日 (YYYY-MM-DD)")
    parser.add_argument("--date", type=str, help="特定日1日だけ検証 (YYYY-MM-DD)")
    parser.add_argument("--sample", action="store_true",
                        help="フォールバック銘柄（50銘柄）で高速実行")
    args = parser.parse_args()

    start_time = time.time()

    # 銘柄リスト
    if args.sample:
        tickers_df = _fallback_tickers()
    else:
        tickers_df = fetch_sp500_tickers()

    # 期間の決定
    if args.date:
        s, e = args.date, args.date
    elif args.start and args.end:
        s, e = args.start, args.end
    else:
        print("❌ --start/--end または --date を指定してください")
        sys.exit(1)

    result_df = run_backtest(tickers_df, s, e)

    if not result_df.empty:
        summary = summarize_results(result_df)
        save_backtest_results(result_df, summary,
                              tag=f"{s.replace('-', '')}_{e.replace('-', '')}")

        print(f"\n📊 サマリー:")
        print(f"   シグナル数: {summary.get('n_signals', 0)}")
        print(f"   翌日勝率:  {summary.get('win_rate_1d', 'N/A')}%")
        print(f"   平均リターン: {summary.get('avg_ret_1d', 'N/A')}%")

    elapsed = time.time() - start_time
    print(f"\n⏱  実行時間: {elapsed:.1f}秒")
