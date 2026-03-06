"""
レベル1 バックテストエンジン

指定した期間・条件（SMA5>SMA20>SMA60, 出来高N万以上）で
日本株の過去シグナルを再現し、翌日〜5日後のリターンを検証する。

使い方:
    # 期間バックテスト
    python3 backtester.py --start 2025-01-01 --end 2025-12-31

    # 特定日1日だけ確認
    python3 backtester.py --date 2025-10-15

    # 少ない銘柄で動作確認（開発用）
    python3 backtester.py --start 2025-01-06 --end 2025-01-10 --sample
"""

import argparse
import io
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
JPX_CSV_URL = (
    "https://www.jpx.co.jp/markets/statistics-equities/misc/"
    "tvdivq0000001vg2-att/data_j.xls"
)

# スクリーニング条件のデフォルト値
DEFAULT_MIN_VOLUME = 500_000   # 最低出来高: 50万株
DEFAULT_HIT_THRESHOLD = 2.0   # 達成フラグのリターン閾値 (%)
FORWARD_DAYS = [1, 2, 3, 4, 5]  # 先読み日数

# 並列実行のワーカー数
MAX_WORKERS = 8

# バッファ（SMA60計算用に検証開始日より前から取得する日数）
# 年末年始など祝日が多い期間でも対応できるよう120日に設定
HISTORY_BUFFER_DAYS = 120

# 出力先
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "results")


# =============================================================
# 銘柄一覧取得
# =============================================================
def fetch_jpx_tickers() -> pd.DataFrame:
    """
    JPX上場銘柄一覧（全市場の内国普通株）を取得する。
    取得失敗時はフォールバックリストを返す。

    Returns:
        pd.DataFrame: columns=[code, name, market, sector]
    """
    print("📥 JPX上場銘柄一覧を取得中...")
    try:
        resp = requests.get(JPX_CSV_URL, timeout=30)
        resp.raise_for_status()
        df = pd.read_excel(io.BytesIO(resp.content))
        df = df.rename(columns={
            "コード": "code",
            "銘柄名": "name",
            "市場・商品区分": "market",
            "33業種区分": "sector",
        })
        df = df[pd.to_numeric(df["code"], errors="coerce").notna()]
        df["code"] = df["code"].astype(int)
        stock_markets = [
            "プライム（内国株式）",
            "スタンダード（内国株式）",
            "グロース（内国株式）",
        ]
        df = df[df["market"].isin(stock_markets)]
        print(f"  ✅ {len(df)}銘柄を取得しました")
        return df[["code", "name", "market", "sector"]].reset_index(drop=True)
    except Exception as e:
        print(f"  ⚠️  JPX取得失敗（{e}）→ フォールバックリストを使用")
        return _fallback_tickers()


def _fallback_tickers() -> pd.DataFrame:
    """フォールバック用の代表的な銘柄リスト（動作確認用）。"""
    tickers = [
        (7203, "トヨタ自動車"), (6758, "ソニーグループ"), (9984, "ソフトバンクグループ"),
        (6861, "キーエンス"), (8035, "東京エレクトロン"), (9983, "ファーストリテイリング"),
        (6501, "日立製作所"), (6902, "デンソー"), (7741, "HOYA"),
        (4063, "信越化学工業"), (6098, "リクルートHD"), (8306, "三菱UFJ"),
        (4519, "中外製薬"), (7974, "任天堂"), (4568, "第一三共"),
        (9432, "NTT"), (6367, "ダイキン工業"), (3382, "セブン&アイHD"),
        (6981, "村田製作所"), (4661, "OLC"), (6273, "SMC"),
        (7267, "ホンダ"), (8001, "伊藤忠商事"), (4502, "武田薬品"),
        (8058, "三菱商事"), (6857, "アドバンテスト"), (7751, "キヤノン"),
        (9433, "KDDI"), (6723, "ルネサス"), (4543, "テルモ"),
        (3407, "旭化成"), (6326, "クボタ"), (7011, "三菱重工業"),
        (8031, "三井物産"), (4901, "富士フイルムHD"), (6702, "富士通"),
        (6752, "パナソニックHD"), (8316, "三井住友FG"), (9434, "ソフトバンク"),
        (8766, "東京海上HD"), (4507, "塩野義製薬"), (6971, "京セラ"),
        (5108, "ブリヂストン"), (7269, "スズキ"), (2802, "味の素"),
        (2914, "JT"), (6503, "三菱電機"), (6594, "日本電産"),
        (7832, "バンダイナムコHD"), (4661, "OLC"),
    ]
    return pd.DataFrame(tickers, columns=["code", "name"])


# =============================================================
# 株価履歴の一括取得
# =============================================================
def fetch_ticker_history(
    code: int,
    name: str,
    start_date: str,
    end_date: str,
) -> Optional[Tuple[int, str, pd.DataFrame]]:
    """
    単一銘柄の日足履歴を指定期間で取得する。

    Args:
        code: 銘柄コード（例: 7203）
        name: 銘柄名
        start_date: 取得開始日（"YYYY-MM-DD"）※ SMA計算バッファ込み
        end_date: 取得終了日（"YYYY-MM-DD"）※ リターン計算のために数日先まで

    Returns:
        (code, name, DataFrame) or None（取得失敗・データ不足の場合）
    """
    symbol = f"{code}.T"
    try:
        ticker_obj = yf.Ticker(symbol)
        df = ticker_obj.history(start=start_date, end=end_date)
        if df.empty or len(df) < 30:
            return None
        # タイムゾーンがある場合は除外（日本株は Asia/Tokyo 仙時で返ってくる）
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        else:
            df.index = pd.to_datetime(df.index)
        return (code, name, df)
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

    Args:
        df: 当該銘柄の日足DataFrame
        as_of: 判定基準日
        min_volume: 最低出来高
        use_pullback: Trueにすると「価格<SMA5 かつ 価格>SMA20」の押し目条件を追加
        near_high_pct: 0超の場合、直近高値からの乖離率(下派)の許容度%
        near_high_days: 直近高値を算出する期間（営楮日数）

    Returns:
        条件合致時は指標値の辞書、不合致時は None
    """
    # as_of 以前のデータのみを使用（未来データ漏洩防止）
    past_df = df[df.index <= as_of].copy()

    # SMA60の計算に最低30行必要（不足の場合はNaNチェックで自然にNoneを返す）
    if len(past_df) < 30:
        return None

    # 移動平均線の計算（過去データのみで）
    past_df["SMA5"] = past_df["Close"].rolling(5).mean()
    past_df["SMA20"] = past_df["Close"].rolling(20).mean()
    past_df["SMA60"] = past_df["Close"].rolling(60).mean()
    past_df["VolMA20"] = past_df["Volume"].rolling(20).mean()

    latest = past_df.iloc[-1]

    # NaN チェック
    for col in ["SMA5", "SMA20", "SMA60", "Volume"]:
        if pd.isna(latest[col]):
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

    # プルバック条件（オプション）: 価格 < SMA5 かつ 価格 > SMA20
    if use_pullback and not (close < sma5 and close > sma20):
        return None

    # 直近高値条件（オプション）: 終値が直近N日のHigh最大値から near_high_pct% 以内
    recent_high = None
    if near_high_pct > 0:
        window = past_df.tail(near_high_days)
        recent_high = float(window["High"].max()) if "High" in window.columns else float(window["Close"].max())
        distance_pct = (recent_high - close) / recent_high * 100
        if distance_pct > near_high_pct:
            return None

    volume_ratio = round(volume / vol_ma20, 2) if vol_ma20 > 0 else 0

    # 一昨日→昨日の出来高増減率を計算する
    if len(past_df) >= 3:
        vol_yesterday = float(past_df.iloc[-2]["Volume"])
        vol_day_before = float(past_df.iloc[-3]["Volume"])
        if vol_day_before > 0:
            volume_change_pct = round((vol_yesterday - vol_day_before) / vol_day_before * 100, 1)
        else:
            volume_change_pct = None
    else:
        volume_change_pct = None

    # 当日の前日比騰落率を計算する（「当日」列）
    if len(past_df) >= 2:
        prev_close = float(past_df.iloc[-2]["Close"])
        day_change_pct = round((close - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0.0
    else:
        prev_close = None
        day_change_pct = 0.0

    # 前日の前々日比騰落率を計算する（「昨日」列）
    if len(past_df) >= 3 and prev_close is not None:
        prev_prev_close = float(past_df.iloc[-3]["Close"])
        prev_day_change_pct = round((prev_close - prev_prev_close) / prev_prev_close * 100, 2) if prev_prev_close > 0 else None
    else:
        prev_prev_close = None
        prev_day_change_pct = None

    # 前々日の騰落率を計算する（「一昨日」列）
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

    # ---- スコアリング用の追加指標を計算する ----

    # 当日の OHLC を取得する
    open_price  = float(latest["Open"])  if "Open"  in past_df.columns else close
    high_price  = float(latest["High"])  if "High"  in past_df.columns else close
    low_price   = float(latest["Low"])   if "Low"   in past_df.columns else close

    # 20 日高値（長大上ヒゲ・高値圏判定に使用）
    window20 = past_df.tail(20)
    hh20 = float(window20["High"].max()) if "High" in window20.columns else float(window20["Close"].max())

    # 当日出来高 / 前日出来高 の比率（110〜130% 判定用）
    vol_today_vs_yday_pct: Optional[float] = None
    if len(past_df) >= 2:
        vol_prev = float(past_df.iloc[-2]["Volume"])
        if vol_prev > 0:
            vol_today_vs_yday_pct = round((volume / vol_prev - 1) * 100, 1)

    # 位置判定：押し目（SMA5 下・SMA20 上）または初動（20 日高値ブレイク）
    is_pullback = bool(close < sma5 and close > sma20)
    is_breakout = bool(close >= hh20)

    # 長大上ヒゲ判定（UpperWick / Range >= 0.55）
    price_range   = high_price - low_price
    upper_wick    = high_price - max(open_price, close)
    upper_wick_ratio = upper_wick / price_range if price_range > 0 else 0.0
    long_upper_wick = bool(upper_wick_ratio >= 0.55)

    # 高値圏終盤判定（20 日高値から 3% 以内 かつ SMA20 より上）
    dist_to_hh20 = (hh20 - close) / close if close > 0 else 1.0
    is_high_zone = bool(dist_to_hh20 <= 0.03 and close > sma20)

    # 大陰線直後の判定（前日ローソク足を使用）
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
            # 過去 10 日のボディ平均
            body10 = (past_df["Close"] - past_df["Open"]).abs().tail(10).mean()
            if (p_body / p_range >= 0.65) and (p_body >= 1.5 * body10):
                big_bearish_yesterday = True

    # 週足 SMA20 判定（20週 ≈ 日足 SMA100 で代替：resample よりも確実）
    weekly_sma20_ok = False
    if len(past_df) >= 100:
        sma100 = past_df["Close"].rolling(100).mean().iloc[-1]
        if not pd.isna(sma100):
            weekly_sma20_ok = bool(close > float(sma100))

    # 曜日（0=月曜, 4=金曜）
    day_of_week = int(as_of.weekday())

    return {
        "close": round(close, 1),
        "prev_prev_day_change_pct": prev_prev_day_change_pct,
        "prev_day_change_pct": prev_day_change_pct,
        "day_change_pct": day_change_pct,
        "rsi": rsi_val,
        "recent_high": round(recent_high, 1) if recent_high is not None else None,
        "sma5": round(sma5, 1),
        "sma20": round(sma20, 1),
        "sma60": round(sma60, 1),
        "volume": volume,
        "volume_ma20": round(vol_ma20, 0),
        "volume_ratio": volume_ratio,
        "volume_change_pct": volume_change_pct,
        # ---- スコアリング用追加フィールド ----
        "open_price":  round(open_price,  1),
        "high_price":  round(high_price,  1),
        "low_price":   round(low_price,   1),
        "hh20":        round(hh20,        1),
        "vol_today_vs_yday_pct": vol_today_vs_yday_pct,
        "is_pullback":           is_pullback,
        "is_breakout":           is_breakout,
        "long_upper_wick":       long_upper_wick,
        "is_high_zone":          is_high_zone,
        "big_bearish_yesterday": big_bearish_yesterday,
        "weekly_sma20_ok":       weekly_sma20_ok,
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
    # signal_date の終値（祠日・休市日の場合は直近の前営楮日を使用）
    past_df_for_base = df[df.index <= signal_date]
    if past_df_for_base.empty:
        return {f"ret_{n}d": None for n in FORWARD_DAYS} | \
               {f"hit_{hit_threshold:.0f}pct_{n}d": None for n in FORWARD_DAYS} | \
               {"next_close": None, "pos_within_3d": None, "pos_within_5d": None}
    base_close = float(past_df_for_base.iloc[-1]["Close"])

    # signal_date より後の営業日リスト
    future_df = df[df.index > signal_date].copy()

    result = {}
    for idx, n in enumerate(FORWARD_DAYS):
        key_ret = f"ret_{n}d"
        key_hit = f"hit_{hit_threshold:.0f}pct_{n}d"
        if len(future_df) >= n and base_close is not None and base_close > 0:
            fwd_close = float(future_df.iloc[n - 1]["Close"])
            # その日自身の前日比（day-by-day）を計算する
            # n=1（翌日）は signal_date 終値が前日
            # n>=2 は1つ前の future_df の終値が前日
            if n == 1:
                prev_for_ret = base_close
            else:
                prev_for_ret = float(future_df.iloc[n - 2]["Close"])
            if prev_for_ret > 0:
                ret = round((fwd_close - prev_for_ret) / prev_for_ret * 100, 3)
            else:
                ret = None
            result[key_ret] = ret
            # 達成フラグは翌日（T+1）の終値が signal_date 終値から+threshold%以上かで判定
            cum_ret = round((fwd_close - base_close) / base_close * 100, 3)
            result[key_hit] = 1 if cum_ret >= hit_threshold else 0
        else:
            # データなし（月末最終日など）
            result[key_ret] = None
            result[key_hit] = None

    # 翌日の終値も記録
    if len(future_df) >= 1:
        result["next_close"] = round(float(future_df.iloc[0]["Close"]), 1)
    else:
        result["next_close"] = None

    # 3日以内・5日以内プラスの判定（高値 or なければ終値が買値を上回ったか）
    # ※期間内に一度でも買値 (base_close) を上回れば 1
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
        tickers_df: 銘柄リスト（code, name列を含む）
        start_date: 検証開始日（"YYYY-MM-DD"）
        end_date: 検証終了日（"YYYY-MM-DD"）
        min_volume: 最低出来高
        hit_threshold: 達成フラグの閾値（%）

    Returns:
        pd.DataFrame: 全シグナル行を含む結果テーブル
    """
    # ---- 取得期間の調整（SMA60バッファ + リターン計算用の先読み分） ----
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    fetch_start = (start_dt - timedelta(days=HISTORY_BUFFER_DAYS)).strftime("%Y-%m-%d")
    fetch_end = (end_dt + timedelta(days=14)).strftime("%Y-%m-%d")  # 5営業日先まで

    total = len(tickers_df)
    print(f"\n📥 株価データ取得中（{total}銘柄 / {fetch_start} 〜 {fetch_end}）...")
    print("   ※ SMA60バッファ込みで取得するため少し時間がかかります")
    print()

    # ---- 並列でデータ取得 ----
    all_data: Dict[int, Tuple[str, pd.DataFrame]] = {}
    completed = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for _, row in tickers_df.iterrows():
            code = int(row["code"])
            name = str(row.get("name", ""))
            f = executor.submit(fetch_ticker_history, code, name, fetch_start, fetch_end)
            futures[f] = (code, name)

        for f in as_completed(futures):
            completed += 1
            code, name = futures[f]
            try:
                res = f.result()
                if res is not None:
                    _, _, df = res
                    all_data[code] = (name, df)
                if completed % 100 == 0 or completed == total:
                    pct = completed / total * 100
                    print(f"  ⏳ [{completed}/{total}] {pct:.0f}% 完了... "
                          f"（有効データ: {len(all_data)}件）")
            except Exception:
                errors += 1

    print(f"\n  ✅ データ取得完了（成功: {len(all_data)}件 / エラー: {errors}件）")

    # ---- 検証対象の営業日リストを生成 ----
    # yfinance のデータに含まれる日付から、start〜end の範囲の日付を抽出
    biz_days: set = set()
    for _, (_, df) in all_data.items():
        dates_in_range = df.index[
            (df.index >= start_dt) & (df.index <= end_dt)
        ]
        biz_days.update(dates_in_range.tolist())
    biz_days_sorted = sorted(biz_days)

    print(f"\n🔍 バックテスト開始")
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

        for code, (name, df) in all_data.items():
            # スクリーニング判定（as_of 以前のデータのみ使用）
            screen_result = screen_at_date(df, signal_ts, min_volume)
            if screen_result is None:
                continue

            # リターン計算
            fwd_returns = calc_forward_returns(df, signal_ts, hit_threshold)

            row = {
                "date": signal_ts.strftime("%Y-%m-%d"),
                "ticker": code,
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

    print(f"\n✅ バックテスト完了！ 総シグナル数: {len(rows)}件")

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
    翌日〜5日後のリターンも含めて返す。

    Args:
        tickers_df: 銘柄リスト
        as_of_date: スクリーニング基準日（"YYYY-MM-DD"）
        min_volume: 最低出来高

    Returns:
        pd.DataFrame: その日の候補銘柄とリターン
    """
    as_of_dt = datetime.strptime(as_of_date, "%Y-%m-%d")
    fetch_start = (as_of_dt - timedelta(days=HISTORY_BUFFER_DAYS)).strftime("%Y-%m-%d")
    fetch_end = (as_of_dt + timedelta(days=14)).strftime("%Y-%m-%d")

    print(f"\n🔍 過去日付スクリーニング: {as_of_date}")
    print(f"   条件: SMA5>SMA20>SMA60, 出来高≥{min_volume:,}株")

    # start〜endを1日だけにして run_backtest を再利用
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

    Args:
        df: run_backtest() の戻り値
        hit_threshold: 達成フラグの閾値（%）

    Returns:
        統計情報の辞書
    """
    if df.empty:
        return {}

    hit_col = f"hit_{hit_threshold:.0f}pct_1d"
    ret_col = "ret_1d"

    # 有効データ（NaN でない行）
    valid = df[df[ret_col].notna()].copy()

    n_signals = len(df)
    n_valid = len(valid)
    n_dates = df["date"].nunique()
    n_tickers = df["ticker"].nunique()

    # 基本統計
    avg_ret_1d = round(valid[ret_col].mean(), 3) if n_valid > 0 else None
    win_rate = round(
        (valid[ret_col] > 0).sum() / n_valid * 100, 1
    ) if n_valid > 0 else None

    # 達成率
    hit_valid = valid[valid[hit_col].notna()]
    hit_rate_1d = round(
        hit_valid[hit_col].mean() * 100, 1
    ) if len(hit_valid) > 0 else None

    # 日数別リターン統計
    ret_stats = {}
    for n in FORWARD_DAYS:
        col = f"ret_{n}d"
        if col in df.columns:
            v = df[df[col].notna()][col]
            ret_stats[col] = {
                "mean": round(v.mean(), 3) if len(v) > 0 else None,
                "median": round(v.median(), 3) if len(v) > 0 else None,
                "win_rate": round((v > 0).sum() / len(v) * 100, 1) if len(v) > 0 else None,
                "max": round(v.max(), 3) if len(v) > 0 else None,
                "min": round(v.min(), 3) if len(v) > 0 else None,
            }

    # 達成率（各日数）
    hit_rates = {}
    for n in FORWARD_DAYS:
        hcol = f"hit_{hit_threshold:.0f}pct_{n}d"
        if hcol in df.columns:
            hv = df[df[hcol].notna()][hcol]
            hit_rates[f"{n}d"] = round(hv.mean() * 100, 1) if len(hv) > 0 else None

    # 上位銘柄（翌日+X%達成回数）
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
    else:
        top_tickers_list = []

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
    バックテスト結果を CSV と JSON で保存する。

    Args:
        df: バックテスト結果 DataFrame
        summary: summarize_results() の戻り値
        tag: ファイル名に付けるタグ（省略可）

    Returns:
        (csv_path, json_path) のタプル
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    now = datetime.now()
    ts = now.strftime("%Y%m%d_%H%M%S")
    prefix = f"backtest_{tag}_{ts}" if tag else f"backtest_{ts}"

    csv_path = os.path.join(OUTPUT_DIR, f"{prefix}.csv")
    json_path = os.path.join(OUTPUT_DIR, f"{prefix}_summary.json")
    latest_csv = os.path.join(OUTPUT_DIR, "backtest_latest.csv")
    latest_json = os.path.join(OUTPUT_DIR, "backtest_latest_summary.json")

    # CSV 保存
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    # サマリー JSON 保存
    summary_with_meta = {
        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "csv_path": csv_path,
        **summary,
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(summary_with_meta, f, ensure_ascii=False, indent=2)

    # latest ファイルも更新
    df.to_csv(latest_csv, index=False, encoding="utf-8-sig")
    with open(latest_json, "w", encoding="utf-8") as f:
        json.dump(summary_with_meta, f, ensure_ascii=False, indent=2)

    print(f"\n💾 結果を保存しました")
    print(f"   CSV:     {csv_path}")
    print(f"   サマリー: {json_path}")
    print(f"   最新CSV:  {latest_csv}")

    return csv_path, json_path


# =============================================================
# CLI エントリーポイント
# =============================================================
def _parse_args() -> argparse.Namespace:
    """コマンドライン引数を解析する。"""
    parser = argparse.ArgumentParser(
        description="レベル1バックテストエンジン",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  # 2025年全期間をバックテスト
  python3 backtester.py --start 2025-01-01 --end 2025-12-31

  # 特定日1日だけ確認
  python3 backtester.py --date 2025-10-15

  # 少ない銘柄で動作確認（開発用・フォールバック50銘柄）
  python3 backtester.py --start 2025-01-06 --end 2025-01-10 --sample
        """,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--start", help="検証開始日 YYYY-MM-DD（--endと組み合わせて使用）")
    group.add_argument("--date", help="単日スクリーニング YYYY-MM-DD")

    parser.add_argument("--end", help="検証終了日 YYYY-MM-DD（--startと組み合わせて使用）")
    parser.add_argument(
        "--min-volume", type=int, default=DEFAULT_MIN_VOLUME,
        help=f"最低出来高株数（デフォルト: {DEFAULT_MIN_VOLUME:,}）"
    )
    parser.add_argument(
        "--hit-pct", type=float, default=DEFAULT_HIT_THRESHOLD,
        help=f"達成フラグの閾値 %（デフォルト: {DEFAULT_HIT_THRESHOLD}）"
    )
    parser.add_argument(
        "--sample", action="store_true",
        help="フォールバック50銘柄で動作確認（JPX取得をスキップ）"
    )
    parser.add_argument(
        "--no-report", action="store_true",
        help="HTMLレポートを自動生成しない"
    )
    return parser.parse_args()


def main() -> None:
    """バックテストのメイン処理。"""
    args = _parse_args()

    start_time = time.time()

    print("=" * 52)
    print("📊 レベル1 バックテストエンジン")
    print("=" * 52)

    # ---- 銘柄リスト取得 ----
    if args.sample:
        print("⚠️  サンプルモード: フォールバック銘柄リストを使用します\n")
        tickers_df = _fallback_tickers()
    else:
        tickers_df = fetch_jpx_tickers()

    # ---- バックテスト実行 ----
    if args.date:
        # 単日モード
        df = run_backtest(
            tickers_df=tickers_df,
            start_date=args.date,
            end_date=args.date,
            min_volume=args.min_volume,
            hit_threshold=args.hit_pct,
        )
        tag = args.date.replace("-", "")
    else:
        # 期間モード
        if not args.end:
            print("❌ --start を指定した場合、--end も必要です")
            sys.exit(1)
        df = run_backtest(
            tickers_df=tickers_df,
            start_date=args.start,
            end_date=args.end,
            min_volume=args.min_volume,
            hit_threshold=args.hit_pct,
        )
        tag = f"{args.start.replace('-','')}_{args.end.replace('-','')}"

    if df.empty:
        print("\n⚠️  条件に合致する銘柄が見つかりませんでした")
        sys.exit(0)

    # ---- 集計 ----
    summary = summarize_results(df, hit_threshold=args.hit_pct)

    # ---- 保存 ----
    csv_path, json_path = save_backtest_results(df, summary, tag=tag)

    # ---- サマリー表示 ----
    print("\n" + "=" * 52)
    print("📈 バックテスト結果サマリー")
    print("=" * 52)
    print(f"  検証日数            : {summary.get('n_dates', 0)}日")
    print(f"  総シグナル数         : {summary.get('n_signals', 0)}件")
    print(f"  対象銘柄数           : {summary.get('n_tickers', 0)}件")
    print(f"  ─────")
    print(f"  翌日平均騰落率       : {summary.get('avg_ret_1d', 'N/A'):.3f}%")
    print(f"  翌日勝率（プラス）    : {summary.get('win_rate_1d', 'N/A'):.1f}%")
    print(f"  翌日+{args.hit_pct:.0f}%達成率     : {summary.get('hit_rate_1d', 'N/A'):.1f}%")

    hr = summary.get("hit_rates_by_day", {})
    print(f"  ─────")
    print(f"  達成率（2日後）        : {hr.get('2d', 'N/A')}")
    print(f"  達成率（3日後）        : {hr.get('3d', 'N/A')}")
    print(f"  達成率（5日後）        : {hr.get('5d', 'N/A')}")

    rs = summary.get("ret_stats_by_day", {})
    for n in FORWARD_DAYS:
        stat = rs.get(f"ret_{n}d", {})
        if stat:
            print(f"  ─────  {n}日後リターン統計")
            print(f"    平均: {stat.get('mean', 'N/A'):.3f}%  "
                  f"中央値: {stat.get('median', 'N/A'):.3f}%  "
                  f"最大: {stat.get('max', 'N/A'):.3f}%  "
                  f"最小: {stat.get('min', 'N/A'):.3f}%")

    if summary.get("top_tickers"):
        print(f"\n  🏆 上位銘柄（翌日+{args.hit_pct:.0f}%達成回数順）")
        for t in summary["top_tickers"][:5]:
            print(f"    {t['ticker']} {t['name'][:10]:10s}  "
                  f"達成: {t['hit_count']}回 / シグナル{t['signal_count']}回  "
                  f"平均翌日リターン: {t['avg_ret']:.3f}%")

    elapsed = time.time() - start_time
    print(f"\n⏱  総実行時間: {elapsed:.1f}秒")
    print("=" * 52)

    # ---- HTMLレポート自動生成 ----
    if not args.no_report:
        print("\n📄 HTMLレポートを生成中...")
        try:
            import generate_backtest_report
            report_path = generate_backtest_report.generate_report(
                csv_path=latest_csv if os.path.exists(latest_csv := os.path.join(OUTPUT_DIR, "backtest_latest.csv")) else csv_path,
                json_path=latest_json if os.path.exists(latest_json := os.path.join(OUTPUT_DIR, "backtest_latest_summary.json")) else json_path,
            )
            if report_path:
                import webbrowser
                webbrowser.open(f"file://{report_path}")
        except ImportError:
            print("  ⚠️  generate_backtest_report.py が見つかりません。手動で実行してください。")


if __name__ == "__main__":
    main()
