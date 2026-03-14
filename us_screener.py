"""
米国株スクリーナー

TradingView Screener APIを使用して、日足条件
（SMA5 > SMA20 > SMA60、出来高100万以上、RSI 30-65、出来高比≥1.2）で
米国株の候補銘柄を自動スクリーニングする。
"""

import json
import os
import time
from datetime import datetime
from typing import Dict, Any, List

import pandas as pd
from tradingview_screener import Query, col


# =========================================================
# 定数
# =========================================================
# スクリーニング条件
MIN_VOLUME = 1_000_000  # 最低出来高: 100万株（米国市場スケール）

# TradingView APIの最大取得件数
TV_LIMIT = 500

# 結果出力先ディレクトリ
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "us_results")


# =========================================================
# TradingView Screener APIでスクリーニング
# =========================================================
def run_screening() -> List[Dict[str, Any]]:
    """
    TradingView Screener APIを使って米国株のスクリーニングを実行する。

    条件:
      1. SMA5 > SMA20 > SMA60（順行配列）
      2. 当日出来高 >= 100万株
      3. RSI(14) 30〜65
      4. 出来高比(10日平均) >= 1.2

    Returns:
        List[Dict]: 条件合致した銘柄情報のリスト
    """
    print("🔍 TradingView Screener API で米国株スクリーニング開始...")
    print(f"   条件: SMA5 > SMA20 > SMA60 かつ 出来高 ≥ {MIN_VOLUME:,}株")
    print(f"         RSI(14) 30〜65 かつ 出来高比(10日平均) ≥ 1.2")
    print()

    try:
        # TradingView APIクエリを構築・実行
        (count, df) = (Query()
            .set_markets('america')
            .select(
                'name', 'description', 'close', 'volume',
                'SMA5', 'SMA20', 'SMA60',
                'relative_volume_10d_calc',
                'RSI',
                'change',
                'exchange',
            )
            .where(
                col('SMA5') > col('SMA20'),
                col('SMA20') > col('SMA60'),
                col('volume') > MIN_VOLUME,
                col('RSI') >= 30,
                col('RSI') <= 65,
                col('relative_volume_10d_calc') >= 1.2,
            )
            .order_by('volume', ascending=False)
            .limit(TV_LIMIT)
            .get_scanner_data())

        print(f"  ✅ TradingView APIから {count} 件の候補を検出（最大{TV_LIMIT}件取得）")

    except Exception as e:
        print(f"  ❌ TradingView APIの呼び出しに失敗しました: {e}")
        return []

    if df.empty:
        print("  ⚠️ 条件に合致する銘柄がありませんでした")
        return []

    # データフレームを辞書のリストに変換
    today_str = datetime.now().strftime("%Y-%m-%d")
    candidates = []

    for _, row in df.iterrows():
        # tickerは "NASDAQ:AAPL" のような形式
        ticker_str = str(row.get("ticker", ""))
        # ティッカーを抽出（"NASDAQ:AAPL" → "AAPL"）
        if ":" in ticker_str:
            exchange_part, symbol = ticker_str.split(":", 1)
        else:
            symbol = ticker_str
            exchange_part = str(row.get("exchange", ""))

        close_val = row.get("close", 0)
        sma5_val = row.get("SMA5", 0)
        sma20_val = row.get("SMA20", 0)
        sma60_val = row.get("SMA60", 0)
        volume_val = int(row.get("volume", 0))
        rel_vol = row.get("relative_volume_10d_calc", 0)

        # volume_ratioとしてrelative_volume_10d_calcを使用
        volume_ratio = round(float(rel_vol), 2) if rel_vol and rel_vol == rel_vol else 0.0

        candidate = {
            "symbol": symbol,
            "exchange": exchange_part,
            "name": str(row.get("description", row.get("name", ""))),
            "date": today_str,
            "close": round(float(close_val), 2) if close_val == close_val else 0,
            "sma5": round(float(sma5_val), 2) if sma5_val == sma5_val else 0,
            "sma20": round(float(sma20_val), 2) if sma20_val == sma20_val else 0,
            "sma60": round(float(sma60_val), 2) if sma60_val == sma60_val else 0,
            "volume": volume_val,
            "volume_ma20": 0,  # TradingView APIではvolume_ma20の直接取得は困難なため0
            "volume_ratio": volume_ratio,
        }
        candidates.append(candidate)

    # 出来高の降順でソート（APIで既にソート済みだが念のため）
    candidates.sort(key=lambda x: x["volume"], reverse=True)

    print(f"\n📊 スクリーニング完了")
    print(f"   候補銘柄数: {len(candidates)}")

    return candidates


def save_results(candidates: List[Dict[str, Any]]) -> str:
    """
    スクリーニング結果をJSONファイルとして保存する。

    Args:
        candidates: 候補銘柄のリスト

    Returns:
        str: 保存先ファイルパス
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    now = datetime.now()
    filename = f"us_screening_{now.strftime('%Y%m%d_%H%M%S')}.json"
    filepath = os.path.join(OUTPUT_DIR, filename)

    output = {
        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "conditions": {
            "sma_alignment": "SMA5 > SMA20 > SMA60",
            "min_volume": MIN_VOLUME,
            "rsi_range": "30-65",
            "min_volume_ratio": 1.2,
        },
        "total_candidates": len(candidates),
        "candidates": candidates,
    }

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    # 最新結果をlatest.jsonにもコピー
    latest_path = os.path.join(OUTPUT_DIR, "latest.json")
    if os.path.exists(latest_path):
        os.remove(latest_path)
    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n💾 結果を保存しました: {filepath}")
    print(f"   最新結果: {latest_path}")

    return filepath


# =========================================================
# エントリーポイント
# =========================================================
if __name__ == "__main__":
    start_time = time.time()

    candidates = run_screening()
    save_results(candidates)

    elapsed = time.time() - start_time
    print(f"\n⏱  実行時間: {elapsed:.1f}秒")
