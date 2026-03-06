"""
レベル1：候補銘柄リスト自動生成スクリーナー

日足条件（SMA5 > SMA20 > SMA60、出来高50万以上）で
日本株の候補銘柄を自動スクリーニングする。
"""

import yfinance as yf
import pandas as pd
import numpy as np
import json
import os
import io
import requests
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List, Optional


# =========================================================
# 定数
# =========================================================
# JPX上場銘柄一覧CSVのURL
JPX_CSV_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"

# スクリーニング条件
MIN_VOLUME = 500_000  # 最低出来高: 50万株

# 並列実行のワーカー数
MAX_WORKERS = 10

# 結果出力先ディレクトリ
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")


# =========================================================
# 銘柄一覧の取得
# =========================================================
def fetch_jpx_tickers() -> pd.DataFrame:
    """
    JPX（日本取引所グループ）の上場銘柄一覧を取得する。

    Returns:
        pd.DataFrame: 銘柄コードと銘柄名等を含むデータフレーム
    """
    print("📥 JPX上場銘柄一覧を取得中...")

    try:
        # JPXのExcelファイルをダウンロード
        response = requests.get(JPX_CSV_URL, timeout=30)
        response.raise_for_status()

        # Excelファイルとして読み込み
        df = pd.read_excel(io.BytesIO(response.content))

        # 必要なカラムを抽出・リネーム
        # JPXの一覧は「コード」「銘柄名」「市場・商品区分」「33業種区分」等のカラムを持つ
        df = df.rename(columns={
            "コード": "code",
            "銘柄名": "name",
            "市場・商品区分": "market",
            "33業種区分": "sector",
        })

        # コードが数値のもののみ（ETF等を含む場合もある）
        df = df[pd.to_numeric(df["code"], errors="coerce").notna()]
        df["code"] = df["code"].astype(int)

        # 普通株のみに絞る（市場区分がプライム、スタンダード、グロースのもの）
        stock_markets = ["プライム（内国株式）", "スタンダード（内国株式）", "グロース（内国株式）"]
        df = df[df["market"].isin(stock_markets)]

        print(f"  ✅ {len(df)}銘柄を取得しました")
        return df[["code", "name", "market", "sector"]].reset_index(drop=True)

    except Exception as e:
        print(f"  ❌ JPX銘柄一覧の取得に失敗: {e}")
        print("  ⚠️ フォールバック: 代表的な銘柄リストを使用します")
        return _fallback_tickers()


def _fallback_tickers() -> pd.DataFrame:
    """
    JPXからの取得に失敗した場合のフォールバック銘柄リスト。
    主要100銘柄程度を手動で定義。

    Returns:
        pd.DataFrame: フォールバック銘柄データフレーム
    """
    tickers = [
        (7203, "トヨタ自動車"), (6758, "ソニーグループ"), (9984, "ソフトバンクグループ"),
        (6861, "キーエンス"), (8035, "東京エレクトロン"), (9983, "ファーストリテイリング"),
        (6501, "日立製作所"), (6902, "デンソー"), (7741, "HOYA"),
        (4063, "信越化学工業"), (6098, "リクルートHD"), (8306, "三菱UFJ"),
        (4519, "中外製薬"), (6594, "日本電産"), (7974, "任天堂"),
        (4568, "第一三共"), (9432, "NTT"), (6367, "ダイキン工業"),
        (3382, "セブン&アイHD"), (2802, "味の素"),
        (6981, "村田製作所"), (4661, "OLC"), (6273, "SMC"),
        (7267, "ホンダ"), (8001, "伊藤忠商事"),
        (4502, "武田薬品"), (8058, "三菱商事"), (6857, "アドバンテスト"),
        (7751, "キヤノン"), (2914, "JT"),
        (9433, "KDDI"), (6723, "ルネサス"), (4543, "テルモ"),
        (3407, "旭化成"), (7832, "バンダイナムコHD"),
        (6326, "クボタ"), (7011, "三菱重工業"), (8031, "三井物産"),
        (4901, "富士フイルムHD"), (6702, "富士通"),
        (6752, "パナソニックHD"), (8316, "三井住友FG"), (9434, "ソフトバンク"),
        (6503, "三菱電機"), (8766, "東京海上HD"),
        (4507, "塩野義製薬"), (6971, "京セラ"), (5108, "ブリヂストン"),
        (2801, "キッコーマン"), (7269, "スズキ"),
    ]
    return pd.DataFrame(tickers, columns=["code", "name"])


# =========================================================
# 単一銘柄の分析
# =========================================================
def analyze_single(ticker_code: int, ticker_name: str = "") -> Optional[Dict[str, Any]]:
    """
    単一銘柄の日足データを取得し、スクリーニング条件を判定する。

    条件:
      1. SMA5 > SMA20 > SMA60（順行配列）
      2. 当日出来高 >= 50万株

    Args:
        ticker_code (int): 銘柄コード（例: 7203）
        ticker_name (str): 銘柄名（表示用）

    Returns:
        Optional[Dict]: 条件合致した場合は銘柄情報の辞書、不合致はNone
    """
    symbol = f"{ticker_code}.T"

    try:
        ticker_obj = yf.Ticker(symbol)
        df = ticker_obj.history(period="6mo")

        if df.empty or len(df) < 60:
            return None

        df = df.copy()

        # 移動平均線の計算
        df["SMA5"] = df["Close"].rolling(window=5).mean()
        df["SMA20"] = df["Close"].rolling(window=20).mean()
        df["SMA60"] = df["Close"].rolling(window=60).mean()

        # 出来高20日平均
        df["Volume_MA20"] = df["Volume"].rolling(window=20).mean()

        # 最新日のデータ
        latest = df.iloc[-1]

        # NaN チェック
        if any(pd.isna(latest[col]) for col in ["SMA5", "SMA20", "SMA60", "Volume"]):
            return None

        sma5 = float(latest["SMA5"])
        sma20 = float(latest["SMA20"])
        sma60 = float(latest["SMA60"])
        close = float(latest["Close"])
        volume = int(latest["Volume"])
        volume_ma20 = float(latest["Volume_MA20"]) if not pd.isna(latest["Volume_MA20"]) else 0

        # ---- スクリーニング条件 ----
        # 条件1: 順行配列 SMA5 > SMA20 > SMA60
        is_trend_aligned = sma5 > sma20 > sma60

        # 条件2: 出来高 >= 50万株
        is_volume_ok = volume >= MIN_VOLUME

        if not (is_trend_aligned and is_volume_ok):
            return None

        # 条件すべて合致 → 結果を返す
        latest_date = df.index[-1].strftime("%Y-%m-%d")
        volume_ratio = round(volume / volume_ma20, 2) if volume_ma20 > 0 else 0

        return {
            "code": ticker_code,
            "symbol": symbol,
            "name": ticker_name,
            "date": latest_date,
            "close": round(close, 1),
            "sma5": round(sma5, 1),
            "sma20": round(sma20, 1),
            "sma60": round(sma60, 1),
            "volume": volume,
            "volume_ma20": round(volume_ma20, 0),
            "volume_ratio": volume_ratio,
        }

    except Exception:
        return None


# =========================================================
# メインスクリーニング処理
# =========================================================
def run_screening() -> List[Dict[str, Any]]:
    """
    全銘柄のスクリーニングを実行する。

    1. JPXから銘柄一覧を取得
    2. 各銘柄に対して並列で分析を実行
    3. 条件合致銘柄のリストを返す

    Returns:
        List[Dict]: 条件合致した銘柄情報のリスト
    """
    # 銘柄一覧取得
    tickers_df = fetch_jpx_tickers()
    total = len(tickers_df)

    print(f"\n🔍 スクリーニング開始（{total}銘柄を分析中...）")
    print(f"   条件: SMA5 > SMA20 > SMA60 かつ 出来高 ≥ {MIN_VOLUME:,}株")
    print()

    candidates = []
    completed = 0
    errors = 0

    # 並列処理で銘柄データを取得・分析
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for _, row in tickers_df.iterrows():
            code = int(row["code"])
            name = str(row.get("name", ""))
            future = executor.submit(analyze_single, code, name)
            futures[future] = (code, name)

        for future in as_completed(futures):
            completed += 1
            code, name = futures[future]

            try:
                result = future.result()
                if result is not None:
                    candidates.append(result)
                    print(f"  ✅ [{completed}/{total}] {code} {name} → 候補に追加")
                else:
                    if completed % 100 == 0:
                        print(f"  ⏳ [{completed}/{total}] 処理中...")
            except Exception:
                errors += 1

    # 終値の降順でソート
    candidates.sort(key=lambda x: x["volume"], reverse=True)

    print(f"\n📊 スクリーニング完了")
    print(f"   分析銘柄数: {total}")
    print(f"   候補銘柄数: {len(candidates)}")
    if errors > 0:
        print(f"   エラー数: {errors}")

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
    filename = f"screening_{now.strftime('%Y%m%d_%H%M%S')}.json"
    filepath = os.path.join(OUTPUT_DIR, filename)

    output = {
        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "conditions": {
            "sma_alignment": "SMA5 > SMA20 > SMA60",
            "min_volume": MIN_VOLUME,
        },
        "total_candidates": len(candidates),
        "candidates": candidates,
    }

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    # 最新結果へのシンボリックリンクも作成
    latest_path = os.path.join(OUTPUT_DIR, "latest.json")
    if os.path.exists(latest_path):
        os.remove(latest_path)
    # シンボリックリンクではなくコピーで対応（互換性のため）
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
