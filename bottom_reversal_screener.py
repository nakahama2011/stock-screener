"""
底打ち反転スクリーナー

TradingView Screener APIを使用して、日足で長期下落→底打ち→
上昇転換しそうな日本株をスクリーニングする。

ロジック:
  1. 長期下落の証拠: Perf.3M < 0 かつ 終値 < SMA200
  2. 底打ちシグナル: RSI 30〜50 かつ MACD > Signal（GC）
  3. 短期上昇転換: SMA5 > SMA20 かつ 終値 > SMA5
  4. 出来高: ≥ 50万株
"""

import io
import json
import os
import time
from datetime import datetime
from typing import Dict, Any, List, Optional

import pandas as pd
import requests
from tradingview_screener import Query, col


# =========================================================
# 定数
# =========================================================
# スクリーニング条件
MIN_VOLUME = 500_000  # 最低出来高: 50万株

# TradingView APIの最大取得件数
TV_LIMIT = 500

# 結果出力先ディレクトリ
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")

# JPX上場銘柄一覧CSVのURL
JPX_CSV_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"


# =========================================================
# JPX銘柄一覧から日本語名を取得
# =========================================================
def _fetch_jpx_name_map() -> Dict[int, str]:
    """
    JPXの上場銘柄一覧から {銘柄コード: 日本語銘柄名} のマッピングを取得する。
    取得失敗時は空の辞書を返す。
    """
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


# =========================================================
# TradingView Screener APIで底打ち反転スクリーニング
# =========================================================
def run_bottom_reversal_screening() -> List[Dict[str, Any]]:
    """
    TradingView Screener APIを使って底打ち反転銘柄のスクリーニングを実行する。

    条件:
      1. Perf.3M < 0（3ヶ月パフォーマンスがマイナス = 長期下落中）
      2. 終値 < SMA200（200日移動平均線の下 = 長期的に弱い）
      3. RSI > 30 かつ RSI < 50（売られすぎ脱出中〜初動段階）
      4. MACD.macd > MACD.signal（MACDゴールデンクロス）
      5. SMA5 > SMA20（短期上昇転換）
      6. 終値 > SMA5（直近価格が5日移動平均の上）
      7. 出来高 ≥ 50万株

    Returns:
        List[Dict]: 条件に合致した銘柄情報のリスト
    """
    print("🔍 底打ち反転スクリーニング開始...")
    print("   ---- 条件 ----")
    print("   📉 長期下落: Perf.3M < 0 かつ Close < SMA200")
    print("   📊 底打ちシグナル: RSI 30〜50 かつ MACD > Signal")
    print("   📈 短期上昇転換: SMA5 > SMA20 かつ Close > SMA5")
    print(f"   📦 出来高: ≥ {MIN_VOLUME:,}株")
    print()

    try:
        # TradingView APIクエリを構築・実行
        (count, df) = (Query()
            .set_markets('japan')
            .select(
                'name', 'description', 'close', 'volume',
                # 移動平均線
                'SMA5', 'SMA20', 'SMA60', 'SMA200',
                # テクニカル指標
                'RSI', 'MACD.macd', 'MACD.signal',
                # パフォーマンス
                'Perf.1M', 'Perf.3M', 'Perf.6M',
                # 3ヶ月・6ヶ月の高値・安値
                'High.3M', 'Low.3M', 'High.6M', 'Low.6M',
                # 出来高指標
                'relative_volume_10d_calc',
                # 日次変動
                'change',
            )
            .where(
                # 1. 長期下落の証拠
                col('Perf.3M') < 0,            # 3ヶ月パフォーマンスがマイナス
                col('close') < col('SMA200'),   # 終値 < 200日移動平均

                # 2. 底打ちシグナル
                col('RSI') > 30,                # 売られすぎ脱出
                col('RSI') < 50,                # まだ上昇相場入り前
                col('MACD.macd') > col('MACD.signal'),  # MACDゴールデンクロス

                # 3. 短期上昇転換
                col('SMA5') > col('SMA20'),     # 短期MA > 中期MA
                col('close') > col('SMA5'),     # 終値が5日線の上

                # 4. 出来高
                col('volume') > MIN_VOLUME,
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

    # JPX銘柄一覧から日本語名を取得
    print("📥 JPX銘柄一覧から日本語名を取得中...")
    jpx_names = _fetch_jpx_name_map()
    if jpx_names:
        print(f"  ✅ {len(jpx_names)}銘柄の日本語名を取得")
    else:
        print("  ⚠️ JPX名取得失敗、英語名を使用します")

    # データフレームを辞書のリストに変換
    today_str = datetime.now().strftime("%Y-%m-%d")
    candidates = []

    for _, row in df.iterrows():
        # tickerは "TSE:1234" のような形式
        ticker_str = str(row.get("ticker", ""))
        code_str = ticker_str.split(":")[-1] if ":" in ticker_str else ticker_str
        try:
            code = int(code_str)
        except ValueError:
            # 数値に変換できない銘柄コードはスキップ（ETF等）
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
        high_6m = _safe_float(row.get("High.6M"))
        low_6m = _safe_float(row.get("Low.6M"))
        rel_vol = _safe_float(row.get("relative_volume_10d_calc"))
        change_pct = _safe_float(row.get("change"))

        # 底からの反発度（3ヶ月レンジ内の位置 0〜100%）
        # = (現在値 - 3ヶ月安値) / (3ヶ月高値 - 3ヶ月安値) × 100
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
            "symbol": f"{code}.T",
            "name": jpx_names.get(code, str(row.get("description", row.get("name", "")))),
            "date": today_str,
            "close": round(close_val, 1),
            # 移動平均線
            "sma5": round(sma5_val, 1),
            "sma20": round(sma20_val, 1),
            "sma60": round(sma60_val, 1),
            "sma200": round(sma200_val, 1),
            "sma200_deviation_pct": sma200_deviation,
            # テクニカル指標
            "rsi": round(rsi_val, 1),
            "macd": round(macd_val, 4),
            "macd_signal": round(macd_signal_val, 4),
            # パフォーマンス
            "perf_1m_pct": round(perf_1m, 2) if perf_1m is not None else None,
            "perf_3m_pct": round(perf_3m, 2) if perf_3m is not None else None,
            "perf_6m_pct": round(perf_6m, 2) if perf_6m is not None else None,
            # 3ヶ月レンジ
            "high_3m": high_3m,
            "low_3m": low_3m,
            "reversal_position_pct": reversal_position,
            # 出来高
            "volume": volume_val,
            "relative_volume_10d": round(rel_vol, 2) if rel_vol is not None else None,
            # 日次変動
            "change_pct": round(change_pct, 2) if change_pct is not None else None,
        }
        candidates.append(candidate)

    # 出来高の降順でソート
    candidates.sort(key=lambda x: x["volume"], reverse=True)

    print(f"\n📊 スクリーニング完了")
    print(f"   候補銘柄数: {len(candidates)}")

    return candidates


def _safe_float(val) -> Optional[float]:
    """NaN安全な浮動小数点変換"""
    if val is None:
        return None
    try:
        f = float(val)
        if f != f:  # NaN チェック
            return None
        return f
    except (ValueError, TypeError):
        return None


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
    filename = f"bottom_reversal_{now.strftime('%Y%m%d_%H%M%S')}.json"
    filepath = os.path.join(OUTPUT_DIR, filename)

    output = {
        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "screening_type": "bottom_reversal",
        "conditions": {
            "long_term_decline": "Perf.3M < 0 かつ Close < SMA200",
            "bottom_signal": "RSI 30〜50 かつ MACD > Signal",
            "short_term_reversal": "SMA5 > SMA20 かつ Close > SMA5",
            "min_volume": MIN_VOLUME,
        },
        "total_candidates": len(candidates),
        "candidates": candidates,
    }

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    # 最新結果を bottom_reversal_latest.json にもコピー
    latest_path = os.path.join(OUTPUT_DIR, "bottom_reversal_latest.json")
    if os.path.exists(latest_path):
        os.remove(latest_path)
    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n💾 結果を保存しました: {filepath}")
    print(f"   最新結果: {latest_path}")

    return filepath


def print_summary(candidates: List[Dict[str, Any]]) -> None:
    """
    スクリーニング結果のサマリーをコンソールに表示する。

    Args:
        candidates: 候補銘柄のリスト
    """
    if not candidates:
        print("\n⚠️ 条件に合致する銘柄はありませんでした")
        return

    print(f"\n{'='*80}")
    print(f"  🔄 底打ち反転スクリーニング結果: {len(candidates)} 銘柄")
    print(f"{'='*80}")
    print()
    print(f"{'コード':>6}  {'銘柄名':<16}  {'終値':>8}  {'RSI':>5}  {'3M騰落率':>8}  {'SMA200乖離':>9}  {'反発度':>6}  {'出来高':>12}")
    print(f"{'-'*6}  {'-'*16}  {'-'*8}  {'-'*5}  {'-'*8}  {'-'*9}  {'-'*6}  {'-'*12}")

    for c in candidates:
        name = c["name"][:8]  # コンソール表示用に切り詰め
        perf_3m = f"{c['perf_3m_pct']:>7.1f}%" if c.get("perf_3m_pct") is not None else "     N/A"
        sma200_dev = f"{c['sma200_deviation_pct']:>8.1f}%" if c.get("sma200_deviation_pct") is not None else "      N/A"
        rev_pos = f"{c['reversal_position_pct']:>5.1f}%" if c.get("reversal_position_pct") is not None else "   N/A"
        print(
            f"{c['code']:>6}  {name:<16}  "
            f"{c['close']:>8.1f}  {c['rsi']:>5.1f}  "
            f"{perf_3m}  {sma200_dev}  {rev_pos}  "
            f"{c['volume']:>12,}"
        )

    print()
    print("  📝 反発度 = (現在値 - 3ヶ月安値) / (3ヶ月高値 - 3ヶ月安値) × 100")
    print("     低い値 → 底に近い位置で反転開始（より早期段階）")
    print("     高い値 → 底から離れて上昇中（反転がより確認済み）")
    print()


# =========================================================
# エントリーポイント
# =========================================================
if __name__ == "__main__":
    start_time = time.time()

    candidates = run_bottom_reversal_screening()
    print_summary(candidates)
    save_results(candidates)

    elapsed = time.time() - start_time
    print(f"\n⏱  実行時間: {elapsed:.1f}秒")
