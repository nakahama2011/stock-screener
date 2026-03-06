"""
レベル2：TradingViewチャート自動スナップショット取得

レベル1の候補銘柄に対して、TradingViewのチャートページを直接Playwrightで開き、
4時間足/1時間足/15分足のスクリーンショットを自動取得する。
"""

import json
import os
import asyncio
import re
import time
from datetime import datetime
from playwright.async_api import async_playwright
from typing import List, Dict, Any


# =========================================================
# 定数
# =========================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(SCRIPT_DIR, "results")
SNAPSHOTS_DIR = os.path.join(SCRIPT_DIR, "snapshots")
LATEST_JSON = os.path.join(RESULTS_DIR, "latest.json")

# 時間足の設定
TIMEFRAMES = [
    {"name": "4h", "interval": "240", "label": "4時間足"},
    {"name": "1h", "interval": "60", "label": "1時間足"},
    {"name": "15m", "interval": "15", "label": "15分足"},
]

# チャートの描画サイズ
CHART_WIDTH = 1400
CHART_HEIGHT = 800

# チャート読み込み待機時間（秒）
CHART_LOAD_WAIT = 8

# 並列で同時に処理する銘柄数
CONCURRENT_LIMIT = 2


# =========================================================
# TradingView チャートURL
# =========================================================
def get_chart_url(code: int, interval: str) -> str:
    """
    TradingViewのチャートURLを生成する。

    Args:
        code: 銘柄コード
        interval: 時間足（"240", "60", "15"）

    Returns:
        str: TradingView チャートURL
    """
    # TradingViewの標準チャートURL
    return f"https://www.tradingview.com/chart/?symbol=TSE%3A{code}&interval={interval}"


# =========================================================
# スナップショット取得
# =========================================================
async def capture_ticker_snapshots(
    context,
    ticker: Dict[str, Any],
    output_dir: str,
) -> Dict[str, str]:
    """
    1つの銘柄に対して3つの時間足でスナップショットを取得する。

    Args:
        context: Playwrightのブラウザコンテキスト
        ticker: 銘柄情報の辞書
        output_dir: 保存先ディレクトリ

    Returns:
        Dict: {timeframe_name: filepath} の辞書
    """
    code = ticker["code"]
    name = ticker.get("name", "")

    # フォルダ名から使えない文字を除去
    safe_name = re.sub(r'[\\/:*?"<>|]', '', name)
    ticker_dir = os.path.join(output_dir, f"{code}_{safe_name}")
    os.makedirs(ticker_dir, exist_ok=True)

    snapshots = {}

    page = await context.new_page()
    try:
        for tf_idx, tf in enumerate(TIMEFRAMES):
            filepath = os.path.join(ticker_dir, f"{tf['name']}.png")

            try:
                url = get_chart_url(code, tf["interval"])
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)

                # チャート描画待ち
                await page.wait_for_timeout(CHART_LOAD_WAIT * 1000)

                # Cookie同意バナーやポップアップを閉じる
                await _dismiss_popups(page)

                # 追加待ち
                await page.wait_for_timeout(2000)

                # スクリーンショット取得
                await page.screenshot(path=filepath)

                snapshots[tf["name"]] = filepath
                print(f"    📸 {tf['label']} → 保存完了")

            except Exception as e:
                print(f"    ⚠️ {tf['label']} → 取得失敗: {e}")

    finally:
        await page.close()

    return snapshots


async def _dismiss_popups(page):
    """
    TradingViewページのポップアップやバナーを閉じる。

    Args:
        page: Playwrightのページオブジェクト
    """
    try:
        # Cookie同意ボタン
        for selector in [
            "button:has-text('Accept')",
            "button:has-text('OK')",
            "button:has-text('Got it')",
            "button:has-text('Close')",
            "[class*='close']",
            "[aria-label='Close']",
            ".tv-dialog__close",
        ]:
            try:
                btn = page.locator(selector).first
                if await btn.is_visible(timeout=500):
                    await btn.click(timeout=1000)
                    await page.wait_for_timeout(500)
            except Exception:
                pass
    except Exception:
        pass


async def run_snapshot_batch(
    candidates: List[Dict[str, Any]],
    output_dir: str,
) -> List[Dict[str, Any]]:
    """
    候補銘柄のバッチスナップショット処理を実行する。

    Args:
        candidates: 候補銘柄のリスト
        output_dir: 保存先ベースディレクトリ

    Returns:
        List[Dict]: スナップショット結果のリスト
    """
    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-web-security",
                "--disable-blink-features=AutomationControlled",
            ],
        )

        context = await browser.new_context(
            viewport={"width": CHART_WIDTH, "height": CHART_HEIGHT},
            device_scale_factor=2,
            locale="ja-JP",
            timezone_id="Asia/Tokyo",
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )

        total = len(candidates)
        semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)

        async def process_one(i: int, ticker: Dict[str, Any]):
            async with semaphore:
                code = ticker["code"]
                name = ticker.get("name", "")
                print(f"\n  [{i+1}/{total}] {code} {name}")

                snapshots = await capture_ticker_snapshots(context, ticker, output_dir)

                result = {
                    "code": code,
                    "name": name,
                    "symbol": f"TSE:{code}",
                    "snapshots": snapshots,
                }
                results.append(result)

        tasks = [process_one(i, t) for i, t in enumerate(candidates)]
        await asyncio.gather(*tasks)

        await browser.close()

    results.sort(key=lambda x: x["code"])
    return results


# =========================================================
# メイン処理
# =========================================================
def run_snapshots(max_tickers: int = None):
    """
    スナップショット取得のメイン処理。

    Args:
        max_tickers: 最大処理銘柄数（デバッグ用、Noneで全件）
    """
    if not os.path.exists(LATEST_JSON):
        print("❌ スクリーニング結果が見つかりません。先に screener.py を実行してください。")
        return

    with open(LATEST_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    candidates = data.get("candidates", [])

    if max_tickers:
        candidates = candidates[:max_tickers]

    if not candidates:
        print("❌ 候補銘柄がありません。")
        return

    today = datetime.now().strftime("%Y%m%d")
    output_dir = os.path.join(SNAPSHOTS_DIR, today)
    os.makedirs(output_dir, exist_ok=True)

    start_time = time.time()

    print("=" * 50)
    print("📸 TradingView チャートスナップショット取得")
    print("=" * 50)
    print(f"   候補銘柄数: {len(candidates)}")
    print(f"   時間足: 4時間足 / 1時間足 / 15分足")
    print(f"   保存先: {output_dir}")

    results = asyncio.run(run_snapshot_batch(candidates, output_dir))

    index_data = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "date": today,
        "output_dir": output_dir,
        "total_tickers": len(results),
        "timeframes": [tf["name"] for tf in TIMEFRAMES],
        "tickers": results,
    }

    index_path = os.path.join(SNAPSHOTS_DIR, "snapshot_index.json")
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(index_data, f, ensure_ascii=False, indent=2)

    elapsed = time.time() - start_time
    print(f"\n✅ スナップショット取得完了")
    print(f"   取得銘柄数: {len(results)}")
    print(f"   インデックス: {index_path}")
    print(f"   実行時間: {elapsed:.1f}秒")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="TradingViewチャートスナップショット取得")
    parser.add_argument(
        "--limit", "-n", type=int, default=None,
        help="処理する銘柄数の上限（デバッグ用）"
    )
    args = parser.parse_args()

    run_snapshots(max_tickers=args.limit)
