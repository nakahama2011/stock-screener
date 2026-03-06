#!/bin/bash
# ==================================================
# 株スクリーニング実行スクリプト
# レベル1: SMA順行配列 + 出来高条件
# レベル2: TradingViewチャート自動スナップショット
#
# ── バックテスト（過去検証）の使い方 ──
#   # 期間指定バックテスト
#   python3 backtester.py --start 2025-01-01 --end 2025-12-31
#
#   # 特定日1日だけ確認
#   python3 backtester.py --date 2025-10-15
#
#   # サンプル50銘柄で短期間テスト（動作確認用）
#   python3 backtester.py --start 2025-01-06 --end 2025-01-10 --sample
#
# ── 日付指定スクリーニングUI（インタラクティブ）の起動 ──
#   /Users/nakahamahirotaka/Library/Python/3.9/bin/streamlit run screener_ui.py
#   → ブラウザで http://localhost:8501 を開く
# ================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "========================================"
echo "📊 株スクリーニングシステム"
echo "========================================"
echo ""

# 1. スクリーニング実行（レベル1）
echo "🔍 Step 1: スクリーニング実行中..."
python3 screener.py

echo ""

# 2. HTMLレポート生成（レベル1）
echo "📄 Step 2: スクリーニングレポート生成中..."
python3 generate_report.py

echo ""

# 3. チャートスナップショット取得（レベル2）
echo "📸 Step 3: チャートスナップショット取得中..."
python3 snapshot.py

echo ""

# 4. ギャラリーHTML生成（レベル2）
echo "🖼️  Step 4: チャートギャラリー生成中..."
python3 generate_gallery.py

echo ""
echo "========================================"
echo "✅ 完了！"
echo "  レポート: results/report.html"
echo "  ギャラリー: snapshots/gallery.html"
echo "========================================"
