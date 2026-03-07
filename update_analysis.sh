#!/bin/bash
# 特徴量分析の再実行とGitHubへのプッシュを行う自動更新スクリプト
#
# 使い方:
#   cd /Users/nakahamahirotaka/Desktop/Antigravity/001
#   bash update_analysis.sh
#
# 週1回（例: 毎週日曜）実行すると、最新データで分析結果が更新されます。
# 自動化したい場合は、以下のコマンドでcronに登録:
#   crontab -e
#   0 9 * * 0 cd /Users/nakahamahirotaka/Desktop/Antigravity/001 && bash update_analysis.sh >> /tmp/stock_analysis.log 2>&1

set -e

echo "=========================================="
echo "📊 特徴量分析 自動更新スクリプト"
echo "   $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="

# 全銘柄で分析を実行する
echo ""
echo "🔍 全銘柄分析を開始..."
python3 analyze_features.py

# 結果をGitHubにプッシュする
echo ""
echo "📤 GitHubにプッシュ中..."
git add results/top_combos.json results/feature_analysis_report.html
git commit -m "📊 分析データ自動更新 $(date '+%Y-%m-%d')" || echo "変更なし"
git push

echo ""
echo "✅ 更新完了！Streamlit Cloudは自動的に最新データを反映します。"
