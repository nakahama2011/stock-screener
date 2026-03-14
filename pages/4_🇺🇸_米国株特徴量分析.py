"""
🇺🇸 米国株 特徴量分析レポート表示ページ

Streamlitマルチページアプリの一部として、
米国株の特徴量分析レポートを全画面で表示する。
"""
import os
import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(
    page_title="🇺🇸 米国株 特徴量分析レポート",
    page_icon="🇺🇸",
    layout="wide",
)

# レポートHTMLを読み込む
_report_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "us_results",
    "us_feature_analysis_report.html",
)

if os.path.exists(_report_path):
    with open(_report_path, "r", encoding="utf-8") as f:
        report_html = f.read()

    # 全画面でHTMLレポートを表示する
    components.html(report_html, height=2000, scrolling=True)
else:
    st.warning("⚠️ 米国株レポートが見つかりません。先に `python3 us_analyze_features.py` を実行してください。")
