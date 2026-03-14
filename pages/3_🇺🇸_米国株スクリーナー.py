"""
🇺🇸 米国株スクリーナーページ

Streamlitマルチページアプリの一部として、
米国株のスクリーニングUIを提供する。
"""
import os
import sys
import importlib

# us_screener_ui.pyのメインロジックを直接実行する
# 注意: このファイルはStreamlitのマルチページ機能で使われるため、
# us_screener_ui.pyを直接importする代わりに、exec()で実行する
_ui_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "us_screener_ui.py",
)

if os.path.exists(_ui_path):
    with open(_ui_path, "r", encoding="utf-8") as f:
        exec(f.read())
else:
    import streamlit as st
    st.error("❌ us_screener_ui.py が見つかりません。")
