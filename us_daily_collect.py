"""
米国株 日次データ収集スクリプト

毎日GitHub Actionsで実行し、以下を行う:
1. S&P500のスクリーニング結果を取得・保存
2. 累積CSV更新
3. MLモデルとランキング更新
"""

import json
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
US_RESULTS_DIR = os.path.join(SCRIPT_DIR, "us_results")
US_DAILY_DIR = os.path.join(US_RESULTS_DIR, "daily")
os.makedirs(US_DAILY_DIR, exist_ok=True)


def step1_fetch_us_screening():
    """S&P500から条件合致銘柄をスクリーニングする"""
    print("=" * 60)
    print("📡 米国株: スクリーニング結果取得")
    print("=" * 60)

    try:
        import yfinance as yf

        # S&P500銘柄一覧取得
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        tables = pd.read_html(url)
        sp500 = tables[0]["Symbol"].tolist()
        sp500 = [s.replace(".", "-") for s in sp500]
        print(f"S&P500: {len(sp500)}銘柄")

        # バッチでダウンロード
        data = yf.download(sp500, period="90d", group_by="ticker", threads=True, progress=False)

        candidates = []
        today = datetime.now().strftime("%Y-%m-%d")

        for ticker in sp500:
            try:
                if ticker not in data.columns.get_level_values(0):
                    continue
                df = data[ticker].dropna()
                if len(df) < 60:
                    continue

                close = df["Close"].iloc[-1]
                sma5 = df["Close"].rolling(5).mean().iloc[-1]
                sma20 = df["Close"].rolling(20).mean().iloc[-1]
                sma60 = df["Close"].rolling(60).mean().iloc[-1]
                vol = df["Volume"].iloc[-1]

                # SMA5 > SMA20 > SMA60, 出来高100万以上
                if not (sma5 > sma20 > sma60 and vol >= 1_000_000):
                    continue

                # RSI(14)
                delta = df["Close"].diff()
                gain = delta.where(delta > 0, 0).rolling(14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
                rs = gain.iloc[-1] / loss.iloc[-1] if loss.iloc[-1] > 0 else 100
                rsi = 100 - (100 / (1 + rs))

                # 出来高比
                vol_ma20 = df["Volume"].rolling(20).mean().iloc[-1]
                vol_ratio = vol / vol_ma20 if vol_ma20 > 0 else 0

                # 騰落率
                day_change = (close - df["Close"].iloc[-2]) / df["Close"].iloc[-2] * 100

                candidates.append({
                    "date": today,
                    "ticker": ticker,
                    "close": round(float(close), 2),
                    "sma5": round(float(sma5), 2),
                    "sma20": round(float(sma20), 2),
                    "sma60": round(float(sma60), 2),
                    "rsi": round(float(rsi), 1),
                    "volume": int(vol),
                    "volume_ratio": round(float(vol_ratio), 2),
                    "day_change_pct": round(float(day_change), 2),
                })
            except Exception:
                continue

        if not candidates:
            print("⚠️ スクリーニング結果が0件")
            return None

        df_out = pd.DataFrame(candidates)
        daily_path = os.path.join(US_DAILY_DIR, f"us_screening_{today}.csv")
        df_out.to_csv(daily_path, index=False)
        print(f"✅ {len(df_out)}銘柄を保存: {daily_path}")
        return df_out

    except Exception as e:
        print(f"❌ 米国株スクリーニングエラー: {e}")
        return None


def step2_update_cumulative():
    """日次CSV結合して累積更新する"""
    print("\n" + "=" * 60)
    print("📊 米国株: 累積データ更新")
    print("=" * 60)

    daily_files = sorted([
        f for f in os.listdir(US_DAILY_DIR)
        if f.startswith("us_screening_") and f.endswith(".csv")
    ])

    if not daily_files:
        print("⚠️ 日次データなし")
        return

    dfs = []
    for f in daily_files:
        try:
            dfs.append(pd.read_csv(os.path.join(US_DAILY_DIR, f)))
        except Exception:
            pass

    if not dfs:
        return

    cum = pd.concat(dfs, ignore_index=True)
    cum = cum.drop_duplicates(subset=["date", "ticker"], keep="last")
    cum_path = os.path.join(US_RESULTS_DIR, "us_daily_cumulative.csv")
    cum.to_csv(cum_path, index=False)
    print(f"✅ 累積: {len(cum)}件（{cum['date'].nunique()}営業日分）")


def step3_update_model():
    """米国株MLモデル更新"""
    print("\n" + "=" * 60)
    print("🤖 米国株: MLモデル更新")
    print("=" * 60)

    csv_path = os.path.join(US_RESULTS_DIR, "us_backtest_latest.csv")
    if not os.path.exists(csv_path):
        print("⚠️ バックテストCSVなし（手動でus_backtester.pyを実行してください）")
        return

    df = pd.read_csv(csv_path)
    print(f"バックテストデータ: {len(df)}件")

    # +2%到達ラベル（米国株は+2%が最適）
    hit_cols = [f"hit_2pct_{n}d" for n in [1,2,3,4,5] if f"hit_2pct_{n}d" in df.columns]
    if not hit_cols:
        print("⚠️ hitラベル列なし")
        return

    df["hit_5d"] = df[hit_cols].max(axis=1)
    print(f"+2%到達率: {df['hit_5d'].mean()*100:.1f}%")

    try:
        import joblib
        from sklearn.ensemble import GradientBoostingClassifier
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import roc_auc_score

        # 特徴量
        bool_cols = ["is_pullback","is_breakout","long_upper_wick","is_high_zone",
                     "big_bearish_yesterday","weekly_sma20_ok","first_sma20_touch"]
        for c in bool_cols:
            if c in df.columns: df[c] = df[c].astype(int)
        for sma, feat in [("sma5","sma5_dist_pct"),("sma20","sma20_dist_pct"),("sma60","sma60_dist_pct")]:
            if sma in df.columns: df[feat] = (df["close"] - df[sma]) / df[sma] * 100
        if "high_price" in df.columns and "low_price" in df.columns:
            df["price_range_pct"] = (df["high_price"] - df["low_price"]) / df["close"] * 100

        feature_names = [
            "rsi", "volume_ratio", "day_change_pct", "prev_day_change_pct",
            "prev_prev_day_change_pct", "volume_change_pct", "vol_today_vs_yday_pct",
            "sma5_dist_pct", "sma20_dist_pct", "sma60_dist_pct",
            "price_range_pct", "atr_pct",
            "is_pullback", "is_breakout", "long_upper_wick", "is_high_zone",
            "big_bearish_yesterday", "weekly_sma20_ok", "first_sma20_touch",
            "sma20_touch_count", "trend_start_days_ago", "day_of_week",
        ]
        avail = [c for c in feature_names if c in df.columns]
        for c in avail: df[c] = pd.to_numeric(df[c], errors="coerce")

        valid = df[df["hit_5d"].notna()].copy()
        X = valid[avail].fillna(0)
        y = valid["hit_5d"].astype(int)

        if len(valid) < 50:
            print("⚠️ データ不足")
            return

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        model = GradientBoostingClassifier(n_estimators=200, max_depth=4, learning_rate=0.1, min_samples_leaf=10, random_state=42)
        model.fit(X_train, y_train)
        auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
        print(f"AUC: {auc:.3f}")

        model_data = {"model": model, "feature_names": avail}
        joblib.dump(model_data, os.path.join(US_RESULTS_DIR, "us_ml_model.pkl"))
        print("✅ 米国株MLモデル更新完了")

    except ImportError:
        print("⚠️ scikit-learn/joblib未インストール")
    except Exception as e:
        print(f"⚠️ モデル更新エラー: {e}")


def main():
    """米国株日次収集のメイン処理"""
    print("🚀 米国株 日次データ収集パイプライン開始")
    print(f"   実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    step1_fetch_us_screening()
    step2_update_cumulative()
    step3_update_model()

    print("\n" + "=" * 60)
    print("✅ 米国株パイプライン完了")
    print("=" * 60)


if __name__ == "__main__":
    main()
