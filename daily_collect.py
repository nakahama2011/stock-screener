"""
日次データ収集スクリプト

毎日GitHub Actionsで実行し、以下を行う:
1. TradingView APIでスクリーニング結果を取得
2. 日付付きCSVとして蓄積保存
3. バックテスト用に累積CSVを更新
4. MLモデルとフィルターランキングを再計算
"""

import json
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(SCRIPT_DIR, "results")
DAILY_DIR = os.path.join(RESULTS_DIR, "daily")
os.makedirs(DAILY_DIR, exist_ok=True)


# =========================================================
# ステップ1: TradingViewからスクリーニング結果を取得
# =========================================================
def step1_fetch_screening():
    """TradingView APIでスクリーニング結果を取得してCSVに保存する"""
    print("=" * 60)
    print("📡 ステップ1: TradingViewからスクリーニング結果取得")
    print("=" * 60)

    try:
        from screener import run_screening, save_results
        candidates = run_screening()

        if not candidates:
            print("⚠️ スクリーニング結果が0件")
            return None

        # latest.jsonにも保存
        save_results(candidates)
        today = datetime.now().strftime("%Y-%m-%d")

        # 各銘柄のデータをフラット化
        rows = []
        for c in candidates:
            row = {
                "date": today,
                "code": c.get("code"),
                "name": c.get("name"),
                "close": c.get("close"),
                "sma5": c.get("sma5"),
                "sma20": c.get("sma20"),
                "sma60": c.get("sma60"),
                "rsi": c.get("rsi"),
                "volume": c.get("volume"),
                "volume_ratio": c.get("volume_ratio"),
                "day_change_pct": c.get("day_change_pct"),
                "prev_day_change_pct": c.get("prev_day_change_pct"),
                "high_price": c.get("high_price"),
                "low_price": c.get("low_price"),
                "atr_pct": c.get("atr_pct"),
            }
            rows.append(row)

        df = pd.DataFrame(rows)

        # 日付付きCSVで保存
        daily_path = os.path.join(DAILY_DIR, f"screening_{today}.csv")
        df.to_csv(daily_path, index=False)
        print(f"✅ {len(df)}銘柄を保存: {daily_path}")
        return df

    except Exception as e:
        print(f"❌ スクリーニング取得エラー: {e}")
        return None


# =========================================================
# ステップ2: 過去の日次データを結合して累積CSV更新
# =========================================================
def step2_update_cumulative():
    """日次CSVを全て結合して累積CSVに更新する"""
    print("\n" + "=" * 60)
    print("📊 ステップ2: 累積データ更新")
    print("=" * 60)

    daily_files = sorted([
        f for f in os.listdir(DAILY_DIR)
        if f.startswith("screening_") and f.endswith(".csv")
    ])

    if not daily_files:
        print("⚠️ 日次データなし")
        return None

    dfs = []
    for f in daily_files:
        try:
            d = pd.read_csv(os.path.join(DAILY_DIR, f))
            dfs.append(d)
        except Exception:
            pass

    if not dfs:
        return None

    cumulative = pd.concat(dfs, ignore_index=True)
    cumulative = cumulative.drop_duplicates(subset=["date", "code"], keep="last")

    cum_path = os.path.join(RESULTS_DIR, "daily_cumulative.csv")
    cumulative.to_csv(cum_path, index=False)
    n_dates = cumulative["date"].nunique()
    print(f"✅ 累積データ: {len(cumulative)}件（{n_dates}営業日分）")
    return cumulative


# =========================================================
# ステップ3: MLモデルとランキングを更新
# =========================================================
def step3_update_model():
    """バックテストCSVが十分なデータがあればMLモデルを再訓練する"""
    print("\n" + "=" * 60)
    print("🤖 ステップ3: MLモデル＆ランキング更新")
    print("=" * 60)

    csv_path = os.path.join(RESULTS_DIR, "backtest_latest.csv")
    if not os.path.exists(csv_path):
        print("⚠️ バックテストCSVがありません（手動でbacktester.pyを実行してください）")
        return

    df = pd.read_csv(csv_path)
    print(f"バックテストデータ: {len(df)}件")

    # +3%到達ラベル（フォールバック+2%）
    hit_cols = [f"hit_3pct_{n}d" for n in [1,2,3,4,5] if f"hit_3pct_{n}d" in df.columns]
    if not hit_cols:
        hit_cols = [f"hit_2pct_{n}d" for n in [1,2,3,4,5] if f"hit_2pct_{n}d" in df.columns]
    if not hit_cols:
        print("⚠️ hitラベル列がありません")
        return

    df["hit_5d"] = df[hit_cols].max(axis=1)
    target_pct = "3" if "hit_3pct_1d" in df.columns else "2"
    print(f"ターゲット: +{target_pct}%到達（5日以内）")
    print(f"+{target_pct}%到達率: {df['hit_5d'].mean()*100:.1f}%")

    # 特徴量準備
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

    if len(valid) < 100:
        print("⚠️ データ不足（100件未満）でモデル更新をスキップ")
        return

    # MLモデル訓練
    try:
        import joblib
        from sklearn.ensemble import GradientBoostingClassifier
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import roc_auc_score

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        model = GradientBoostingClassifier(
            n_estimators=200, max_depth=4, learning_rate=0.1,
            min_samples_leaf=20, random_state=42
        )
        model.fit(X_train, y_train)
        auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
        print(f"AUC: {auc:.3f}")

        # 保存
        model_data = {"model": model, "feature_names": avail}
        joblib.dump(model_data, os.path.join(RESULTS_DIR, "jp_ml_model.pkl"))

        fi = sorted(zip(avail, model.feature_importances_), key=lambda x: -x[1])
        report = {
            "feature_importance": [{"name": n, "importance": round(float(i), 4)} for n, i in fi],
            "feature_names": avail,
            "n_samples": len(valid),
            "auc": round(auc, 3),
            "target": f"+{target_pct}%到達(5日以内高値ベース)",
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        with open(os.path.join(RESULTS_DIR, "jp_ml_report.json"), "w") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        print("✅ MLモデル更新完了")

    except ImportError:
        print("⚠️ scikit-learn/joblib未インストール。MLモデル更新をスキップ")
        return

    # フィルターランキング更新
    print("\n🏆 フィルターランキング更新...")
    from itertools import combinations

    conditions = {
        "RSI≤40": ("rsi", "<=", 40), "RSI≤50": ("rsi", "<=", 50),
        "RSI 30-50": ("rsi", "range", 30, 50), "RSI 40-55": ("rsi", "range", 40, 55),
        "出来高比≥1.5": ("volume_ratio", ">=", 1.5), "出来高比≥2.0": ("volume_ratio", ">=", 2.0),
        "出来高比≥1.2": ("volume_ratio", ">=", 1.2),
        "当日↓(マイナス)": ("day_change_pct", "<", 0),
        "当日≤-1%": ("day_change_pct", "<=", -1), "当日≤-2%": ("day_change_pct", "<=", -2),
        "前日↓(マイナス)": ("prev_day_change_pct", "<", 0), "前日≤-1%": ("prev_day_change_pct", "<=", -1),
        "ATR%≥2.5": ("atr_pct", ">=", 2.5), "ATR%≥3.0": ("atr_pct", ">=", 3.0),
        "ATR%≥4.0": ("atr_pct", ">=", 4.0),
        "SMA60乖離≤5%": ("sma60_dist_pct", "<=", 5), "SMA60乖離≤10%": ("sma60_dist_pct", "<=", 10),
        "SMA20乖離≤3%": ("sma20_dist_pct", "<=", 3),
        "レンジ幅≥3%": ("price_range_pct", ">=", 3), "レンジ幅≥4%": ("price_range_pct", ">=", 4),
        "プルバック": ("is_pullback", "==", 1), "前日大陰線": ("big_bearish_yesterday", "==", 1),
    }

    def apply_cond(df, cond_def):
        col_name, op = cond_def[0], cond_def[1]
        if col_name not in df.columns: return pd.Series([False]*len(df))
        s = df[col_name]
        if op == "<=": return s <= cond_def[2]
        elif op == ">=": return s >= cond_def[2]
        elif op == "<": return s < cond_def[2]
        elif op == "==": return s == cond_def[2]
        elif op == "range": return (s >= cond_def[2]) & (s <= cond_def[3])
        return pd.Series([False]*len(df))

    combo_results = []
    for c1, c2 in combinations(conditions.keys(), 2):
        mask = apply_cond(df, conditions[c1]) & apply_cond(df, conditions[c2]) & df["hit_5d"].notna()
        n = mask.sum()
        if n < 20: continue
        rate = df.loc[mask, "hit_5d"].mean() * 100
        avg_d = df.loc[mask & df["days_to_target"].notna(), "days_to_target"].mean() if "days_to_target" in df.columns else None
        combo_results.append({
            "条件1": c1, "条件2": c2, "件数": int(n),
            f"+{target_pct}%到達率": round(rate, 1),
            "平均到達日": round(avg_d, 1) if avg_d else None
        })

    combo_df = pd.DataFrame(combo_results).sort_values(f"+{target_pct}%到達率", ascending=False)

    output = {
        "combo": combo_df.head(30).to_dict("records"),
        "overall_rate": round(float(df["hit_5d"].mean() * 100), 1),
        "total_signals": len(df),
        "target": f"+{target_pct}%",
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    with open(os.path.join(RESULTS_DIR, "filter_ranking.json"), "w") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"✅ ランキング更新完了（TOP: {combo_df.iloc[0]['条件1']} + {combo_df.iloc[0]['条件2']}）")


# =========================================================
# メイン
# =========================================================
def main():
    """日次データ収集のメイン処理"""
    print("🚀 日次データ収集パイプライン開始")
    print(f"   実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # ステップ1: スクリーニング結果取得
    df = step1_fetch_screening()

    # ステップ2: 累積データ更新
    step2_update_cumulative()

    # ステップ3: MLモデル＆ランキング更新
    step3_update_model()

    print("\n" + "=" * 60)
    print("✅ 日次データ収集パイプライン完了")
    print("=" * 60)


if __name__ == "__main__":
    main()
