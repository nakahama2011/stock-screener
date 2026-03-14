"""
米国株 +2%到達確率予測モデル

バックテストCSVデータを使用してGradientBoosting/RandomForestモデルを訓練し、
5日以内に高値ベースで+2%に到達する確率を予測する。

使い方:
    python3 us_ml_model.py   # モデルを訓練・保存
"""

import json
import os
import warnings
from typing import Dict, List, Optional, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import accuracy_score, roc_auc_score
from sklearn.model_selection import TimeSeriesSplit

warnings.filterwarnings("ignore")

# =========================================================
# 定数
# =========================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(SCRIPT_DIR, "us_results", "us_backtest_latest.csv")
MODEL_DIR = os.path.join(SCRIPT_DIR, "us_results")
MODEL_PATH = os.path.join(MODEL_DIR, "us_ml_model.pkl")
REPORT_PATH = os.path.join(MODEL_DIR, "us_ml_report.json")

# 特徴量として使用する列
FEATURE_COLS = [
    "rsi",
    "volume_ratio",
    "day_change_pct",
    "prev_day_change_pct",
    "prev_prev_day_change_pct",
    "volume_change_pct",
    "is_pullback",
    "is_breakout",
    "long_upper_wick",
    "is_high_zone",
    "big_bearish_yesterday",
    "weekly_sma20_ok",
    "first_sma20_touch",
    "sma20_touch_count",
    "trend_start_days_ago",
    "day_of_week",
    "vol_today_vs_yday_pct",
]

# 追加で計算する特徴量
DERIVED_FEATURES = [
    "atr_pct",           # 当日の高値-安値 / 終値
    "sma5_dist_pct",     # 終値とSMA5の乖離率
    "sma20_dist_pct",    # 終値とSMA20の乖離率
    "sma60_dist_pct",    # 終値とSMA60の乖離率
]


def load_and_prepare_data(csv_path: str = CSV_PATH) -> Tuple[pd.DataFrame, pd.Series, List[str]]:
    """バックテストCSVを読み込み、特徴量とラベルを生成する。"""
    df = pd.read_csv(csv_path)
    print(f"📥 CSVデータ読み込み: {len(df)}行")

    # ラベル: 5日以内に+2%到達
    hit_cols = [f"hit_2pct_{n}d" for n in [1, 2, 3, 4, 5] if f"hit_2pct_{n}d" in df.columns]
    df["target"] = df[hit_cols].max(axis=1)

    # 派生特徴量
    if "high_price" in df.columns and "low_price" in df.columns:
        df["atr_pct"] = (df["high_price"] - df["low_price"]) / df["close"] * 100
    else:
        df["atr_pct"] = 0.0

    for sma_col, feat in [("sma5", "sma5_dist_pct"), ("sma20", "sma20_dist_pct"), ("sma60", "sma60_dist_pct")]:
        if sma_col in df.columns:
            df[feat] = (df["close"] - df[sma_col]) / df[sma_col] * 100
        else:
            df[feat] = 0.0

    # ブール列を数値化
    for col in ["is_pullback", "is_breakout", "long_upper_wick",
                 "is_high_zone", "big_bearish_yesterday", "weekly_sma20_ok",
                 "first_sma20_touch"]:
        if col in df.columns:
            df[col] = df[col].astype(int)

    all_features = FEATURE_COLS + DERIVED_FEATURES
    available = [c for c in all_features if c in df.columns]

    valid = df[df["target"].notna()].copy()
    for c in available:
        valid[c] = pd.to_numeric(valid[c], errors="coerce")

    X = valid[available].fillna(0)
    y = valid["target"].astype(int)

    print(f"  有効データ: {len(valid)}行, +2%到達率: {y.mean()*100:.1f}%, 特徴量: {len(available)}")
    return X, y, available


def train_model(X: pd.DataFrame, y: pd.Series, feature_names: List[str]):
    """GradientBoostingで訓練し、モデルと評価結果を返す。"""
    print("\n🤖 GradientBoosting モデル訓練開始...")

    # 時系列クロスバリデーション
    tscv = TimeSeriesSplit(n_splits=3)
    cv_scores = []

    for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
        X_tr, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_tr, y_val = y.iloc[train_idx], y.iloc[val_idx]

        m = GradientBoostingClassifier(
            n_estimators=200, max_depth=5, learning_rate=0.05,
            subsample=0.8, min_samples_leaf=20, random_state=42,
        )
        m.fit(X_tr, y_tr)

        y_pred = m.predict(X_val)
        y_proba = m.predict_proba(X_val)[:, 1]
        acc = accuracy_score(y_val, y_pred)
        auc = roc_auc_score(y_val, y_proba)
        cv_scores.append({"fold": fold+1, "acc": round(acc, 4), "auc": round(auc, 4)})
        print(f"  Fold {fold+1}: Accuracy={acc:.3f}, AUC={auc:.3f}")

    # 全データで最終モデル
    print("\n📊 最終モデル訓練（全データ）...")
    final = GradientBoostingClassifier(
        n_estimators=200, max_depth=5, learning_rate=0.05,
        subsample=0.8, min_samples_leaf=20, random_state=42,
    )
    final.fit(X, y)

    # 特徴量重要度
    imps = final.feature_importances_
    feat_imp = sorted(zip(feature_names, imps.tolist()), key=lambda x: -x[1])
    print("\n📋 特徴量重要度 TOP10:")
    for i, (name, imp) in enumerate(feat_imp[:10], 1):
        bar = "█" * int(imp / max(imps) * 25)
        print(f"  {i:2d}. {name:<30s} {imp:.4f} {bar}")

    # 確率帯別の精度
    y_proba_all = final.predict_proba(X)[:, 1]
    prob_bins = [(0, 0.3), (0.3, 0.5), (0.5, 0.6), (0.6, 0.7), (0.7, 0.8), (0.8, 1.01)]
    print("\n📊 予測確率帯別の実際の+2%到達率:")
    prob_analysis = []
    for lo, hi in prob_bins:
        mask = (y_proba_all >= lo) & (y_proba_all < hi)
        if mask.sum() > 0:
            actual = y.values[mask].mean() * 100
            cnt = int(mask.sum())
            label = f"{lo*100:.0f}-{hi*100:.0f}%"
            print(f"  予測 {label:>10s}: {cnt:>5d}件 → 実際の到達率 {actual:.1f}%")
            prob_analysis.append({"range": label, "count": cnt, "actual_rate": round(actual, 1)})

    report = {
        "cv_scores": cv_scores,
        "avg_auc": round(np.mean([s["auc"] for s in cv_scores]), 4),
        "avg_acc": round(np.mean([s["acc"] for s in cv_scores]), 4),
        "feature_importance": [{"name": n, "importance": round(i, 4)} for n, i in feat_imp],
        "probability_analysis": prob_analysis,
        "n_samples": len(X),
        "feature_names": feature_names,
    }

    return final, report


def save_model(model, report, feature_names):
    """モデルと評価レポートを保存する。"""
    os.makedirs(MODEL_DIR, exist_ok=True)
    joblib.dump({"model": model, "feature_names": feature_names}, MODEL_PATH)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"\n💾 モデル保存: {MODEL_PATH}")
    print(f"   レポート保存: {REPORT_PATH}")


def load_model():
    """保存済みモデルを読み込む。"""
    if not os.path.exists(MODEL_PATH):
        return None, None
    data = joblib.load(MODEL_PATH)
    return data["model"], data["feature_names"]


def predict_hit_probability(screen_result: Dict) -> Optional[float]:
    """
    スクリーニング結果1行に対して+2%到達確率を予測する。

    Args:
        screen_result: screen_at_dateの返却辞書

    Returns:
        到達確率（0.0〜1.0）、モデル未読み込み時はNone
    """
    model, feature_names = load_model()
    if model is None:
        return None

    features = {}
    for name in feature_names:
        val = screen_result.get(name, 0)
        if isinstance(val, bool):
            val = int(val)
        try:
            features[name] = float(val) if val is not None else 0.0
        except (TypeError, ValueError):
            features[name] = 0.0

    # SMA乖離率
    close = screen_result.get("close", 0)
    if close and close > 0:
        for sma_key, feat in [("sma5", "sma5_dist_pct"), ("sma20", "sma20_dist_pct"), ("sma60", "sma60_dist_pct")]:
            sma_val = screen_result.get(sma_key, 0)
            if sma_val and sma_val > 0 and feat in feature_names:
                features[feat] = (close - sma_val) / sma_val * 100

    # ATR%
    high = screen_result.get("high_price", 0)
    low = screen_result.get("low_price", 0)
    if high and low and close and close > 0 and "atr_pct" in feature_names:
        features["atr_pct"] = (high - low) / close * 100

    X = pd.DataFrame([features])[feature_names].fillna(0)
    proba = model.predict_proba(X)[0][1]
    return round(float(proba), 3)


if __name__ == "__main__":
    import time
    start = time.time()

    X, y, features = load_and_prepare_data()
    model, report = train_model(X, y, features)
    save_model(model, report, features)

    print(f"\n⏱  完了: {time.time()-start:.1f}秒")
    print(f"   平均AUC: {report['avg_auc']:.3f}")
