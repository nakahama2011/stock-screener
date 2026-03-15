"""
レベル1 単日スクリーニングUI（推定売買可能額付き）

カレンダーで日付を選び、その日付時点の
SMA条件・出来高条件を満たす銘柄を抽出し、
翌日騰落率を一覧表示するStreamlitアプリ。

起動方法:
    /Users/nakahamahirotaka/Library/Python/3.9/bin/streamlit run screener_ui.py
"""

import os
import sys
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta

import pandas as pd
import requests
import streamlit as st

# -- レポート用HTTPサーバー（ポート8502）をバックグラウンドで起動する ----------
import socket, threading, http.server, functools

def _start_report_server():
    """results/ ディレクトリを配信する軽量HTTPサーバーを起動する。"""
    _results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
    if not os.path.isdir(_results_dir):
        return
    # ポート8502がすでに使用中ならスキップする
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as _s:
        if _s.connect_ex(("localhost", 8502)) == 0:
            return  # すでに起動済み
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=_results_dir)
    server = http.server.HTTPServer(("0.0.0.0", 8502), handler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()

_start_report_server()

warnings.filterwarnings("ignore")

# backtester.py と同じディレクトリから import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from backtester import (
    fetch_jpx_tickers,
    _fallback_tickers,
    fetch_ticker_history,
    screen_at_date,
    calc_forward_returns,
    DEFAULT_MIN_VOLUME,
    DEFAULT_HIT_THRESHOLD,
    HISTORY_BUFFER_DAYS,
    MAX_WORKERS,
)

# TradingView Screener API
try:
    from tradingview_screener import Query, col as tv_col
    TV_API_AVAILABLE = True
except ImportError:
    TV_API_AVAILABLE = False


# =====================================================
# ページ設定
# =====================================================
st.set_page_config(
    page_title="📊 日付指定スクリーニング",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =====================================================
# カスタムCSS
# =====================================================
st.markdown("""
<style>
  html, body, [class*="css"] {
    font-family: 'Inter', 'Noto Sans JP', -apple-system, sans-serif;
  }
  /* ページ全体の背景をグレーに */
  .stApp {
    background-color: #f0f2f6 !important;
  }
  section[data-testid="stSidebar"] {
    background-color: #e4e7ed !important;
  }
  [data-testid="metric-container"] {
    background: #1a2332;
    border: 1px solid #2a3a4e;
    border-radius: 10px;
    padding: 1rem !important;
  }
  .app-header {
    background: linear-gradient(135deg, rgba(59,130,246,0.15), rgba(6,182,212,0.08));
    border: 1px solid rgba(59,130,246,0.25);
    border-radius: 12px;
    padding: 1.2rem 1.5rem;
    margin-bottom: 1.5rem;
  }
  .app-header h1 { margin: 0; font-size: 1.6rem; }
  .app-header p { margin: 0.3rem 0 0; color: #8899aa; font-size: 0.9rem; }
  .badge-level1 {
    display: inline-block;
    background: rgba(59,130,246,0.2);
    border: 1px solid rgba(59,130,246,0.4);
    color: #3b82f6;
    padding: 0.15rem 0.6rem;
    border-radius: 6px;
    font-size: 0.75rem;
    font-weight: 700;
    margin-left: 0.5rem;
  }
</style>
""", unsafe_allow_html=True)

# =====================================================
# ヘッダー
# =====================================================
st.markdown("""
<div class="app-header">
  <h1>📊 日付指定スクリーニング <span class="badge-level1">LEVEL 1</span></h1>
  <p>指定した日付の終値時点で条件を満たす銘柄を抽出し、翌日騰落率を一覧表示します</p>
</div>
""", unsafe_allow_html=True)

# 特徴量分析レポートへのリンク（別ページへ案内する）
_report_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results", "feature_analysis_report.html")
if os.path.exists(_report_path):
    st.markdown(
        '<div style="text-align:right;margin-bottom:1rem">'
        '<span style="color:#94a3b8;font-size:0.9rem">'
        '👈 左サイドバーの「📈 特徴量分析レポート」ページでレポート全画面表示'
        '</span></div>',
        unsafe_allow_html=True,
    )

# =====================================================
# サイドバー（設定パネル）
# =====================================================
with st.sidebar:
    st.markdown("## ⚙️ スクリーニング設定")
    st.markdown("---")

    # --- 日付選択 ---
    st.markdown("### 📅 検証日付")

    # 最近5営業日のクイック選択
    quick_dates = []
    d = date.today() - timedelta(days=1)
    while len(quick_dates) < 5:
        if d.weekday() < 5:
            quick_dates.append(d)
        d -= timedelta(days=1)
    quick_labels = [
        f"{d.strftime('%m/%d')}（{['月','火','水','木','金','土','日'][d.weekday()]}）"
        for d in quick_dates
    ]

    quick_choice = st.selectbox(
        "クイック選択（最近の営業日）",
        options=["カレンダーで指定"] + quick_labels,
        index=0,
        key="quick_choice",
    )

    if quick_choice == "カレンダーで指定":
        selected_date = st.date_input(
            "カレンダーで選択",
            value=date.today() - timedelta(days=1),
            min_value=date(2020, 1, 1),
            max_value=date.today(),
            key="cal_date",
        )
    else:
        idx = quick_labels.index(quick_choice)
        selected_date = quick_dates[idx]
        st.info(f"選択中: **{selected_date.strftime('%Y年%m月%d日')}**")

    st.markdown("---")

    # --- スクリーニング条件 ---
    st.markdown("### 📈 スクリーニング条件")

    min_volume = st.number_input(
        "最低出来高（株）",
        min_value=100_000,
        max_value=10_000_000,
        value=DEFAULT_MIN_VOLUME,
        step=100_000,
        format="%d",
        key="min_vol",
    )

    hit_threshold = st.slider(
        "利確目標（%）",
        min_value=0.5,
        max_value=5.0,
        value=3.0,
        step=0.5,
        key="hit_thr",
    )

    st.info(
        f"**基本条件**\n"
        f"- SMA5 > SMA20 > SMA60\n"
        f"- 出来高 ≥ {min_volume:,}株\n"
        f"- 達成フラグ: +{hit_threshold:.1f}%以上"
    )

    st.markdown("---")

    # --- 追加条件 ---
    st.markdown("### ③ 追加条件（オプション）")
    use_pullback = st.checkbox(
        "プルバック条件を有効にする",
        value=False,
        key="use_pullback",
        help="価格 < SMA5 かつ 価格 > SMA20\n（5日線を割り込んでいるが20日線は維持している押し目）",
    )
    if use_pullback:
        st.success(
            "✅ ON: 価格 < SMA5  かつ  価格 > SMA20\n\n"
            "上昇トレンド継続中の一時的な押し目を狙うフィルターです。"
        )

    st.markdown("---")

    # --- 直近高値条件 ---
    use_near_high = st.checkbox(
        "直近高値条件を有効にする",
        value=False,
        key="use_near_high",
        help="終値が直近N日間の高値から指定%以内のとき合致（高値圏にある銘柄を狙う）",
    )
    if use_near_high:
        near_high_pct = st.slider(
            "高値からの乖離許容幅（%以内）",
            min_value=1.0, max_value=10.0, value=3.0, step=0.5,
            key="near_high_pct",
        )
        near_high_days = st.selectbox(
            "高値算出期間",
            options=[20, 40, 60, 90, 120],
            index=2,
            key="near_high_days",
            format_func=lambda x: f"直近{x}営業日",
        )
        st.info(f"✅ ON: 終値が直近{near_high_days}日高値から {near_high_pct:.1f}% 以内")
    else:
        near_high_pct = 0.0
        near_high_days = 60

    st.markdown("---")

    # --- 実行ボタン ---
    run_button = st.button(
        f"🔍 {selected_date.strftime('%Y/%m/%d')} でスクリーニング実行",
        type="primary",
        use_container_width=True,
    )


# =====================================================
# TradingView APIで候補銘柄を事前取得（プレフィルタ）
# =====================================================
def _fetch_tv_candidates(min_volume_val: int = DEFAULT_MIN_VOLUME) -> dict:
    """
    TradingView Screener APIで、SMA順行配列+出来高条件を満たす
    日本株の候補銘柄コードと名前を取得する。
    JPX銘柄一覧から日本語名を取得してマッピングする。

    Returns:
        dict: {銘柄コード(int): 銘柄名(str)} のマッピング
    """
    if not TV_API_AVAILABLE:
        return {}

    try:
        (count, df) = (Query()
            .set_markets('japan')
            .select('name', 'description', 'close', 'volume',
                    'SMA5', 'SMA20', 'SMA60')
            .where(
                tv_col('SMA5') > tv_col('SMA20'),
                tv_col('SMA20') > tv_col('SMA60'),
                tv_col('volume') > min_volume_val,
            )
            .order_by('volume', ascending=False)
            .limit(500)
            .get_scanner_data())

        # JPX銘柄一覧から日本語名を取得
        jpx_names = {}
        try:
            import io as _io
            _jpx_url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
            _resp = requests.get(_jpx_url, timeout=30)
            _resp.raise_for_status()
            _jpx_df = pd.read_excel(_io.BytesIO(_resp.content))
            _jpx_df = _jpx_df.rename(columns={"コード": "code", "銘柄名": "name"})
            _jpx_df = _jpx_df[pd.to_numeric(_jpx_df["code"], errors="coerce").notna()]
            _jpx_df["code"] = _jpx_df["code"].astype(int)
            jpx_names = dict(zip(_jpx_df["code"], _jpx_df["name"]))
        except Exception:
            pass

        candidates = {}
        for _, row in df.iterrows():
            ticker_str = str(row.get("ticker", ""))
            code_str = ticker_str.split(":")[-1] if ":" in ticker_str else ticker_str
            try:
                code = int(code_str)
                # JPX日本語名を優先、なければTVの英語名を使用
                name = jpx_names.get(code, str(row.get("description", row.get("name", ""))))
                candidates[code] = name
            except ValueError:
                continue
        return candidates
    except Exception:
        return {}


# =====================================================
# データ取得関数（キャッシュ付き — 日付とサンプルフラグのみで管理）
# =====================================================
@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_all_data(as_of_date_str: str, use_sample_val: bool):
    """
    TradingView APIで候補銘柄を特定し、その候補のみ yfinance で
    株価データを取得してキャッシュする。
    TV APIが利用不可の場合は従来通り全銘柄を取得する。
    """
    as_of_dt = datetime.strptime(as_of_date_str, "%Y-%m-%d")
    fetch_start = (as_of_dt - timedelta(days=HISTORY_BUFFER_DAYS)).strftime("%Y-%m-%d")
    fetch_end = (as_of_dt + timedelta(days=14)).strftime("%Y-%m-%d")

    # TradingView APIで候補銘柄を事前取得（プレフィルタ）
    tv_candidates = _fetch_tv_candidates()

    if tv_candidates and not use_sample_val:
        # TV APIの候補銘柄のみを対象にする（3,800銘柄→約100銘柄に削減）
        tickers_data = [(code, name) for code, name in tv_candidates.items()]
        total = len(tickers_data)
    else:
        # フォールバック：従来通り全銘柄を取得する
        tickers_df = _fallback_tickers() if use_sample_val else fetch_jpx_tickers()
        tickers_data = [(int(row["code"]), str(row.get("name", ""))) for _, row in tickers_df.iterrows()]
        total = len(tickers_data)

    # 並列データ取得
    all_data = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for code, name in tickers_data:
            f = executor.submit(fetch_ticker_history, code, name, fetch_start, fetch_end)
            futures[f] = (code, name)

        for f in as_completed(futures):
            code, name = futures[f]
            try:
                res = f.result()
                if res is not None:
                    _, _, df = res
                    all_data[code] = (name, df)
            except Exception:
                pass

    return all_data, total


# =====================================================
# スクリーニング実行関数（キャッシュなし — 条件変更時に即座に再判定）
# =====================================================
def run_single_day_screen(
    as_of_date_str: str,
    min_volume_val: int,
    hit_threshold_val: float,
    use_sample_val: bool,
    use_pullback_val: bool = False,
    near_high_pct_val: float = 0.0,
    near_high_days_val: int = 60,
):
    """
    指定日のスクリーニングを実行する。
    データ取得はキャッシュされた _fetch_all_data を利用し、
    スクリーニング条件の適用はキャッシュ外で毎回実行する。

    Returns:
        tuple: (result_df, error_msg, date_labels)
    """
    # キャッシュ済みデータを取得（条件に関係なく同一データを使う）
    all_data, total = _fetch_all_data(as_of_date_str, use_sample_val)

    as_of_dt = datetime.strptime(as_of_date_str, "%Y-%m-%d")
    signal_ts = pd.Timestamp(as_of_dt)
    rows = []

    for code, (name, df) in all_data.items():
        screen_result = screen_at_date(
            df, signal_ts, min_volume_val,
            use_pullback=use_pullback_val,
            near_high_pct=near_high_pct_val,
            near_high_days=near_high_days_val,
        )
        if screen_result is None:
            continue

        fwd = calc_forward_returns(df, signal_ts, hit_threshold_val)

        # hit_2pct_5d を判定（5日以内の高値が+2%以上）
        hit_5d_val = None
        max_5d = fwd.get("max_ret_5d")
        if max_5d is not None:
            hit_5d_val = 1 if max_5d >= hit_threshold_val else 0

        rows.append({
            "銘柄コード": code,
            "銘柄名": name,
            # ―― 過去の値動き ――
            "前々日(%)": screen_result.get("prev_prev_day_change_pct"),
            "前日(%)": screen_result.get("prev_day_change_pct"),
            "当日(%)": screen_result.get("day_change_pct"),
            # ―― 将来の値動き ――
            "明日(%)": fwd.get("ret_1d"),
            "明後日(%)": fwd.get("ret_2d"),
            "3日後(%)": fwd.get("ret_3d"),
            "4日後(%)": fwd.get("ret_4d"),
            "5日後(%)": fwd.get("ret_5d"),
            # ―― +3%到達関連 ――
            f"+{hit_threshold_val:.0f}%到達(5日)": hit_5d_val,
            "3日以内最大(%)": fwd.get("max_ret_3d"),
            "5日以内最大(%)": fwd.get("max_ret_5d"),
            f"_hit_{hit_threshold_val:.0f}pct_1d": fwd.get(f"hit_{hit_threshold_val:.0f}pct_1d"),
            f"_hit_{hit_threshold_val:.0f}pct_3d": fwd.get(f"hit_{hit_threshold_val:.0f}pct_3d"),
            f"_hit_{hit_threshold_val:.0f}pct_5d": fwd.get(f"hit_{hit_threshold_val:.0f}pct_5d"),
            "+3%到達日": fwd.get("days_to_target"),
            # ―― 出来高・ボラ ――
            "出来高": screen_result["volume"],
            "出来高増減(%)": screen_result.get("volume_change_pct"),
            "出来高比(20MA)": screen_result["volume_ratio"],
            "ATR%": screen_result.get("atr_pct"),
            "RSI(14)": screen_result.get("rsi"),
            # 推定売買可能額計算用
            "_volume_ratio_raw": screen_result.get("volume_ratio", 0),
            # ―― スコアリング用内部フィールド ――
            "_close": screen_result["close"],
            "_sma5": screen_result["sma5"],
            "_sma20": screen_result["sma20"],
            "_sma60": screen_result["sma60"],
            "_weekly_sma20_ok":       screen_result.get("weekly_sma20_ok", False),
            "_vol_today_vs_yday_pct": screen_result.get("vol_today_vs_yday_pct"),
            "_is_pullback":           screen_result.get("is_pullback", False),
            "_is_breakout":           screen_result.get("is_breakout", False),
            "_long_upper_wick":       screen_result.get("long_upper_wick", False),
            "_is_high_zone":          screen_result.get("is_high_zone", False),
            "_big_bearish_yesterday": screen_result.get("big_bearish_yesterday", False),
            "_first_sma20_touch":     screen_result.get("first_sma20_touch", False),
            "_sma20_touch_count":     screen_result.get("sma20_touch_count", 0),
            "_trend_days":            screen_result.get("trend_start_days_ago", 0),
            "_day_of_week":           screen_result.get("day_of_week", -1),
            "_screen_result":         screen_result,
        })

    if not rows:
        return pd.DataFrame(), f"条件に合致する銘柄が見つかりませんでした（{total}銘柄を検索）", {}

    result_df = pd.DataFrame(rows)

    # None を NaN に統一する（object型列の Python None は na_rep が効かないため明示変換する）
    for col in result_df.columns:
        result_df[col] = pd.to_numeric(result_df[col], errors="ignore")
        result_df[col] = result_df[col].where(result_df[col].notna(), other=float("nan"))

    # 実際の取引日を取得して日付ラベルを作成する
    date_labels: dict = {}
    for _code, (_name, _df) in all_data.items():
        _past  = _df[_df.index <= signal_ts]
        _future = _df[_df.index > signal_ts]
        if len(_past) >= 3:
            t   = _past.index[-1]
            t_1 = _past.index[-2]
            t_2 = _past.index[-3]

            date_labels = {
                "前々日(%)": f"{t_2.month}/{t_2.day}",
                "前日(%)": f"{t_1.month}/{t_1.day}",
                "当日(%)": f"{t.month}/{t.day}",
            }

            # 将来の営業日を取得する（データがあればそこから、なければカレンダーで推定する）
            if len(_future) >= 5:
                for j, label in enumerate(["明日(%)", "明後日(%)", "3日後(%)", "4日後(%)", "5日後(%)"]):
                    date_labels[label] = f"{_future.index[j].month}/{_future.index[j].day}"
            else:
                # データがない場合は営業日を推定する（土日を飛ばす）
                from datetime import timedelta
                _next = t.to_pydatetime()
                for label in ["明日(%)", "明後日(%)", "3日後(%)", "4日後(%)", "5日後(%)"]:
                    _next += timedelta(days=1)
                    while _next.weekday() >= 5:  # 土日を飛ばす
                        _next += timedelta(days=1)
                    date_labels[label] = f"{_next.month}/{_next.day}"
            break

    # 表示する列と順序を定義する
    hit_col_name = f"+{hit_threshold_val:.0f}%到達(5日)"
    display_cols = [
        "銘柄コード", "銘柄名",
        "AI予測(%)",
        "🏆TOP該当",
        "+3%到達日",
        "翌日到達",                     # 翌日の高値が+3%以上か
        "3日目到達",                    # 3日以内の高値が+3%以上か
        "5日目到達",                    # 5日以内の高値が+3%以上か
        "前々日(%)", "前日(%)", "当日(%)",  # 過去日次リターン
        "明日(%)", "明後日(%)", "3日後(%)", "4日後(%)", "5日後(%)",  # 将来日次リターン
        "出来高",
        "RSI(14)",
        "ATR%",
    ]
    # スコアリング用の内部列（_ プレフィックス）も一緒に保持する
    score_internal_cols = [c for c in result_df.columns if c.startswith("_")]

    # 推定売買可能額を計算（20日平均出来高 × 終値 × 1%）
    has_vr = "_volume_ratio_raw" in result_df.columns
    has_cl = "_close" in result_df.columns
    has_vol = "出来高" in result_df.columns
    if has_vr and has_cl and has_vol:
        vol_ratio = pd.to_numeric(result_df["_volume_ratio_raw"], errors="coerce").replace(0, float("nan"))
        vol_20ma = pd.to_numeric(result_df["出来高"], errors="coerce") / vol_ratio
        close = pd.to_numeric(result_df["_close"], errors="coerce")
        result_df["推定売買可能額"] = vol_20ma * close * 0.01
    else:
        # 条件が合わない場合でもNaN列を作成
        result_df["推定売買可能額"] = float("nan")

    # 翌日到達・3日目到達・5日目到達 列を作成（○/✕）
    hit_1d_col = f"_hit_{hit_threshold_val:.0f}pct_1d"
    hit_3d_col = f"_hit_{hit_threshold_val:.0f}pct_3d"
    hit_5d_col = f"_hit_{hit_threshold_val:.0f}pct_5d"
    if hit_1d_col in result_df.columns:
        result_df["翌日到達"] = result_df[hit_1d_col].apply(
            lambda x: "○" if x == 1 else "✕" if pd.notna(x) else "")
    if hit_3d_col in result_df.columns:
        result_df["3日目到達"] = result_df[hit_3d_col].apply(
            lambda x: "○" if x == 1 else "✕" if pd.notna(x) else "")
    if hit_5d_col in result_df.columns:
        result_df["5日目到達"] = result_df[hit_5d_col].apply(
            lambda x: "○" if x == 1 else "✕" if pd.notna(x) else "")

    # 既存列だけ抽出（KPI計算用列も保持）
    kpi_internal_cols = ["5日以内最大(%)", "3日以内最大(%)", "+3%到達日"]
    existing = [c for c in display_cols if c in result_df.columns] + score_internal_cols
    for kc in kpi_internal_cols:
        if kc in result_df.columns and kc not in existing:
            existing.append(kc)
    result_df = result_df[existing]

    # ※ AI予測(%)列は後段で計算されるため、ここでは銘柄コード順で返す
    result_df = result_df.sort_values("銘柄コード", ascending=True, na_position="last")
    return result_df, "", date_labels


# =====================================================
# ボタン押下時：実行して session_state に保存
# =====================================================
if run_button:
    as_of_str = selected_date.strftime("%Y-%m-%d")

    with st.spinner(f"⏳ {as_of_str} のデータを取得・分析中..."):
        # 当日検索時はキャッシュをクリアして最新データを取得する
        from datetime import date as _date_cls
        if as_of_str == _date_cls.today().strftime("%Y-%m-%d"):
            _fetch_all_data.clear()

        result_df, err_msg, date_labels = run_single_day_screen(
            as_of_str,
            min_volume,
            hit_threshold,
            False,  # 常に全銘柄モード
            use_pullback,
            near_high_pct,
            near_high_days,
        )

    # 結果を session_state に保存（フィルタ操作時も保持される）
    st.session_state["result_df"] = result_df
    st.session_state["result_err"] = err_msg
    st.session_state["result_date"] = as_of_str
    st.session_state["result_hit_thr"] = hit_threshold
    st.session_state["date_labels"] = date_labels


# =====================================================
# 結果表示（session_state から取り出す）
# =====================================================
if "result_df" in st.session_state:
    result_df = st.session_state["result_df"]
    err_msg = st.session_state.get("result_err", "")
    as_of_str = st.session_state.get("result_date", "")
    saved_hit_thr = st.session_state.get("result_hit_thr", hit_threshold)

    if err_msg:
        st.warning(f"⚠️ {err_msg}")

    elif not result_df.empty:
        n = len(result_df)
        date_label = datetime.strptime(as_of_str, "%Y-%m-%d").strftime("%Y年%m月%d日")
        st.markdown(f"## 📋 {date_label} のスクリーニング結果")

        # ---- KPIサマリー（フィルタ後のデータで計算するため、後で描画） ----
        hit_col = f"+{saved_hit_thr:.0f}%到達(5日)"

        # =====================================================
        # ▼ フィルタ・ソート・検索  ← session_state で状態保持
        # =====================================================
        col_f1, col_f2, col_f3 = st.columns([2, 2, 3])

        with col_f1:
            show_filter = st.selectbox(
                "フィルタ",
                [
                    "すべて表示",
                    "🏆 高勝率コンボ",
                    "🎯 RSI 30-50（割安）",
                    "🎯 RSI 50-65（適正）",
                    "📉 当日マイナス（押し目買い）",
                    "📉 前日マイナス（反発期待）",
                    "🔮 明日+予測（高確信）",
                    "🔮 明日+予測（候補）",
                    "📈 当日↑ 前日↓ 前々日↓",
                    "📉 3日連続マイナス",
                    "明日プラスのみ",
                    "明日マイナスのみ",
                    f"+{saved_hit_thr:.0f}%到達のみ",
                    f"3日以内+{saved_hit_thr:.0f}%到達",
                    f"5日以内+{saved_hit_thr:.0f}%到達",
                    "🏆 TOP30該当のみ",
                    "🎯 初回SMA20タッチ",
                ],
                key="show_filter",
            )
        with col_f2:
            sort_col = st.selectbox(
                "並び順",
                ["AI予測（降順）", "到達日（昇順）", "明日（降順）", "明日（昇順）", "出来高（降順）", "銘柄コード"],
                key="sort_col",
            )
        with col_f3:
            search_input = st.text_input(
                "🔍 銘柄コード・名前で検索",
                placeholder="例: 7203 または トヨタ",
                key="search_input",
            )

        # ---- フィルタ適用 ----
        # result_df は session_state からの DataFrame（変更しない）
        display_df = result_df.copy()

        # 検索
        if search_input:
            mask = (
                display_df["銘柄コード"].astype(str).str.contains(search_input, na=False) |
                display_df["銘柄名"].str.contains(search_input, na=False)
            )
            display_df = display_df[mask]

        # 明日プラス予測スコアを計算する（フィルタ・表示の両方で使用するため事前に計算）
        def _calc_score(row) -> int:
            """明日プラス予測スコアを計算する（加点 최大100点・減点最大 -45点）。"""
            score = 0

            # ====== 加点 ======

            # ① SMA5 > SMA20 > SMA60（スクリーニング通過済み → 全件加点）+25
            score += 25

            # ② 週足 SMA20 を上回っている +15
            if row.get("_weekly_sma20_ok"):
                score += 15

            # ③ セクター強代替：出来高比(20MA) ≥ 1.10 +20
            try:
                if float(row.get("出来高比(20MA)", 0)) >= 1.10:
                    score += 20
            except (TypeError, ValueError):
                pass

            # ④ RSI 50〜65 +15
            try:
                rsi = float(row.get("RSI(14)", 0))
                if 50.0 <= rsi <= 65.0:
                    score += 15
            except (TypeError, ValueError):
                pass

            # ⑤ 当日出来高が前日比 110〜130% +10
            try:
                vty = float(row.get("_vol_today_vs_yday_pct", 0))
                if 10.0 <= vty <= 30.0:
                    score += 10
            except (TypeError, ValueError):
                pass

            # ⑥ 位置（押し目 or 初動） +10
            if row.get("_is_pullback") or row.get("_is_breakout"):
                score += 10

            # ⑦ 曜日補正：火〜木（weekday 1〜3）+5
            try:
                dow = int(row.get("_day_of_week", -1))
                if dow in (1, 2, 3):
                    score += 5
            except (TypeError, ValueError):
                pass

            # ====== 減点 ======

            # A) RSI70 超 -10
            try:
                if float(row.get("RSI(14)", 0)) > 70.0:
                    score -= 10
            except (TypeError, ValueError):
                pass

            # B) 長大上ヒゲ -15
            if row.get("_long_upper_wick"):
                score -= 15

            # C) 高値圏終盤（20日高値から 3% 以内） -15
            if row.get("_is_high_zone"):
                score -= 15

            # D) 大陰線直後 -20
            if row.get("_big_bearish_yesterday"):
                score -= 20

            return score

        display_df = display_df.copy()
        display_df["予測スコア"] = display_df.apply(_calc_score, axis=1)

        # ---- 回転スコア計算（米国株と同じロジック） ----
        def _calc_rotation_score(row):
            score = 0
            try:
                rsi = float(row.get("RSI(14)", 50))
                if 30 <= rsi <= 50: score += 20
                elif 50 < rsi <= 55: score += 10
            except: pass
            try:
                vr = float(row.get("出来高比(20MA)", 0))
                if vr >= 2.0: score += 25
                elif vr >= 1.5: score += 15
                elif vr >= 1.2: score += 5
            except: pass
            if row.get("_is_pullback"): score += 15
            if row.get("_big_bearish_yesterday"): score += 10
            try:
                dc = float(row.get("当日(%)", 0))
                if dc < -1.0: score += 10
                elif dc < 0: score += 5
            except: pass
            try:
                dow = int(row.get("_day_of_week", -1))
                if dow in [1, 2, 3]: score += 5
            except: pass
            try:
                if float(row.get("RSI(14)", 0)) > 62: score -= 10
            except: pass
            if row.get("_long_upper_wick"): score -= 10
            if row.get("_is_high_zone"): score -= 10
            return max(score, 0)

        display_df["回転スコア"] = display_df.apply(_calc_rotation_score, axis=1)

        # 到達日列のリネーム
        if "+3%到達日" in display_df.columns:
            display_df["到達日"] = display_df["+3%到達日"]

        # ---- AI予測確率の計算（日本株専用モデル） ----
        try:
            import joblib as _jl
            _jp_model_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results", "jp_ml_model.pkl")
            if os.path.exists(_jp_model_path):
                _model_data = _jl.load(_jp_model_path)
                _jp_model = _model_data["model"]
                _jp_features = _model_data["feature_names"]

                def _predict_jp(row):
                    sr = row.get("_screen_result")
                    if sr is None:
                        return None
                    try:
                        features = {}
                        for name in _jp_features:
                            val = sr.get(name, 0)
                            if isinstance(val, bool): val = int(val)
                            try: features[name] = float(val) if val is not None else 0.0
                            except: features[name] = 0.0
                        close = sr.get("close", 0)
                        if close and close > 0:
                            for sk, fn in [("sma5","sma5_dist_pct"),("sma20","sma20_dist_pct"),("sma60","sma60_dist_pct")]:
                                sv = sr.get(sk, 0)
                                if sv and sv > 0 and fn in _jp_features:
                                    features[fn] = (close - sv) / sv * 100
                        high = sr.get("high_price", 0)
                        low = sr.get("low_price", 0)
                        if high and low and close and close > 0 and "price_range_pct" in _jp_features:
                            features["price_range_pct"] = (high - low) / close * 100
                        X = pd.DataFrame([features])[_jp_features].fillna(0)
                        return round(float(_jp_model.predict_proba(X)[0][1]) * 100, 1)
                    except:
                        return None

                display_df["AI予測(%)"] = display_df.apply(_predict_jp, axis=1)
            else:
                display_df["AI予測(%)"] = None
        except Exception:
            display_df["AI予測(%)"] = None

        # _screen_resultの削除はTOPマッチング後に行う（レンジ幅条件で必要）

        # ---- フィルターコンボランキングのマッチング ----
        _ranking_json_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "results", "filter_ranking.json",
        )
        if os.path.exists(_ranking_json_path):
            import json as _json
            with open(_ranking_json_path, "r", encoding="utf-8") as _cf:
                _ranking_data = _json.load(_cf)
            _top_combos = _ranking_data.get("combo", [])[:15]

            def _check_condition(row, cond_name):
                """1つの条件を銘柄データに対して評価する"""
                rsi = float(row.get("RSI(14)", 0) or 0)
                vol_ratio = float(row.get("出来高比(20MA)", 0) or 0)
                day_pct = float(row.get("当日(%)", 0) or 0)
                prev_pct = float(row.get("前日(%)", 0) or 0)
                atr = float(row.get("ATR%", 0) or 0)
                dow = int(row.get("_day_of_week", -1))
                # SMA乖離率は内部データから計算
                close = float(row.get("_close", 0) or 0)
                sma60 = float(row.get("_sma60", 0) or 0)
                sma20 = float(row.get("_sma20", 0) or 0)
                sma60_dist = ((close - sma60) / sma60 * 100) if sma60 > 0 else 0
                sma20_dist = ((close - sma20) / sma20 * 100) if sma20 > 0 else 0

                # RSI条件
                if cond_name == "RSI≤40": return rsi <= 40
                elif cond_name == "RSI≤50": return rsi <= 50
                elif cond_name == "RSI 30-50": return 30 <= rsi <= 50
                elif cond_name == "RSI 40-55": return 40 <= rsi <= 55
                elif cond_name == "RSI 50-65": return 50 <= rsi <= 65
                # 出来高条件
                elif cond_name == "出来高比≥1.2": return vol_ratio >= 1.2
                elif cond_name == "出来高比≥1.5": return vol_ratio >= 1.5
                elif cond_name == "出来高比≥2.0": return vol_ratio >= 2.0
                # 騰落率条件
                elif cond_name == "当日↓(マイナス)": return day_pct < 0
                elif cond_name == "当日≤-1%": return day_pct <= -1
                elif cond_name == "当日≤-2%": return day_pct <= -2
                elif cond_name == "前日↓(マイナス)": return prev_pct < 0
                elif cond_name == "前日≤-1%": return prev_pct <= -1
                # ATR条件
                elif cond_name == "ATR%≥2.5": return atr >= 2.5
                elif cond_name == "ATR%≥3.0": return atr >= 3.0
                elif cond_name == "ATR%≥4.0": return atr >= 4.0
                # SMA乖離条件
                elif cond_name == "SMA60乖離≤5%": return sma60_dist <= 5
                elif cond_name == "SMA60乖離≤10%": return sma60_dist <= 10
                elif cond_name == "SMA20乖離≤3%": return sma20_dist <= 3
                # レンジ幅（内部データから計算）
                elif cond_name == "レンジ幅≥3%" or cond_name == "レンジ幅≥4%":
                    sr = row.get("_screen_result") if "_screen_result" in row.index else None
                    if sr and isinstance(sr, dict):
                        hp = float(sr.get("high_price", 0) or 0)
                        lp = float(sr.get("low_price", 0) or 0)
                        if close > 0 and hp > 0 and lp > 0:
                            rng = (hp - lp) / close * 100
                            thr = 4.0 if "4%" in cond_name else 3.0
                            return rng >= thr
                    return False
                # フラグ条件
                elif cond_name == "プルバック": return bool(row.get("_is_pullback"))
                elif cond_name == "前日大陰線": return bool(row.get("_big_bearish_yesterday"))
                # 旧条件名の互換性
                elif cond_name == "当日陰線": return day_pct < 0
                elif cond_name == "前日陰線": return prev_pct < 0
                elif cond_name == "当日陽線": return day_pct > 0
                elif cond_name == "前日陽線": return prev_pct > 0
                elif cond_name == "20日高値ブレイク": return bool(row.get("_is_breakout"))
                elif cond_name == "長大上ヒゲ": return bool(row.get("_long_upper_wick"))
                elif cond_name == "高値圏(20日HH 3%以内)": return bool(row.get("_is_high_zone"))
                elif cond_name == "週足SMA20上抜け": return bool(row.get("_weekly_sma20_ok"))
                elif cond_name == "火〜木曜": return dow in [1, 2, 3]
                return False

            def _match_top_combos(row):
                """銘柄が該当するTOPコンボを特定する"""
                matched = []
                for i, combo in enumerate(_top_combos):
                    c1 = combo.get("条件1", "")
                    c2 = combo.get("条件2", "")
                    if _check_condition(row, c1) and _check_condition(row, c2):
                        rank = i + 1
                        medal = "🥇" if rank == 1 else "🥈" if rank == 2 else "🥉" if rank == 3 else f"#{rank}"
                        matched.append(medal)
                        if len(matched) >= 5:
                            break
                return " ".join(matched) if matched else ""

            display_df["🏆TOP該当"] = display_df.apply(_match_top_combos, axis=1)
            # 🏆TOP該当列を一番左に配置する
            cols = display_df.columns.tolist()
            if "🏆TOP該当" in cols:
                cols.remove("🏆TOP該当")
                cols.insert(0, "🏆TOP該当")
                display_df = display_df[cols]

        # _screen_resultは表示に不要なので削除
        if "_screen_result" in display_df.columns:
            display_df = display_df.drop(columns=["_screen_result"])

        # フィルタ
        if show_filter == "🏆 高勝率コンボ":
            # 特徴量分析レポートの上位条件: RSI 30-65 + (押し目 or 当日マイナス)
            rsi_mask = (display_df["RSI(14)"] >= 30) & (display_df["RSI(14)"] <= 65)
            pullback_or_dip = (
                (display_df["_is_pullback"] == True) |
                (display_df["当日(%)"] < 0)
            )
            display_df = display_df[rsi_mask & pullback_or_dip]
        elif show_filter == "🎯 RSI 30-50（割安）":
            # RSI 30-50は勝率86.2%（全体比+5.1%）
            display_df = display_df[
                (display_df["RSI(14)"] >= 30) & (display_df["RSI(14)"] <= 50)
            ]
        elif show_filter == "🎯 RSI 50-65（適正）":
            # RSI 50-65は勝率84.0%（全体比+2.9%）
            display_df = display_df[
                (display_df["RSI(14)"] > 50) & (display_df["RSI(14)"] <= 65)
            ]
        elif show_filter == "📉 当日マイナス（押し目買い）":
            # 当日マイナスの銘柄は5日以内勝率+2〜4%高い
            display_df = display_df[display_df["当日(%)"] < 0]
        elif show_filter == "📉 前日マイナス（反発期待）":
            # 前日マイナスの銘柄は反発傾向（勝率+2%）
            display_df = display_df[display_df["前日(%)"] < 0]
        elif show_filter == "🔮 明日+予測（高確信）":
            display_df = display_df[display_df["予測スコア"] >= 60]
        elif show_filter == "🔮 明日+予測（候補）":
            display_df = display_df[display_df["予測スコア"] >= 40]
        elif show_filter == "📈 当日↑ 前日↓ 前々日↓":
            # 当日がプラス、前日がマイナス、前々日がマイナスの銘柄に絞り込む
            display_df = display_df[
                (display_df["当日(%)"] > 0) &
                (display_df["前日(%)"] < 0) &
                (display_df["前々日(%)"] < 0)
            ]
        elif show_filter == "📉 3日連続マイナス":
            display_df = display_df[
                (display_df["当日(%)"] < 0) &
                (display_df["前日(%)"] < 0) &
                (display_df["前々日(%)"] < 0)
            ]
        elif show_filter == "明日プラスのみ":
            display_df = display_df[display_df["明日(%)"] > 0]
        elif show_filter == "明日マイナスのみ":
            display_df = display_df[display_df["明日(%)"] < 0]
        elif "達成のみ" in show_filter:
            if "5日以内最大(%)" in display_df.columns:
                display_df = display_df[display_df["5日以内最大(%)"] >= 2.0]
        elif show_filter == f"3日以内+{saved_hit_thr:.0f}%到達":
            if "3日以内最大(%)" in display_df.columns:
                display_df = display_df[display_df["3日以内最大(%)"] >= saved_hit_thr]
        elif show_filter == f"5日以内+{saved_hit_thr:.0f}%到達":
            if "5日以内最大(%)" in display_df.columns:
                display_df = display_df[display_df["5日以内最大(%)"] >= saved_hit_thr]
        elif show_filter == "🏆 TOP30該当のみ":
            if "🏆TOP該当" in display_df.columns:
                display_df = display_df[display_df["🏆TOP該当"].astype(str).str.len() > 0]
        elif show_filter == "🎯 初回SMA20タッチ":
            if "_first_sma20_touch" in display_df.columns:
                display_df = display_df[display_df["_first_sma20_touch"] == True]

        # ソート
        if sort_col == "AI予測（降順）" and "AI予測(%)" in display_df.columns:
            display_df = display_df.sort_values("AI予測(%)", ascending=False, na_position="last")
        elif sort_col == "回転スコア（降順）":
            display_df = display_df.sort_values("回転スコア", ascending=False, na_position="last")
        elif sort_col == "到達日（昇順）" and "+3%到達日" in display_df.columns:
            display_df = display_df.sort_values("+3%到達日", ascending=True, na_position="last")
        elif sort_col == "明日（降順）":
            display_df = display_df.sort_values("明日(%)", ascending=False, na_position="last")
        elif sort_col == "明日（昇順）":
            display_df = display_df.sort_values("明日(%)", ascending=True, na_position="last")
        elif sort_col == "出来高（降順）":
            display_df = display_df.sort_values("出来高", ascending=False)
        elif sort_col == "銘柄コード":
            display_df = display_df.sort_values("銘柄コード")

        st.markdown(f"**{len(display_df)}件 表示中**（全 {n}件）")

        # ---- KPIサマリー（フィルタ後のデータで計算） ----
        # 全銘柄を対象にする（NaNは「未達」としてカウント）
        n_total = len(display_df)
        col_k1, col_k2, col_k3, col_k4 = st.columns(4)
        with col_k1:
            st.metric("候補銘柄数", f"{n_total}件")
        with col_k2:
            _tomorrow_valid = display_df[display_df["明日(%)"].notna()]
            n_1d = len(_tomorrow_valid)
            if n_1d > 0:
                win_rate_1d = (_tomorrow_valid["明日(%)"] > 0).sum() / n_1d * 100
                st.metric("明日勝率", f"{win_rate_1d:.1f}% ({n_1d}件)")
            else:
                st.metric("明日勝率", "N/A")
        with col_k3:
            if "5日以内最大(%)" in display_df.columns:
                _col_5d = display_df["5日以内最大(%)"].dropna()
                n_hit = int((_col_5d >= saved_hit_thr).sum())
                rate_hit = n_hit / n_total * 100 if n_total > 0 else 0
                st.metric(f"+{saved_hit_thr:.0f}%到達率(5日)", f"{rate_hit:.1f}% ({n_hit}/{n_total})")
            else:
                st.metric(f"+{saved_hit_thr:.0f}%到達率(5日)", "N/A")
        with col_k4:
            _d2t_col = None
            if "+3%到達日" in display_df.columns:
                _d2t_col = display_df["+3%到達日"].dropna()
            elif "到達日" in display_df.columns:
                _d2t_col = display_df["到達日"].dropna()
            if _d2t_col is not None and len(_d2t_col) > 0:
                avg_days = _d2t_col.mean()
                st.metric("平均到達日数", f"{avg_days:.1f}日 ({len(_d2t_col)}件)")
            else:
                st.metric("平均到達日数", "N/A")

        # ---- 日付ラベルでカラムをリネームしてから表示する ----
        date_labels = st.session_state.get("date_labels", {})
        # フィルタ・ソートは内部名で完了済みなので、ここでリネームする
        display_df = display_df.rename(columns=date_labels)

        # 騰落率列の名称（リネーム後）
        _ret_internals = ["前々日(%)", "前日(%)", "当日(%)", "明日(%)", "明後日(%)", "3日後(%)", "4日後(%)", "5日後(%)"]
        ret_cols = set(date_labels.get(c, c) for c in _ret_internals)

        import math as _math
        import json as _json

        def _fmt_val(col, val):
            """列に応じた表示文字列とスタイル文字列を返す。"""
            # 🏆TOP該当列はそのまま文字列表示する
            if col == "🏆TOP該当":
                s = str(val).strip() if val else ""
                if s and s != "nan":
                    return s, "font-weight:bold;color:#fbbf24"
                return "—", ""

            try:
                v = float(val)
                is_nan = _math.isnan(v)
            except (TypeError, ValueError):
                # ○/✕ 表示列
                if col in ["翌日到達", "3日目到達", "5日目到達"]:
                    if val == "○":
                        return "○", "color:#10b981;font-weight:bold;text-align:center"
                    elif val == "✕":
                        return "✕", "color:#ef4444;text-align:center"
                return "—", ""

            if is_nan:
                return "—", ""

            # 騰落率・出来高増減
            if col in ret_cols or col == "出来高増減(%)":
                pct_str = f"{v:+.2f}%"
                intensity = min(int(abs(v) / 5 * 80), 70)
                if col == "出来高増減(%)":
                    intensity = min(int(abs(v) / 50 * 80), 70)
                if v > 0:
                    style = (f"background:rgba(16,185,129,0.{intensity:02d});"
                             f"color:#10b981;font-weight:bold")
                elif v < 0:
                    style = (f"background:rgba(239,68,68,0.{intensity:02d});"
                             f"color:#ef4444;font-weight:bold")
                else:
                    style = ""
                return pct_str, style

            # +2%到達(5日) フラグ表示
            if "到達(5日)" in col:
                if v == 1:
                    return "✅ 到達", "background:rgba(16,185,129,0.2);color:#10b981;font-weight:bold"
                elif v == 0:
                    return "✗", "background:rgba(239,68,68,0.1);color:#ef4444"
                return "—", ""

            # 3/5日以内最大(%)
            if col in ["3日以内最大(%)", "5日以内最大(%)"]:
                pct_str = f"{v:+.2f}%"
                if v >= 2.0:
                    return pct_str, "background:rgba(16,185,129,0.25);color:#10b981;font-weight:bold"
                elif v > 0:
                    return pct_str, "background:rgba(16,185,129,0.10);color:#10b981"
                elif v < 0:
                    return pct_str, "background:rgba(239,68,68,0.15);color:#ef4444"
                return pct_str, ""

            # +3%到達日
            if col in ["+3%到達日", "到達日"]:
                d = int(v)
                if d == 1:
                    return f"🎯{d}日目", "background:rgba(234,179,8,0.25);color:#eab308;font-weight:bold"
                elif d == 2:
                    return f"✅{d}日目", "background:rgba(16,185,129,0.20);color:#10b981;font-weight:bold"
                elif d <= 3:
                    return f"✅{d}日目", "background:rgba(16,185,129,0.12);color:#10b981"
                else:
                    return f"{d}日目", "color:#94a3b8"

            # 回転スコア
            if col == "回転スコア":
                s = int(v)
                if s >= 70:
                    return f"⭐{s}", "background:rgba(234,179,8,0.20);color:#eab308;font-weight:bold"
                elif s >= 50:
                    return f"{s}", "background:rgba(16,185,129,0.15);color:#10b981;font-weight:bold"
                elif s >= 30:
                    return f"{s}", "color:#10b981"
                return f"{s}", "color:#94a3b8"

            # ATR%
            if col == "ATR%":
                s = f"{v:.1f}%"
                if v >= 4.0:
                    return s, "background:rgba(234,179,8,0.20);color:#eab308;font-weight:bold"
                elif v >= 3.0:
                    return s, "color:#10b981"
                return s, "color:#94a3b8"

            # RSI
            if col == "RSI(14)":
                s = f"{v:.1f}"
                if v >= 70:
                    return s, "background:rgba(239,68,68,0.25);color:#ef4444;font-weight:bold"
                elif v >= 60:
                    return s, "background:rgba(251,146,60,0.20);color:#f97316"
                elif v <= 30:
                    return s, "background:rgba(16,185,129,0.25);color:#10b981;font-weight:bold"
                elif v <= 40:
                    return s, "background:rgba(16,185,129,0.12);color:#10b981"
                return s, ""

            # 終値
            if col == "終値":
                return f"{v:,.1f}", ""

            # 推定売買可能額（万円/億円表記）
            if col == "推定売買可能額":
                if v >= 1e8:
                    return f"{v/1e8:.1f}億", "color:#6366f1;font-weight:bold"
                elif v >= 1e4:
                    return f"{v/1e4:.0f}万", "color:#6366f1"
                else:
                    return f"{v:,.0f}", "color:#94a3b8"

            # 出来高
            if col == "出来高":
                return f"{v:,.0f}", ""

            # 出来高比
            if col == "出来高比(20MA)":
                return f"{v:.2f}", ""

            if col == "AI予測(%)":
                s = f"{v:.0f}%"
                if v >= 80:
                    return s, "background:rgba(234,179,8,0.30);color:#eab308;font-weight:bold"
                elif v >= 70:
                    return s, "background:rgba(234,179,8,0.20);color:#eab308;font-weight:bold"
                elif v >= 60:
                    return s, "background:rgba(16,185,129,0.20);color:#10b981;font-weight:bold"
                elif v >= 50:
                    return s, "color:#10b981"
                elif v < 30:
                    return s, "background:rgba(239,68,68,0.10);color:#ef4444"
                return s, "color:#94a3b8"

            return str(val), ""

        # ---- カスタムHTMLテーブルを生成する ----
        # 表示列（銘柄コード・銘柄名・予測スコアは固定、残りを順番どおりに。内部変数_付きは除外）
        skip_cols = {"銘柄コード", "銘柄名", "予測スコア", "AI予測(%)", "推定売買可能額", "5日以内最大(%)", "3日以内最大(%)", "回転スコア"}
        # 固定表示列（先頭に配置）
        priority_cols = []
        for pc in ["推定売買可能額", "AI予測(%)"]:
            if pc in display_df.columns:
                priority_cols.append(pc)
        other_cols = priority_cols + [c for c in display_df.columns if c not in skip_cols and not c.startswith("_") and c not in priority_cols]

        # ヘッダー
        th_cells = "<th>銘柄コード</th><th>銘柄名</th>"
        for col in other_cols:
            th_cells += f"<th>{col}</th>"

        # 銘柄コード別スコア内訳データ（JS ツールチップ用）
        breakdown_data = {}
        rows_html = ""
        tv_base = "https://jp.tradingview.com/chart/M3vhlCeS/?symbol=TSE%3A"
        for _, row in display_df.iterrows():
            code = int(row.get("銘柄コード", 0))
            name = str(row.get("銘柄名", ""))
            tv_url = f"{tv_base}{code}"

            # 銘柄コードセル：クリックでTradingViewに遷移するリンク
            code_cell = (
                f'<td class="code-cell">'
                f'<a class="code-link" href="{tv_url}" target="_blank">{code}</a>'
                f'</td>'
            )

            # 内訳項目を計算する
            bd = []
            bd.append(["① SMA5>SMA20>SMA60", "+25"])
            if row.get("_weekly_sma20_ok"):
                bd.append(["② 週足SMA20維持", "+15"])
            try:
                if float(row.get("出来高比(20MA)", 0)) >= 1.10:
                    bd.append(["③ 出来高比≥ 1.10", "+20"])
            except (TypeError, ValueError):
                pass
            try:
                rsi = float(row.get("RSI(14)", 0))
                if 50.0 <= rsi <= 65.0:
                    bd.append(["④ RSI 50～65", "+15"])
            except (TypeError, ValueError):
                pass
            try:
                vty = float(row.get("_vol_today_vs_yday_pct") or 0)
                if 10.0 <= vty <= 30.0:
                    bd.append(["⑤ 出来高110～130%", "+10"])
            except (TypeError, ValueError):
                pass
            if row.get("_is_pullback"):
                bd.append(["⑥ 位置：押し目", "+10"])
            elif row.get("_is_breakout"):
                bd.append(["⑥ 位置：初動", "+10"])
            try:
                dow = int(row.get("_day_of_week", -1))
                if dow in (1, 2, 3):
                    bd.append(["⑦ 曜日補正(火～木)", "+5"])
            except (TypeError, ValueError):
                pass
            # 減点
            try:
                if float(row.get("RSI(14)", 0)) > 70.0:
                    bd.append(["RSI70超", "-10"])
            except (TypeError, ValueError):
                pass
            if row.get("_long_upper_wick"):
                bd.append(["長大上ヒゲ", "-15"])
            if row.get("_is_high_zone"):
                bd.append(["高値圈終盤", "-15"])
            if row.get("_big_bearish_yesterday"):
                bd.append(["大陰線直後", "-20"])
            breakdown_data[str(code)] = bd

            name_cell = f'<td class="name-cell">{name}</td>'

            other_cells = ""
            for col in other_cols:
                val = row.get(col)
                text, style = _fmt_val(col, val)
                style_attr = f' style="{style}"' if style else ""
                other_cells += f"<td{style_attr}>{text}</td>"

            rows_html += f"<tr>{code_cell}{name_cell}{other_cells}</tr>\n"

        # 凡例
        st.caption(
            "📌 **日次リターン（%）= 終値ベース**（前日終値→当日終値の騰落率）　"
            "| **到達判定（○/✕）・到達日 = 高値ベース**（日中の高値が+3%指値に到達したか）"
        )

        n_rows = len(display_df)
        table_height = min(max(n_rows * 42 + 60, 200), 600)
        bd_json = _json.dumps(breakdown_data, ensure_ascii=False)

        _table_html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,'Noto Sans JP',sans-serif;background:transparent;overflow-x:auto}}
table{{border-collapse:collapse;width:100%;font-size:12px}}
th{{
  position:sticky;top:0;z-index:10;
  background:#1a2332;color:#8899aa;font-weight:600;
  padding:8px 10px;text-align:right;border-bottom:1px solid #2a3a4e;
  white-space:nowrap
}}
th:first-child,th:nth-child(2){{text-align:left}}
td{{
  padding:7px 10px;text-align:right;border-bottom:1px solid rgba(255,255,255,0.04);
  color:#111;white-space:nowrap
}}
td.name-cell{{text-align:left;color:#111;max-width:160px;overflow:hidden;text-overflow:ellipsis}}
td.code-cell{{text-align:left}}
tr:hover td{{background:rgba(255,255,255,0.03)}}
.code-link{{
  display:inline-block;
  color:#60a5fa;font-weight:700;text-decoration:none;
  background:rgba(59,130,246,0.1);
  border:1px solid rgba(59,130,246,0.4);
  border-radius:12px;padding:2px 10px;
  transition:background .15s,border-color .15s
}}
.code-link:hover{{background:rgba(59,130,246,0.25);border-color:#60a5fa}}
.score-badge{{
  display:inline-block;font-size:10px;font-weight:700;
  border-radius:8px;padding:1px 7px;margin-left:4px;
  vertical-align:middle;white-space:nowrap
}}
.badge-high{{background:rgba(251,191,36,0.22);color:#d97706;border:1px solid rgba(251,191,36,0.5)}}
.badge-mid{{background:rgba(139,92,246,0.18);color:#7c3aed;border:1px solid rgba(139,92,246,0.4)}}
.badge-low{{background:rgba(100,116,139,0.15);color:#64748b;border:1px solid rgba(100,116,139,0.3)}}
/* ---- スコア内訳ツールチップ ---- */
#score-tip{{
  display:none;position:fixed;z-index:9999;
  background:#1a2332;border:1px solid #2a3a4e;border-radius:10px;
  box-shadow:0 12px 40px rgba(0,0,0,0.8);
  padding:10px 14px;min-width:190px;pointer-events:none
}}
.st-title{{font-size:11px;font-weight:800;color:#8899aa;margin-bottom:7px;letter-spacing:.5px}}
.st-row{{display:flex;justify-content:space-between;align-items:center;
  padding:2px 0;font-size:12px}}
.st-label{{color:#d4dae3}}
.st-plus{{color:#10b981;font-weight:700;}}
.st-minus{{color:#ef4444;font-weight:700;}}
.st-total{{border-top:1px solid #2a3a4e;margin-top:6px;padding-top:5px;
  font-weight:800;font-size:13px;color:#60a5fa}}
.tbl-wrap{{height:{table_height}px;overflow-y:auto;overflow-x:auto}}
</style>
</head>
<body>
<div class="tbl-wrap">
  <table>
    <thead><tr>{th_cells}</tr></thead>
    <tbody>{rows_html}</tbody>
  </table>
</div>
<div id="score-tip"><div class="st-title">スコア内訳</div><div id="st-body"></div></div>
<script>
(function(){{
  // ---- ヘッダークリックで昼降順ソートする ----
  const table = document.querySelector('table');
  const tbody = table.querySelector('tbody');
  const ths   = table.querySelectorAll('th');
  let sortColIdx = -1;
  let sortAsc    = true;

  ths.forEach(function(th, i){{
    th.style.cursor = 'pointer';
    th.style.userSelect = 'none';
    th.addEventListener('click', function(){{
      if(sortColIdx === i){{
        sortAsc = !sortAsc;
      }} else {{
        sortColIdx = i;
        sortAsc = true;
      }}
      // アイコン更新
      ths.forEach(function(t){{ t.dataset.sort = ''; }});
      th.dataset.sort = sortAsc ? 'asc' : 'desc';

      // 行を配列に取り出してソート
      const rows = Array.from(tbody.querySelectorAll('tr'));
      rows.sort(function(a, b){{
        const aCell = a.cells[i];
        const bCell = b.cells[i];
        const aRaw  = aCell ? (aCell.innerText || aCell.textContent).trim() : '';
        const bRaw  = bCell ? (bCell.innerText || bCell.textContent).trim() : '';

        // 数値（%やカンマを除去してパース）
        const aNum = parseFloat(aRaw.replace(/[,%+pt—]/g,''));
        const bNum = parseFloat(bRaw.replace(/[,%+pt—]/g,''));
        const bothNum = !isNaN(aNum) && !isNaN(bNum);

        // 「—」は常に最後尾
        if(aRaw==='—' && bRaw!=='—') return 1;
        if(bRaw==='—' && aRaw!=='—') return -1;

        let cmp = bothNum ? (aNum - bNum) : aRaw.localeCompare(bRaw,'ja');
        return sortAsc ? cmp : -cmp;
      }});
      rows.forEach(function(r){{ tbody.appendChild(r); }});
    }});
  }});

  // CSS: ソートインジケーター
  const style = document.createElement('style');
  style.textContent = [
    'th[data-sort="asc"]::after{{content:" ▲";font-size:9px;color:#60a5fa}}',
    'th[data-sort="desc"]::after{{content:" ▼";font-size:9px;color:#60a5fa}}',
    'th:hover{{background:#1e2d42!important}}',
  ].join('');
  document.head.appendChild(style);

  // ---- スコアバッジホバーで内訳ポップアップ ----
  const BD   = {bd_json};
  const tip  = document.getElementById('score-tip');
  const body = document.getElementById('st-body');
  let hideT  = null;

  document.addEventListener('mouseover', function(e){{
    const badge = e.target.closest('.score-badge');
    if(!badge) return;
    clearTimeout(hideT);
    const code = badge.dataset.code;
    const items = BD[code];
    if(!items) return;

    // 内訳 HTML を生成する
    let total = 0;
    let html  = '';
    items.forEach(function(it){{
      const pts = parseInt(it[1]);
      total += pts;
      const cls = pts > 0 ? 'st-plus' : 'st-minus';
      html += '<div class="st-row"><span class="st-label">' + it[0] +
              '</span><span class="' + cls + '">' + it[1] + 'pt</span></div>';
    }});
    html += '<div class="st-row st-total"><span class="st-label">合計</span>' +
            '<span>' + (total >= 0 ? '+' : '') + total + 'pt</span></div>';
    body.innerHTML = html;

    // 位置調整（画面端に収める）
    const W = window.innerWidth, H = window.innerHeight;
    const tw = 200, th2 = items.length * 24 + 80;
    let lx = e.clientX + 14, ty = e.clientY + 14;
    if(lx + tw > W - 8) lx = e.clientX - tw - 14;
    if(ty + th2 > H - 8) ty = e.clientY - th2 - 14;
    tip.style.left = lx + 'px';
    tip.style.top  = ty + 'px';
    tip.style.display = 'block';
  }});
  document.addEventListener('mouseout', function(e){{
    if(!e.target.closest('.score-badge')) return;
    hideT = setTimeout(function(){{ tip.style.display='none'; }}, 200);
  }});
}})();
</script>
</body>
</html>"""

        st.components.v1.html(_table_html, height=table_height + 4, scrolling=False)






        # ---- CSVダウンロード ----
        st.markdown("---")
        col_dl1, col_dl2 = st.columns([1, 4])
        with col_dl1:
            csv_data = display_df.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                label="⬇️ CSV ダウンロード",
                data=csv_data,
                file_name=f"screening_{as_of_str.replace('-','')}.csv",
                mime="text/csv; charset=utf-8-sig",
            )
        with col_dl2:
            st.caption("※ 現在の絞り込み結果がダウンロードされます")


else:
    # ---- 未実行の案内 ----
    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("""
        ### 📋 使い方
        1. **左サイドバーで日付を選択**
        2. 必要に応じて条件を調整
        3. **「スクリーニング実行」ボタンを押す**

        結果として以下が表示されます：
        - SMA5>SMA20>SMA60 かつ 出来高≥50万 の銘柄一覧
        - 明日・明後日・4日後・5日後の騰落率
        - +2%達成フラグ・分布グラフ
        """)
    with col2:
        st.markdown("""
        ### ⚡ クイックスタート
        **実行時間の目安：** 1〜3分（TradingView APIで候補を絞り込んでからデータ取得するため高速です。2回目以降はキャッシュで即時表示。）

        **ヒント：** 同日・同条件の再実行はキャッシュが効き即時表示。
        """)

    st.markdown("---")
    st.caption("⚠️ 本ツールは投資助言を目的としたものではありません。")
