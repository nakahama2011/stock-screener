"""
レベル1：HTMLレポート生成スクリプト

スクリーニング結果のJSONを読み込み、
リッチなHTMLダッシュボードを生成する。
"""

import json
import os
from datetime import datetime


# =========================================================
# 定数
# =========================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(SCRIPT_DIR, "results")
LATEST_JSON = os.path.join(RESULTS_DIR, "latest.json")
OUTPUT_HTML = os.path.join(RESULTS_DIR, "report.html")
OUTPUT_ADMIN = os.path.join(RESULTS_DIR, "admin.html")


# =========================================================
# HTML テンプレート
# =========================================================
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>株スクリーニング結果 | レベル1</title>
  <!-- レベル2ギャラリーへの相対パス -->
  <script>const GALLERY_PATH = '../snapshots/gallery.html';</script>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Noto+Sans+JP:wght@300;400;500;700&display=swap" rel="stylesheet">
  <style>
    :root {{
      --bg-primary: #0a0e17;
      --bg-secondary: #111827;
      --bg-card: #1a2332;
      --bg-card-hover: #1f2b3d;
      --border-color: #2a3a4e;
      --text-primary: #e8edf5;
      --text-secondary: #8899aa;
      --text-muted: #5a6a7a;
      --accent-blue: #3b82f6;
      --accent-cyan: #06b6d4;
      --accent-green: #10b981;
      --accent-yellow: #f59e0b;
      --accent-red: #ef4444;
      --gradient-primary: linear-gradient(135deg, #3b82f6, #06b6d4);
      --gradient-card: linear-gradient(135deg, rgba(59,130,246,0.08), rgba(6,182,212,0.04));
      --shadow-glow: 0 0 30px rgba(59,130,246,0.15);
    }}

    * {{
      margin: 0;
      padding: 0;
      box-sizing: border-box;
    }}

    body {{
      font-family: 'Inter', 'Noto Sans JP', -apple-system, sans-serif;
      background: var(--bg-primary);
      color: var(--text-primary);
      min-height: 100vh;
      line-height: 1.6;
    }}

    /* ヘッダー */
    .header {{
      background: var(--bg-secondary);
      border-bottom: 1px solid var(--border-color);
      padding: 1.5rem 2rem;
      position: sticky;
      top: 0;
      z-index: 100;
      backdrop-filter: blur(12px);
    }}

    .header-inner {{
      max-width: 1400px;
      margin: 0 auto;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }}

    .header h1 {{
      font-size: 1.5rem;
      font-weight: 700;
      background: var(--gradient-primary);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
      letter-spacing: -0.02em;
    }}

    .header-meta {{
      display: flex;
      gap: 1.5rem;
      align-items: center;
    }}

    .header-meta span {{
      color: var(--text-secondary);
      font-size: 0.85rem;
    }}

    .header-meta .value {{
      color: var(--text-primary);
      font-weight: 600;
    }}

    /* メインコンテンツ */
    .main {{
      max-width: 1400px;
      margin: 0 auto;
      padding: 2rem;
    }}

    /* 統計カード */
    .stats-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 1rem;
      margin-bottom: 2rem;
    }}

    .stat-card {{
      background: var(--bg-card);
      border: 1px solid var(--border-color);
      border-radius: 12px;
      padding: 1.25rem 1.5rem;
      transition: all 0.3s ease;
      position: relative;
      overflow: hidden;
    }}

    .stat-card::before {{
      content: '';
      position: absolute;
      top: 0;
      left: 0;
      right: 0;
      height: 3px;
      background: var(--gradient-primary);
      opacity: 0;
      transition: opacity 0.3s ease;
    }}

    .stat-card:hover {{
      background: var(--bg-card-hover);
      box-shadow: var(--shadow-glow);
      transform: translateY(-2px);
    }}

    .stat-card:hover::before {{
      opacity: 1;
    }}

    .stat-label {{
      color: var(--text-secondary);
      font-size: 0.8rem;
      font-weight: 500;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      margin-bottom: 0.5rem;
    }}

    .stat-value {{
      font-size: 1.8rem;
      font-weight: 700;
      background: var(--gradient-primary);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
    }}

    /* 条件バッジ */
    .conditions {{
      display: flex;
      gap: 0.75rem;
      margin-bottom: 2rem;
      flex-wrap: wrap;
    }}

    .condition-badge {{
      background: var(--bg-card);
      border: 1px solid var(--border-color);
      border-radius: 8px;
      padding: 0.6rem 1rem;
      font-size: 0.85rem;
      color: var(--text-secondary);
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }}

    .condition-badge .icon {{
      font-size: 1rem;
    }}

    /* テーブル */
    .table-container {{
      background: var(--bg-card);
      border: 1px solid var(--border-color);
      border-radius: 12px;
      overflow: hidden;
    }}

    .table-header {{
      padding: 1rem 1.5rem;
      border-bottom: 1px solid var(--border-color);
      display: flex;
      justify-content: space-between;
      align-items: center;
    }}

    .table-header h2 {{
      font-size: 1.1rem;
      font-weight: 600;
      color: var(--text-primary);
    }}

    .search-box {{
      background: var(--bg-secondary);
      border: 1px solid var(--border-color);
      border-radius: 8px;
      padding: 0.5rem 1rem;
      color: var(--text-primary);
      font-size: 0.85rem;
      width: 250px;
      outline: none;
      transition: border-color 0.3s ease;
    }}

    .search-box:focus {{
      border-color: var(--accent-blue);
    }}

    .search-box::placeholder {{
      color: var(--text-muted);
    }}

    table {{
      width: 100%;
      border-collapse: collapse;
    }}

    thead th {{
      background: var(--bg-secondary);
      padding: 0.85rem 1rem;
      text-align: left;
      font-size: 0.78rem;
      font-weight: 600;
      color: var(--text-secondary);
      text-transform: uppercase;
      letter-spacing: 0.05em;
      border-bottom: 1px solid var(--border-color);
      cursor: pointer;
      user-select: none;
      position: relative;
      white-space: nowrap;
    }}

    thead th:hover {{
      color: var(--accent-blue);
    }}

    thead th .sort-icon {{
      margin-left: 0.3rem;
      opacity: 0.3;
      transition: opacity 0.2s;
    }}

    thead th.sorted .sort-icon {{
      opacity: 1;
      color: var(--accent-blue);
    }}

    tbody tr {{
      border-bottom: 1px solid rgba(42, 58, 78, 0.5);
      transition: background 0.2s ease;
    }}

    tbody tr:hover {{
      background: var(--bg-card-hover);
    }}

    tbody td {{
      padding: 0.85rem 1rem;
      font-size: 0.9rem;
      color: var(--text-primary);
    }}

    .ticker-code {{
      font-weight: 700;
      color: var(--accent-blue);
      font-family: 'Inter', monospace;
    }}

    .ticker-name {{
      color: var(--text-secondary);
      font-size: 0.82rem;
      margin-top: 0.15rem;
    }}

    .num-cell {{
      text-align: right;
      font-family: 'Inter', monospace;
      font-weight: 500;
    }}

    .volume-bar {{
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }}

    .volume-bar .bar {{
      flex: 1;
      height: 4px;
      background: var(--bg-secondary);
      border-radius: 2px;
      overflow: hidden;
      min-width: 40px;
    }}

    .volume-bar .bar-fill {{
      height: 100%;
      border-radius: 2px;
      background: var(--gradient-primary);
      transition: width 0.5s ease;
    }}

    /* ソートアニメーション */
    tbody tr {{
      animation: fadeIn 0.3s ease;
    }}

    @keyframes fadeIn {{
      from {{ opacity: 0; transform: translateY(4px); }}
      to {{ opacity: 1; transform: translateY(0); }}
    }}

    /* 空状態 */
    .empty-state {{
      text-align: center;
      padding: 4rem 2rem;
      color: var(--text-secondary);
    }}

    .empty-state .icon {{
      font-size: 3rem;
      margin-bottom: 1rem;
    }}

    /* レベル間ナビゲーションボタン */
    .nav-level-btn {{
      display: inline-flex;
      align-items: center;
      gap: 0.4rem;
      padding: 0.5rem 1rem;
      background: var(--gradient-primary);
      color: #fff;
      text-decoration: none;
      border-radius: 8px;
      font-size: 0.85rem;
      font-weight: 600;
      cursor: pointer;
      border: none;
      transition: all 0.2s ease;
      white-space: nowrap;
    }}

    .nav-level-btn:hover {{
      opacity: 0.85;
      transform: translateY(-1px);
      box-shadow: 0 4px 16px rgba(59,130,246,0.35);
    }}

    /* チャートリンクセル */
    .chart-link-btn {{
      display: inline-flex;
      align-items: center;
      gap: 0.3rem;
      padding: 0.3rem 0.7rem;
      background: rgba(59,130,246,0.12);
      border: 1px solid rgba(59,130,246,0.3);
      color: var(--accent-blue);
      border-radius: 6px;
      font-size: 0.78rem;
      font-weight: 600;
      cursor: pointer;
      text-decoration: none;
      transition: all 0.2s ease;
    }}

    .chart-link-btn:hover {{
      background: rgba(59,130,246,0.25);
      border-color: var(--accent-blue);
    }}

    /* フッター */
    .footer {{
      text-align: center;
      padding: 2rem;
      color: var(--text-muted);
      font-size: 0.8rem;
    }}

    /* レスポンシブ */
    @media (max-width: 768px) {{
      .header-inner {{
        flex-direction: column;
        gap: 0.75rem;
        align-items: flex-start;
      }}

      .main {{
        padding: 1rem;
      }}

      .stats-grid {{
        grid-template-columns: repeat(2, 1fr);
      }}

      .search-box {{
        width: 100%;
      }}

      .table-header {{
        flex-direction: column;
        gap: 0.75rem;
        align-items: stretch;
      }}
    }}
  </style>
</head>
<body>

  <!-- ヘッダー -->
  <header class="header">
    <div class="header-inner">
      <h1>📊 株スクリーニング結果</h1>
      <div class="header-meta">
        <span>実行日時: <span class="value">{generated_at}</span></span>
        <span style="background: rgba(59,130,246,0.15); border: 1px solid rgba(59,130,246,0.3); padding: 0.2rem 0.6rem; border-radius: 6px; color: var(--accent-blue); font-size: 0.8rem; font-weight: 700;">LEVEL 1</span>
        <button class="nav-level-btn" onclick="openGallery(null)">
          📸 チャートギャラリー（レベル2）を開く
        </button>
      </div>
    </div>
  </header>

  <!-- メイン -->
  <main class="main">

    <!-- 統計 -->
    <div class="stats-grid">
      <div class="stat-card">
        <div class="stat-label">候補銘柄数</div>
        <div class="stat-value">{total_candidates}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">スクリーニング条件</div>
        <div class="stat-value" style="font-size: 1rem;">レベル1</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">基準日</div>
        <div class="stat-value" style="font-size: 1.2rem;">{base_date}</div>
      </div>
    </div>

    <!-- 条件 -->
    <div class="conditions">
      <div class="condition-badge">
        <span class="icon">📈</span>
        SMA5 &gt; SMA20 &gt; SMA60（順行配列）
      </div>
      <div class="condition-badge">
        <span class="icon">📊</span>
        出来高 ≥ {min_volume}株
      </div>
    </div>

    <!-- テーブル -->
    <div class="table-container">
      <div class="table-header">
        <h2>候補銘柄一覧</h2>
        <input type="text" class="search-box" placeholder="🔍 銘柄コードまたは名前で検索..." id="searchInput">
      </div>

      {table_content}
    </div>
  </main>

  <!-- フッター -->
  <footer class="footer">
    <p>⚠️ 本ツールは投資助言を目的としたものではありません。投資判断は自己責任でお願いいたします。</p>
    <p style="margin-top: 0.5rem;">Generated at {generated_at}</p>
  </footer>

  <script>
    // レベル2（ギャラリー）を開く関数
    // code: 銘柄コード（nullの場合はトップ表示）
    function openGallery(code) {{
      const url = code
        ? GALLERY_PATH + '#ticker=' + code
        : GALLERY_PATH;
      window.open(url, '_blank');
    }}

    // 検索機能
    document.getElementById('searchInput').addEventListener('input', function(e) {{
      const query = e.target.value.toLowerCase();
      const rows = document.querySelectorAll('tbody tr');
      rows.forEach(row => {{
        const text = row.textContent.toLowerCase();
        row.style.display = text.includes(query) ? '' : 'none';
      }});
    }});

    // ソート機能
    let sortState = {{}};

    document.querySelectorAll('thead th[data-sort]').forEach(th => {{
      th.addEventListener('click', function() {{
        const key = this.dataset.sort;
        const type = this.dataset.type || 'string';
        const tbody = document.querySelector('tbody');
        const rows = Array.from(tbody.querySelectorAll('tr'));

        // ソート方向の切り替え
        sortState[key] = sortState[key] === 'asc' ? 'desc' : 'asc';
        const dir = sortState[key];

        // 他のカラムのソートアイコンをリセット
        document.querySelectorAll('thead th').forEach(h => h.classList.remove('sorted'));
        this.classList.add('sorted');
        this.querySelector('.sort-icon').textContent = dir === 'asc' ? '▲' : '▼';

        rows.sort((a, b) => {{
          let va = a.querySelector(`td[data-key="${{key}}"]`)?.dataset.value || '';
          let vb = b.querySelector(`td[data-key="${{key}}"]`)?.dataset.value || '';

          if (type === 'number') {{
            va = parseFloat(va) || 0;
            vb = parseFloat(vb) || 0;
          }}

          if (va < vb) return dir === 'asc' ? -1 : 1;
          if (va > vb) return dir === 'asc' ? 1 : -1;
          return 0;
        }});

        rows.forEach(row => tbody.appendChild(row));
      }});
    }});
  </script>

</body>
</html>"""


# =========================================================
# HTMLテーブル生成
# =========================================================
def _build_table(candidates: list) -> str:
    """
    候補銘柄リストからHTMLのテーブルを組み立てる。

    Args:
        candidates: 候補銘柄の辞書リスト

    Returns:
        str: HTML テーブル文字列
    """
    if not candidates:
        return """
        <div class="empty-state">
          <div class="icon">🔍</div>
          <h3>候補銘柄が見つかりませんでした</h3>
          <p>条件に合致する銘柄がありません。条件を見直してください。</p>
        </div>
        """

    # 出来高の最大値（バーの表示用）
    max_volume = max(c["volume"] for c in candidates)

    rows = []
    for i, c in enumerate(candidates, 1):
        volume_pct = (c["volume"] / max_volume * 100) if max_volume > 0 else 0
        volume_display = f"{c['volume']:,.0f}"

        row = f"""
        <tr>
          <td style="text-align: center; color: var(--text-muted);" data-key="rank" data-value="{i}">{i}</td>
          <td data-key="code" data-value="{c['code']}">
            <div class="ticker-code">{c['code']}</div>
            <div class="ticker-name">{c.get('name', '')}</div>
          </td>
          <td class="num-cell" data-key="close" data-value="{c['close']}">{c['close']:,.1f}</td>
          <td class="num-cell" data-key="sma5" data-value="{c['sma5']}">{c['sma5']:,.1f}</td>
          <td class="num-cell" data-key="sma20" data-value="{c['sma20']}">{c['sma20']:,.1f}</td>
          <td class="num-cell" data-key="sma60" data-value="{c['sma60']}">{c['sma60']:,.1f}</td>
          <td data-key="volume" data-value="{c['volume']}">
            <div class="volume-bar">
              <span class="num-cell">{volume_display}</span>
              <div class="bar"><div class="bar-fill" style="width: {volume_pct:.0f}%"></div></div>
            </div>
          </td>
          <td class="num-cell" data-key="volume_ratio" data-value="{c['volume_ratio']}">{c['volume_ratio']:.2f}x</td>
          <td style="text-align: center;">
            <button class="chart-link-btn" onclick="openGallery('{c['code']}')" title="{c['code']} のチャートを見る">
              📸 チャート
            </button>
          </td>
        </tr>"""
        rows.append(row)

    table = f"""
      <table>
        <thead>
          <tr>
            <th data-sort="rank" data-type="number" style="width: 50px; text-align: center;"># <span class="sort-icon">⇅</span></th>
            <th data-sort="code" data-type="number">銘柄 <span class="sort-icon">⇅</span></th>
            <th data-sort="close" data-type="number" style="text-align: right;">終値 <span class="sort-icon">⇅</span></th>
            <th data-sort="sma5" data-type="number" style="text-align: right;">SMA5 <span class="sort-icon">⇅</span></th>
            <th data-sort="sma20" data-type="number" style="text-align: right;">SMA20 <span class="sort-icon">⇅</span></th>
            <th data-sort="sma60" data-type="number" style="text-align: right;">SMA60 <span class="sort-icon">⇅</span></th>
            <th data-sort="volume" data-type="number">出来高 <span class="sort-icon">⇅</span></th>
            <th data-sort="volume_ratio" data-type="number" style="text-align: right;">出来高比 <span class="sort-icon">⇅</span></th>
            <th style="width: 90px; text-align: center;">チャート</th>
          </tr>
        </thead>
        <tbody>
          {''.join(rows)}
        </tbody>
      </table>"""

    return table


# =========================================================
# レポート生成
# =========================================================
def generate_report(json_path: str = None) -> str:
    """
    スクリーニング結果のJSONからHTMLレポートを生成する。

    Args:
        json_path: JSONファイルのパス（省略時はlatest.jsonを使用）

    Returns:
        str: 生成されたHTMLファイルのパス
    """
    if json_path is None:
        json_path = LATEST_JSON

    if not os.path.exists(json_path):
        print(f"❌ 結果ファイルが見つかりません: {json_path}")
        print("   先に screener.py を実行してください。")
        return ""

    # JSONの読み込み
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    candidates = data.get("candidates", [])
    generated_at = data.get("generated_at", "不明")
    conditions = data.get("conditions", {})
    total_candidates = data.get("total_candidates", len(candidates))
    min_volume = f"{conditions.get('min_volume', 500000):,}"

    # 基準日（候補銘柄の最初のもののdateを使用）
    base_date = candidates[0]["date"] if candidates else "N/A"

    # テーブル組み立て
    table_content = _build_table(candidates)

    # HTMLの生成
    html = HTML_TEMPLATE.format(
        generated_at=generated_at,
        total_candidates=total_candidates,
        base_date=base_date,
        min_volume=min_volume,
        table_content=table_content,
    )

    # ファイルへ書き出し
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"✅ HTMLレポートを生成しました: {OUTPUT_HTML}")
    return OUTPUT_HTML


# =========================================================
# 管理ダッシュボードHTMLテンプレート
# =========================================================
ADMIN_TEMPLATE = """<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>レベル1 管理ダッシュボード</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Noto+Sans+JP:wght@300;400;500;700&display=swap" rel="stylesheet">
  <style>
    :root {{
      --bg-primary: #0a0e17;
      --bg-secondary: #111827;
      --bg-card: #1a2332;
      --bg-card-hover: #1f2b3d;
      --border-color: #2a3a4e;
      --text-primary: #e8edf5;
      --text-secondary: #8899aa;
      --text-muted: #5a6a7a;
      --accent-blue: #3b82f6;
      --accent-cyan: #06b6d4;
      --accent-green: #10b981;
      --accent-yellow: #f59e0b;
      --accent-red: #ef4444;
      --gradient-primary: linear-gradient(135deg, #3b82f6, #06b6d4);
    }}
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{
      font-family: 'Inter', 'Noto Sans JP', -apple-system, sans-serif;
      background: var(--bg-primary);
      color: var(--text-primary);
      min-height: 100vh;
      line-height: 1.6;
    }}
    /* ヘッダー */
    .header {{
      background: var(--bg-secondary);
      border-bottom: 2px solid var(--border-color);
      padding: 1.2rem 2rem;
      position: sticky;
      top: 0;
      z-index: 100;
      backdrop-filter: blur(12px);
    }}
    .header-inner {{
      max-width: 1400px;
      margin: 0 auto;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 1rem;
    }}
    .header-left {{ display: flex; align-items: center; gap: 1rem; }}
    .header h1 {{
      font-size: 1.4rem;
      font-weight: 700;
      background: var(--gradient-primary);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
      letter-spacing: -0.02em;
    }}
    .badge {{
      background: rgba(59,130,246,0.15);
      border: 1px solid rgba(59,130,246,0.35);
      color: var(--accent-blue);
      padding: 0.2rem 0.65rem;
      border-radius: 6px;
      font-size: 0.75rem;
      font-weight: 700;
      letter-spacing: 0.05em;
    }}
    .header-meta {{ color: var(--text-secondary); font-size: 0.82rem; }}
    .header-meta strong {{ color: var(--text-primary); }}
    /* メイン */
    .main {{ max-width: 1400px; margin: 0 auto; padding: 2rem; }}
    /* セクションタイトル */
    .section-title {{
      font-size: 0.75rem;
      font-weight: 700;
      color: var(--text-secondary);
      text-transform: uppercase;
      letter-spacing: 0.1em;
      margin-bottom: 0.9rem;
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }}
    .section-title::after {{
      content: '';
      flex: 1;
      height: 1px;
      background: var(--border-color);
    }}
    /* KPIカード */
    .kpi-grid {{
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 1rem;
      margin-bottom: 2rem;
    }}
    @media (max-width: 900px) {{ .kpi-grid {{ grid-template-columns: repeat(2, 1fr); }} }}
    .kpi-card {{
      background: var(--bg-card);
      border: 1px solid var(--border-color);
      border-radius: 14px;
      padding: 1.4rem 1.5rem;
      position: relative;
      overflow: hidden;
      transition: all 0.3s ease;
      cursor: default;
    }}
    .kpi-card::before {{
      content: '';
      position: absolute;
      top: 0; left: 0; right: 0;
      height: 3px;
      background: var(--gradient-primary);
      opacity: 0;
      transition: opacity 0.3s;
    }}
    .kpi-card:hover {{
      background: var(--bg-card-hover);
      transform: translateY(-2px);
      box-shadow: 0 8px 30px rgba(59,130,246,0.12);
    }}
    .kpi-card:hover::before {{ opacity: 1; }}
    .kpi-icon {{ font-size: 1.5rem; margin-bottom: 0.5rem; }}
    .kpi-label {{
      font-size: 0.75rem;
      font-weight: 600;
      color: var(--text-secondary);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 0.4rem;
    }}
    .kpi-value {{
      font-size: 2rem;
      font-weight: 700;
      background: var(--gradient-primary);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
      line-height: 1.1;
    }}
    .kpi-sub {{ font-size: 0.8rem; color: var(--text-muted); margin-top: 0.3rem; }}
    /* クイックアクション */
    .actions-row {{
      display: flex;
      gap: 0.75rem;
      margin-bottom: 2rem;
      flex-wrap: wrap;
    }}
    .action-btn {{
      display: inline-flex;
      align-items: center;
      gap: 0.45rem;
      padding: 0.65rem 1.2rem;
      border-radius: 10px;
      font-size: 0.88rem;
      font-weight: 600;
      cursor: pointer;
      border: none;
      text-decoration: none;
      transition: all 0.2s ease;
      white-space: nowrap;
    }}
    .action-btn-primary {{
      background: var(--gradient-primary);
      color: #fff;
    }}
    .action-btn-primary:hover {{
      opacity: 0.88;
      transform: translateY(-1px);
      box-shadow: 0 6px 20px rgba(59,130,246,0.35);
    }}
    .action-btn-secondary {{
      background: var(--bg-card);
      border: 1px solid var(--border-color);
      color: var(--text-primary);
    }}
    .action-btn-secondary:hover {{
      border-color: var(--accent-blue);
      background: var(--bg-card-hover);
      transform: translateY(-1px);
    }}
    /* TOP10グラフ */
    .chart-section {{ margin-bottom: 2rem; }}
    .chart-wrap {{
      background: var(--bg-card);
      border: 1px solid var(--border-color);
      border-radius: 14px;
      padding: 1.5rem;
    }}
    .bar-row {{
      display: flex;
      align-items: center;
      gap: 1rem;
      margin-bottom: 0.65rem;
      animation: fadeIn 0.4s ease both;
    }}
    .bar-label {{
      width: 130px;
      font-size: 0.8rem;
      color: var(--text-secondary);
      text-align: right;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      flex-shrink: 0;
    }}
    .bar-label a {{
      color: var(--accent-blue);
      text-decoration: none;
      font-weight: 600;
    }}
    .bar-label a:hover {{ text-decoration: underline; }}
    .bar-track {{
      flex: 1;
      height: 20px;
      background: rgba(255,255,255,0.04);
      border-radius: 4px;
      overflow: hidden;
    }}
    .bar-fill {{
      height: 100%;
      background: var(--gradient-primary);
      border-radius: 4px;
      transition: width 0.8s ease;
    }}
    .bar-val {{
      font-size: 0.78rem;
      color: var(--text-secondary);
      width: 80px;
      flex-shrink: 0;
      font-variant-numeric: tabular-nums;
    }}
    /* テーブル */
    .table-section {{ margin-bottom: 2rem; }}
    .table-container {{
      background: var(--bg-card);
      border: 1px solid var(--border-color);
      border-radius: 14px;
      overflow: hidden;
    }}
    .table-toolbar {{
      padding: 1rem 1.5rem;
      border-bottom: 1px solid var(--border-color);
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 1rem;
      flex-wrap: wrap;
    }}
    .table-toolbar h2 {{
      font-size: 1rem;
      font-weight: 600;
      color: var(--text-primary);
    }}
    .search-box {{
      background: var(--bg-secondary);
      border: 1px solid var(--border-color);
      border-radius: 8px;
      padding: 0.45rem 1rem;
      color: var(--text-primary);
      font-size: 0.85rem;
      width: 220px;
      outline: none;
      transition: border-color 0.2s;
    }}
    .search-box:focus {{ border-color: var(--accent-blue); }}
    .search-box::placeholder {{ color: var(--text-muted); }}
    table {{ width: 100%; border-collapse: collapse; }}
    thead th {{
      background: var(--bg-secondary);
      padding: 0.8rem 1rem;
      text-align: left;
      font-size: 0.75rem;
      font-weight: 700;
      color: var(--text-secondary);
      text-transform: uppercase;
      letter-spacing: 0.05em;
      border-bottom: 1px solid var(--border-color);
      cursor: pointer;
      user-select: none;
      white-space: nowrap;
    }}
    thead th:hover {{ color: var(--accent-blue); }}
    thead th.sorted {{ color: var(--accent-cyan); }}
    tbody tr {{
      border-bottom: 1px solid rgba(42,58,78,0.5);
      transition: background 0.15s ease;
      animation: fadeIn 0.3s ease;
    }}
    @keyframes fadeIn {{
      from {{ opacity: 0; transform: translateY(4px); }}
      to   {{ opacity: 1; transform: translateY(0); }}
    }}
    tbody tr:hover {{ background: var(--bg-card-hover); }}
    tbody td {{ padding: 0.8rem 1rem; font-size: 0.88rem; color: var(--text-primary); }}
    .code-link {{
      color: var(--accent-blue);
      font-weight: 700;
      text-decoration: none;
      background: rgba(59,130,246,0.1);
      border: 1px solid rgba(59,130,246,0.3);
      border-radius: 10px;
      padding: 2px 9px;
      transition: background .15s;
    }}
    .code-link:hover {{ background: rgba(59,130,246,0.22); }}
    .ticker-name {{ font-size: 0.8rem; color: var(--text-secondary); margin-top: 2px; }}
    .num-cell {{ text-align: right; font-variant-numeric: tabular-nums; }}
    .vol-bar-wrap {{ display: flex; align-items: center; gap: 0.5rem; }}
    .vol-bar {{ flex: 1; height: 4px; background: var(--bg-secondary); border-radius: 2px; min-width: 40px; overflow: hidden; }}
    .vol-bar-fill {{ height:100%; background: var(--gradient-primary); border-radius:2px; }}
    /* フッター */
    .footer {{
      text-align: center;
      padding: 2rem;
      color: var(--text-muted);
      font-size: 0.78rem;
      border-top: 1px solid var(--border-color);
    }}
  </style>
</head>
<body>

  <!-- ヘッダー -->
  <header class="header">
    <div class="header-inner">
      <div class="header-left">
        <h1>📊 管理ダッシュボード</h1>
        <span class="badge">LEVEL 1</span>
      </div>
      <div class="header-meta">
        最終実行: <strong>{generated_at}</strong>
      </div>
    </div>
  </header>

  <main class="main">

    <!-- KPIカード -->
    <div class="section-title">📈 サマリー</div>
    <div class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-icon">🎯</div>
        <div class="kpi-label">候補銘柄数</div>
        <div class="kpi-value">{total_candidates}</div>
        <div class="kpi-sub">スクリーニング合致</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-icon">📅</div>
        <div class="kpi-label">基準日</div>
        <div class="kpi-value" style="font-size:1.3rem;">{base_date}</div>
        <div class="kpi-sub">スクリーニング実行日</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-icon">🏆</div>
        <div class="kpi-label">出来高トップ銘柄</div>
        <div class="kpi-value" style="font-size:1.3rem;">{top1_code}</div>
        <div class="kpi-sub">{top1_name} ({top1_volume})</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-icon">🔍</div>
        <div class="kpi-label">スクリーニング条件</div>
        <div class="kpi-value" style="font-size:0.9rem; line-height:1.5;">SMA順行配列</div>
        <div class="kpi-sub">出来高 ≥ {min_volume}株</div>
      </div>
    </div>

    <!-- クイックアクション -->
    <div class="section-title">⚡ クイックアクション</div>
    <div class="actions-row">
      <a class="action-btn action-btn-primary" href="report.html" target="_blank">
        📋 スクリーニングレポートを開く
      </a>
      <a class="action-btn action-btn-secondary" href="http://localhost:8501" target="_blank">
        📊 Streamlit UI（日付指定）
      </a>
      <a class="action-btn action-btn-secondary" href="../snapshots/gallery.html" target="_blank">
        📸 チャートギャラリー
      </a>
      <a class="action-btn action-btn-secondary" href="backtest_report.html" target="_blank">
        📈 バックテストレポート
      </a>
    </div>

    <!-- 出来高TOP10グラフ -->
    <div class="section-title chart-section">📊 出来高 TOP 10</div>
    <div class="chart-section">
      <div class="chart-wrap">
        {top10_bars}
      </div>
    </div>

    <!-- 候補銘柄テーブル -->
    <div class="section-title table-section">📋 候補銘柄一覧（全{total_candidates}件）</div>
    <div class="table-section">
      <div class="table-container">
        <div class="table-toolbar">
          <h2>候補銘柄</h2>
          <input type="text" class="search-box" placeholder="🔍 コード・名前で検索..." id="searchInput">
        </div>
        {table_content}
      </div>
    </div>

  </main>

  <footer class="footer">
    <p>⚠️ 本ツールは投資助言を目的としたものではありません。投資判断は自己責任でお願いいたします。</p>
    <p style="margin-top:0.4rem;">Generated at {generated_at}</p>
  </footer>

  <script>
    // 検索
    document.getElementById('searchInput').addEventListener('input', function(e) {{
      const q = e.target.value.toLowerCase();
      document.querySelectorAll('tbody tr').forEach(function(row) {{
        row.style.display = row.textContent.toLowerCase().includes(q) ? '' : 'none';
      }});
    }});

    // ソート
    const sortState = {{}};
    document.querySelectorAll('thead th[data-sort]').forEach(function(th) {{
      th.addEventListener('click', function() {{
        const key = this.dataset.sort;
        const isNum = this.dataset.type === 'number';
        sortState[key] = sortState[key] === 'asc' ? 'desc' : 'asc';
        const asc = sortState[key] === 'asc';
        document.querySelectorAll('thead th').forEach(function(h) {{
          h.classList.remove('sorted');
          h.textContent = h.textContent.replace(/ [▲▼]$/, '');
        }});
        this.classList.add('sorted');
        this.textContent += asc ? ' ▲' : ' ▼';
        const tbody = document.querySelector('tbody');
        const rows = Array.from(tbody.querySelectorAll('tr'));
        rows.sort(function(a, b) {{
          const av = a.querySelector(`td[data-key="${{key}}"]`)?.dataset.value || '';
          const bv = b.querySelector(`td[data-key="${{key}}"]`)?.dataset.value || '';
          const an = isNum ? (parseFloat(av) || 0) : av;
          const bn = isNum ? (parseFloat(bv) || 0) : bv;
          return asc ? (an > bn ? 1 : -1) : (an < bn ? 1 : -1);
        }});
        rows.forEach(function(r) {{ tbody.appendChild(r); }});
      }});
    }});
  </script>

</body>
</html>"""


def _build_admin_table(candidates: list) -> str:
    """
    管理画面用の候補銘柄テーブルHTMLを生成する。

    Args:
        candidates: 候補銘柄の辞書リスト

    Returns:
        str: HTML テーブル文字列
    """
    if not candidates:
        return "<p style='padding:2rem;color:#8899aa;text-align:center;'>候補銘柄が見つかりませんでした</p>"

    # 出来高の最大値（バー幅算出用）
    max_volume = max(c["volume"] for c in candidates)

    # TradingViewのベースURL
    tv_base = "https://jp.tradingview.com/chart/M3vhlCeS/?symbol=TSE%3A"

    rows = []
    for i, c in enumerate(candidates, 1):
        vol_pct = (c["volume"] / max_volume * 100) if max_volume > 0 else 0
        tv_url = f"{tv_base}{c['code']}"
        row = (
            f"<tr>"
            f"<td style='text-align:center;color:#5a6a7a;' data-key='rank' data-value='{i}'>{i}</td>"
            f"<td data-key='code' data-value='{c['code']}'>"
            f"  <a class='code-link' href='{tv_url}' target='_blank'>{c['code']}</a>"
            f"  <div class='ticker-name'>{c.get('name', '')}</div>"
            f"</td>"
            f"<td class='num-cell' data-key='close' data-value='{c['close']}'>{c['close']:,.1f}</td>"
            f"<td class='num-cell' data-key='sma5' data-value='{c['sma5']}'>{c['sma5']:,.1f}</td>"
            f"<td class='num-cell' data-key='sma20' data-value='{c['sma20']}'>{c['sma20']:,.1f}</td>"
            f"<td class='num-cell' data-key='sma60' data-value='{c['sma60']}'>{c['sma60']:,.1f}</td>"
            f"<td data-key='volume' data-value='{c['volume']}'>"
            f"  <div class='vol-bar-wrap'>"
            f"    <span class='num-cell'>{c['volume']:,.0f}</span>"
            f"    <div class='vol-bar'><div class='vol-bar-fill' style='width:{vol_pct:.0f}%'></div></div>"
            f"  </div>"
            f"</td>"
            f"<td class='num-cell' data-key='volume_ratio' data-value='{c['volume_ratio']}'>{c['volume_ratio']:.2f}x</td>"
            f"</tr>"
        )
        rows.append(row)

    table = (
        "<table>"
        "<thead><tr>"
        "<th style='width:48px;text-align:center;'>#</th>"
        "<th data-sort='code' data-type='number'>銘柄</th>"
        "<th data-sort='close' data-type='number' style='text-align:right;'>終値</th>"
        "<th data-sort='sma5' data-type='number' style='text-align:right;'>SMA5</th>"
        "<th data-sort='sma20' data-type='number' style='text-align:right;'>SMA20</th>"
        "<th data-sort='sma60' data-type='number' style='text-align:right;'>SMA60</th>"
        "<th data-sort='volume' data-type='number'>出来高</th>"
        "<th data-sort='volume_ratio' data-type='number' style='text-align:right;'>出来高比</th>"
        "</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )
    return table


def _build_top10_bars(candidates: list) -> str:
    """
    出来高TOP10の横棒グラフHTMLを生成する。

    Args:
        candidates: 候補銘柄の辞書リスト（出来高降順でソート済みを想定）

    Returns:
        str: HTML 文字列
    """
    if not candidates:
        return ""

    # 出来高降順でTOP10を取得する
    top10 = sorted(candidates, key=lambda x: x["volume"], reverse=True)[:10]
    max_vol = top10[0]["volume"] if top10 else 1
    tv_base = "https://jp.tradingview.com/chart/M3vhlCeS/?symbol=TSE%3A"

    bars = []
    for c in top10:
        pct = (c["volume"] / max_vol) * 100
        tv_url = f"{tv_base}{c['code']}"
        label = f"{c['code']} {c.get('name', '')[:6]}"
        vol_str = f"{c['volume']:,.0f}株"
        bar = (
            f"<div class='bar-row'>"
            f"  <div class='bar-label'><a href='{tv_url}' target='_blank'>{label}</a></div>"
            f"  <div class='bar-track'><div class='bar-fill' style='width:{pct:.1f}%'></div></div>"
            f"  <div class='bar-val'>{vol_str}</div>"
            f"</div>"
        )
        bars.append(bar)
    return "\n".join(bars)


# =========================================================
# 管理ダッシュボード生成
# =========================================================
def generate_admin(json_path: str = None) -> str:
    """
    スクリーニング結果のJSONから管理ダッシュボードHTMLを生成する。

    Args:
        json_path: JSONファイルのパス（省略時はlatest.jsonを使用）

    Returns:
        str: 生成されたHTMLファイルのパス
    """
    if json_path is None:
        json_path = LATEST_JSON

    if not os.path.exists(json_path):
        print(f"❌ 結果ファイルが見つかりません: {json_path}")
        return ""

    # JSONの読み込み
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    candidates = data.get("candidates", [])
    generated_at = data.get("generated_at", "不明")
    conditions = data.get("conditions", {})
    total_candidates = data.get("total_candidates", len(candidates))
    min_volume = f"{conditions.get('min_volume', 500000):,}"
    base_date = candidates[0]["date"] if candidates else "N/A"

    # 出来高トップ銘柄
    if candidates:
        top1 = max(candidates, key=lambda x: x["volume"])
        top1_code = top1["code"]
        top1_name = top1.get("name", "")
        top1_volume = f"{top1['volume']:,}株"
    else:
        top1_code = "—"
        top1_name = ""
        top1_volume = "—"

    # HTMLパーツ生成
    table_content = _build_admin_table(candidates)
    top10_bars = _build_top10_bars(candidates)

    # テンプレートに埋め込む
    html = ADMIN_TEMPLATE.format(
        generated_at=generated_at,
        total_candidates=total_candidates,
        base_date=base_date,
        top1_code=top1_code,
        top1_name=top1_name,
        top1_volume=top1_volume,
        min_volume=min_volume,
        top10_bars=top10_bars,
        table_content=table_content,
    )

    # ファイルへ書き出す
    with open(OUTPUT_ADMIN, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"✅ 管理ダッシュボードを生成しました: {OUTPUT_ADMIN}")
    return OUTPUT_ADMIN


# =========================================================
# エントリーポイント
# =========================================================
if __name__ == "__main__":
    path = generate_report()
    admin_path = generate_admin()
    if admin_path:
        print(f"\n🌐 管理ダッシュボードをブラウザで開きます...")
        import webbrowser
        webbrowser.open(f"file://{admin_path}")
