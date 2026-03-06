"""
レベル2：チャートスナップショット ギャラリーHTML生成

スナップショットフォルダを走査し、銘柄別にチャート画像を一覧表示する
リッチなHTMLギャラリーを生成する。
"""

import json
import os
import base64
from datetime import datetime
from typing import List, Dict, Any


# =========================================================
# 定数
# =========================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SNAPSHOTS_DIR = os.path.join(SCRIPT_DIR, "snapshots")
INDEX_JSON = os.path.join(SNAPSHOTS_DIR, "snapshot_index.json")
OUTPUT_HTML = os.path.join(SNAPSHOTS_DIR, "gallery.html")


# =========================================================
# 画像をBase64エンコード（HTMLに埋め込み用）
# =========================================================
def _image_to_data_uri(filepath: str) -> str:
    """
    画像ファイルをBase64データURIに変換する。

    Args:
        filepath: 画像ファイルパス

    Returns:
        str: data:image/png;base64,... 形式の文字列
    """
    if not os.path.exists(filepath):
        return ""
    with open(filepath, "rb") as f:
        encoded = base64.b64encode(f.read()).decode("utf-8")
    return f"data:image/png;base64,{encoded}"


# =========================================================
# HTMLギャラリー生成
# =========================================================
def generate_gallery(index_path: str = None) -> str:
    """
    スナップショットのHTMLギャラリーを生成する。

    Args:
        index_path: snapshot_index.json のパス

    Returns:
        str: 生成されたHTMLファイルのパス
    """
    if index_path is None:
        index_path = INDEX_JSON

    if not os.path.exists(index_path):
        print("❌ スナップショットインデックスが見つかりません。先に snapshot.py を実行してください。")
        return ""

    with open(index_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    tickers = data.get("tickers", [])
    generated_at = data.get("generated_at", "不明")
    date = data.get("date", "")

    if not tickers:
        print("❌ スナップショットデータがありません。")
        return ""

    # 銘柄リストのHTML（サイドバー用）
    sidebar_items = []
    for i, t in enumerate(tickers):
        active_class = "active" if i == 0 else ""
        sidebar_items.append(
            f'<div class="sidebar-item {active_class}" data-index="{i}" onclick="selectTicker({i})">'
            f'  <span class="sidebar-code">{t["code"]}</span>'
            f'  <span class="sidebar-name">{t.get("name", "")}</span>'
            f'</div>'
        )
    sidebar_html = "\n".join(sidebar_items)

    # 各銘柄のチャートデータ（JavaScript用）
    chart_data = []
    for t in tickers:
        snapshots = t.get("snapshots", {})
        images = {}
        for tf_name in ["4h", "1h", "15m"]:
            filepath = snapshots.get(tf_name, "")
            if filepath and os.path.exists(filepath):
                images[tf_name] = _image_to_data_uri(filepath)
            else:
                images[tf_name] = ""

        chart_data.append({
            "code": t["code"],
            "name": t.get("name", ""),
            "images": images,
        })

    chart_data_json = json.dumps(chart_data, ensure_ascii=False)

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>チャートギャラリー | レベル2</title>
  <!-- レベル1レポートへの相対パス -->
  <script>const REPORT_PATH = '../results/report.html';</script>
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
      --gradient-primary: linear-gradient(135deg, #3b82f6, #06b6d4);
    }}

    * {{ margin: 0; padding: 0; box-sizing: border-box; }}

    /* レベル間ナビゲーションボタン */
    .nav-level-btn {{
      display: inline-flex;
      align-items: center;
      gap: 0.4rem;
      padding: 0.4rem 0.9rem;
      background: rgba(59,130,246,0.12);
      border: 1px solid rgba(59,130,246,0.35);
      color: var(--accent-blue);
      border-radius: 8px;
      font-size: 0.82rem;
      font-weight: 600;
      cursor: pointer;
      text-decoration: none;
      transition: all 0.2s ease;
      white-space: nowrap;
    }}

    .nav-level-btn:hover {{
      background: rgba(59,130,246,0.25);
      transform: translateY(-1px);
    }}

    /* レベルバッジ */
    .level-badge {{
      background: rgba(6,182,212,0.15);
      border: 1px solid rgba(6,182,212,0.35);
      color: var(--accent-cyan);
      padding: 0.2rem 0.6rem;
      border-radius: 6px;
      font-size: 0.8rem;
      font-weight: 700;
    }}

    body {{
      font-family: 'Inter', 'Noto Sans JP', -apple-system, sans-serif;
      background: var(--bg-primary);
      color: var(--text-primary);
      height: 100vh;
      overflow: hidden;
      display: flex;
      flex-direction: column;
    }}

    /* ヘッダー */
    .header {{
      background: var(--bg-secondary);
      border-bottom: 1px solid var(--border-color);
      padding: 1rem 1.5rem;
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-shrink: 0;
    }}

    .header h1 {{
      font-size: 1.3rem;
      font-weight: 700;
      background: var(--gradient-primary);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
    }}

    .header-meta {{
      display: flex;
      gap: 1.5rem;
      color: var(--text-secondary);
      font-size: 0.85rem;
    }}

    .header-meta .value {{ color: var(--text-primary); font-weight: 600; }}

    /* メインレイアウト */
    .layout {{
      display: flex;
      flex: 1;
      overflow: hidden;
    }}

    /* サイドバー */
    .sidebar {{
      width: 240px;
      background: var(--bg-secondary);
      border-right: 1px solid var(--border-color);
      overflow-y: auto;
      flex-shrink: 0;
    }}

    .sidebar-search {{
      padding: 0.75rem;
      border-bottom: 1px solid var(--border-color);
      position: sticky;
      top: 0;
      background: var(--bg-secondary);
      z-index: 10;
    }}

    .sidebar-search input {{
      width: 100%;
      background: var(--bg-card);
      border: 1px solid var(--border-color);
      border-radius: 6px;
      padding: 0.5rem 0.75rem;
      color: var(--text-primary);
      font-size: 0.8rem;
      outline: none;
    }}

    .sidebar-search input:focus {{
      border-color: var(--accent-blue);
    }}

    .sidebar-search input::placeholder {{
      color: var(--text-muted);
    }}

    .sidebar-item {{
      padding: 0.7rem 1rem;
      cursor: pointer;
      border-bottom: 1px solid rgba(42,58,78,0.3);
      transition: all 0.2s ease;
      display: flex;
      flex-direction: column;
      gap: 0.15rem;
    }}

    .sidebar-item:hover {{
      background: var(--bg-card-hover);
    }}

    .sidebar-item.active {{
      background: var(--bg-card);
      border-left: 3px solid var(--accent-blue);
    }}

    .sidebar-code {{
      font-weight: 700;
      color: var(--accent-blue);
      font-size: 0.9rem;
      font-family: 'Inter', monospace;
    }}

    .sidebar-name {{
      color: var(--text-secondary);
      font-size: 0.75rem;
    }}

    /* メインコンテンツ */
    .content {{
      flex: 1;
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }}

    /* タブ */
    .tabs {{
      display: flex;
      gap: 0;
      border-bottom: 1px solid var(--border-color);
      background: var(--bg-secondary);
      flex-shrink: 0;
    }}

    .tab {{
      padding: 0.8rem 1.5rem;
      cursor: pointer;
      color: var(--text-secondary);
      font-size: 0.9rem;
      font-weight: 500;
      border-bottom: 2px solid transparent;
      transition: all 0.2s ease;
    }}

    .tab:hover {{
      color: var(--text-primary);
      background: rgba(59,130,246,0.05);
    }}

    .tab.active {{
      color: var(--accent-blue);
      border-bottom-color: var(--accent-blue);
    }}

    /* チャート表示エリア */
    .chart-area {{
      flex: 1;
      display: flex;
      align-items: center;
      justify-content: center;
      overflow: hidden;
      padding: 1rem;
      position: relative;
    }}

    .chart-area img {{
      max-width: 100%;
      max-height: 100%;
      object-fit: contain;
      border-radius: 8px;
      cursor: zoom-in;
      transition: transform 0.3s ease;
    }}

    .chart-area .no-image {{
      color: var(--text-muted);
      font-size: 1.1rem;
      text-align: center;
    }}

    .chart-area .no-image .icon {{
      font-size: 3rem;
      margin-bottom: 1rem;
      display: block;
    }}

    /* ナビゲーションボタン */
    .nav-btn {{
      position: absolute;
      top: 50%;
      transform: translateY(-50%);
      background: var(--bg-card);
      border: 1px solid var(--border-color);
      color: var(--text-primary);
      width: 44px;
      height: 44px;
      border-radius: 50%;
      cursor: pointer;
      font-size: 1.2rem;
      display: flex;
      align-items: center;
      justify-content: center;
      transition: all 0.2s ease;
      z-index: 10;
    }}

    .nav-btn:hover {{
      background: var(--accent-blue);
      border-color: var(--accent-blue);
    }}

    .nav-prev {{ left: 1rem; }}
    .nav-next {{ right: 1rem; }}

    /* ステータスバー */
    .statusbar {{
      background: var(--bg-secondary);
      border-top: 1px solid var(--border-color);
      padding: 0.5rem 1rem;
      display: flex;
      justify-content: space-between;
      font-size: 0.78rem;
      color: var(--text-muted);
      flex-shrink: 0;
    }}

    /* モーダル（画像拡大） */
    .modal {{
      display: none;
      position: fixed;
      top: 0; left: 0;
      width: 100%; height: 100%;
      background: rgba(0,0,0,0.9);
      z-index: 1000;
      cursor: zoom-out;
      align-items: center;
      justify-content: center;
    }}

    .modal.active {{
      display: flex;
    }}

    .modal img {{
      max-width: 95%;
      max-height: 95%;
      object-fit: contain;
    }}

    /* ショートカットヘルプ */
    .shortcuts {{
      display: none;
      position: fixed;
      bottom: 3rem;
      right: 1rem;
      background: var(--bg-card);
      border: 1px solid var(--border-color);
      border-radius: 8px;
      padding: 1rem;
      font-size: 0.8rem;
      color: var(--text-secondary);
      z-index: 100;
    }}

    .shortcuts.active {{ display: block; }}

    .shortcuts kbd {{
      background: var(--bg-secondary);
      border: 1px solid var(--border-color);
      border-radius: 3px;
      padding: 0.1rem 0.4rem;
      font-family: 'Inter', monospace;
      font-size: 0.75rem;
      color: var(--text-primary);
    }}
  </style>
</head>
<body>

  <!-- ヘッダー -->
  <header class="header">
    <h1>📸 チャートギャラリー</h1>
    <div class="header-meta">
      <button class="nav-level-btn" onclick="window.open(REPORT_PATH, '_blank')" title="レベル1スクリーニング結果へ戻る">
        ← レベル1 スクリーニング結果
      </button>
      <span class="level-badge">LEVEL 2</span>
      <span>銘柄数: <span class="value">{len(tickers)}</span></span>
      <span>取得日: <span class="value">{date}</span></span>
    </div>
  </header>

  <!-- メイン -->
  <div class="layout">
    <!-- サイドバー -->
    <aside class="sidebar">
      <div class="sidebar-search">
        <input type="text" placeholder="🔍 銘柄検索..." id="sidebarSearch">
      </div>
      {sidebar_html}
    </aside>

    <!-- コンテンツ -->
    <main class="content">
      <!-- タブ -->
      <div class="tabs">
        <div class="tab active" data-tf="4h" onclick="selectTimeframe('4h')">4時間足</div>
        <div class="tab" data-tf="1h" onclick="selectTimeframe('1h')">1時間足</div>
        <div class="tab" data-tf="15m" onclick="selectTimeframe('15m')">15分足</div>
      </div>

      <!-- チャートエリア -->
      <div class="chart-area" id="chartArea">
        <button class="nav-btn nav-prev" onclick="prevTicker()">◀</button>
        <img id="chartImage" src="" alt="チャート">
        <button class="nav-btn nav-next" onclick="nextTicker()">▶</button>
      </div>
    </main>
  </div>

  <!-- ステータスバー -->
  <div class="statusbar">
    <span id="tickerInfo">-</span>
    <span>← → キーで銘柄切替 | 1 2 3 キーで時間足切替 | ? でヘルプ</span>
  </div>

  <!-- モーダル -->
  <div class="modal" id="modal" onclick="closeModal()">
    <img id="modalImage" src="" alt="">
  </div>

  <!-- ショートカットヘルプ -->
  <div class="shortcuts" id="shortcuts">
    <p><kbd>←</kbd> <kbd>→</kbd> 前/次の銘柄</p>
    <p><kbd>1</kbd> <kbd>2</kbd> <kbd>3</kbd> 4H / 1H / 15M切替</p>
    <p><kbd>Space</kbd> 画像拡大</p>
    <p><kbd>?</kbd> ヘルプ表示/非表示</p>
  </div>

  <script>
    // チャートデータ
    const chartData = {chart_data_json};

    let currentIndex = 0;
    let currentTimeframe = '4h';

    // 銘柄選択
    function selectTicker(index) {{
      if (index < 0 || index >= chartData.length) return;

      // サイドバーの更新
      document.querySelectorAll('.sidebar-item').forEach(el => el.classList.remove('active'));
      const items = document.querySelectorAll('.sidebar-item');
      if (items[index]) {{
        items[index].classList.add('active');
        items[index].scrollIntoView({{ behavior: 'smooth', block: 'nearest' }});
      }}

      currentIndex = index;
      updateChart();
    }}

    // 時間足選択
    function selectTimeframe(tf) {{
      document.querySelectorAll('.tab').forEach(el => el.classList.remove('active'));
      document.querySelector(`.tab[data-tf="${{tf}}"]`).classList.add('active');
      currentTimeframe = tf;
      updateChart();
    }}

    // チャート更新
    function updateChart() {{
      const ticker = chartData[currentIndex];
      const img = document.getElementById('chartImage');
      const src = ticker.images[currentTimeframe];

      if (src) {{
        img.src = src;
        img.style.display = 'block';
        img.onclick = () => openModal(src);
      }} else {{
        img.style.display = 'none';
        const area = document.getElementById('chartArea');
        let noImg = area.querySelector('.no-image');
        if (!noImg) {{
          noImg = document.createElement('div');
          noImg.className = 'no-image';
          area.appendChild(noImg);
        }}
        noImg.innerHTML = '<span class="icon">📊</span>チャート画像がありません';
      }}

      // no-image要素の除去（画像がある場合）
      if (src) {{
        const noImg = document.querySelector('.chart-area .no-image');
        if (noImg) noImg.remove();
      }}

      // ステータスバー更新
      const tfLabels = {{ '4h': '4時間足', '1h': '1時間足', '15m': '15分足' }};
      document.getElementById('tickerInfo').textContent =
        `${{currentIndex + 1}} / ${{chartData.length}} | ${{ticker.code}} ${{ticker.name}} | ${{tfLabels[currentTimeframe]}}`;
    }}

    function prevTicker() {{ selectTicker(currentIndex - 1); }}
    function nextTicker() {{ selectTicker(currentIndex + 1); }}

    // モーダル
    function openModal(src) {{
      const modal = document.getElementById('modal');
      document.getElementById('modalImage').src = src;
      modal.classList.add('active');
    }}

    function closeModal() {{
      document.getElementById('modal').classList.remove('active');
    }}

    // キーボードショートカット
    document.addEventListener('keydown', (e) => {{
      if (e.target.tagName === 'INPUT') return;

      switch(e.key) {{
        case 'ArrowLeft':  prevTicker(); break;
        case 'ArrowRight': nextTicker(); break;
        case '1': selectTimeframe('4h'); break;
        case '2': selectTimeframe('1h'); break;
        case '3': selectTimeframe('15m'); break;
        case ' ':
          e.preventDefault();
          const src = chartData[currentIndex]?.images[currentTimeframe];
          if (src) openModal(src);
          break;
        case 'Escape': closeModal(); break;
        case '?':
          document.getElementById('shortcuts').classList.toggle('active');
          break;
      }}
    }});

    // サイドバー検索
    document.getElementById('sidebarSearch').addEventListener('input', (e) => {{
      const query = e.target.value.toLowerCase();
      document.querySelectorAll('.sidebar-item').forEach(item => {{
        const text = item.textContent.toLowerCase();
        item.style.display = text.includes(query) ? '' : 'none';
      }});
    }});

    // URLハッシュに基づく初期銘柄選択
    // レベル1から #ticker=CODE で指定された銘柄を表示
    function initFromHash() {{
      const hash = window.location.hash;
      if (hash && hash.startsWith('#ticker=')) {{
        const code = hash.slice('#ticker='.length).trim();
        const idx = chartData.findIndex(t => t.code === code || t.code === code + '.T');
        if (idx >= 0) {{
          selectTicker(idx);
          return;
        }}
      }}
      // ハッシュなし or 見つからない場合は先頭を表示
      updateChart();
    }}

    initFromHash();
  </script>

</body>
</html>"""

    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"✅ ギャラリーHTMLを生成しました: {OUTPUT_HTML}")
    return OUTPUT_HTML


# =========================================================
# エントリーポイント
# =========================================================
if __name__ == "__main__":
    path = generate_gallery()
    if path:
        print(f"\n🌐 ブラウザで開きます...")
        import webbrowser
        webbrowser.open(f"file://{path}")
