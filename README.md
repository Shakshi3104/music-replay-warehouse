# Music Replay Warehouse

macOSのMusic.appライブラリから再生回数のスナップショットを取得し、**Apple Music Replay風の振り返り分析**を自作するためのDWHプロジェクト。

## 技術スタック

| 項目 | 選定 |
|------|------|
| データベース | DuckDB |
| データソース | Music Library XML |
| 変換ツール | dbt-duckdb |
| アーキテクチャ | メダリオンアーキテクチャ |


## アーキテクチャ

```
┌─────────────────────────────────────────────────────────────────┐
│  Bronze    │ raw_itunes_library → bronze_itunes_library        │
├─────────────────────────────────────────────────────────────────┤
│  Silver    │ silver_tracks（クリーニング・型変換）              │
├─────────────────────────────────────────────────────────────────┤
│  Gold      │ dim_track, dim_artist, dim_album                  │
│            │ fact_play_count_snapshot                          │
├─────────────────────────────────────────────────────────────────┤
│  Platinum  │ platinum_summary, platinum_top_songs              │
│            │ platinum_top_artists, platinum_top_albums         │
└─────────────────────────────────────────────────────────────────┘
```

## セットアップ

### 1. 依存関係のインストール

```bash
pip install -r requirements.txt
```

### 2. データの準備

#### XMLファイルがある場合

```bash
python3 scripts/load_xml_snapshot.py data/snapshots/YYYY-MM-DD/music-library.xml
```

#### CSVファイルがある場合（music-library-exporter-swift出力）

```bash
python3 scripts/load_csv_snapshot.py data/snapshots/YYYY-MM-DD/music-library.csv
```

### 3. dbtモデルを実行

```bash
dbt run --profiles-dir .
```

### 4. 結果を確認

```bash
duckdb music_replay.duckdb

# サマリー
SELECT * FROM platinum_summary;

# トップソング
SELECT * FROM platinum_top_songs LIMIT 20;

# トップアーティスト
SELECT * FROM platinum_top_artists LIMIT 20;

# トップアルバム
SELECT * FROM platinum_top_albums LIMIT 20;
```

## データカタログ（OKF）

dbt の yml に書いた description と、実際のテーブルの型から、[OKF（Open Knowledge Format）](https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md) v0.2 のデータカタログを生成できる。1モデルが1つの Markdown ファイルになる。

```bash
dbt docs generate --profiles-dir .
python3 scripts/generate_okf.py --manifest target/manifest.json --catalog target/catalog.json --out target/okf
```

GitHub Actions（`.github/workflows/okf_catalog.yml`）でも、`models/` などが変わるたびに同梱のスナップショットで生成し、成果物 `okf-catalog` として出力している。

モデルをカタログから外したいときは、yml で `meta.okf: false` を付ける。

```yaml
models:
  - name: some_model
    config:
      meta:
        okf: false
```

## ディレクトリ構成

```
music-replay-warehouse/
├── data/
│   └── snapshots/           # スナップショットデータ
│       └── YYYY-MM-DD/
├── docs/
│   └── design.md            # 設計書
├── scripts/
│   └── generate_okf.py      # OKF カタログの生成
├── models/
│   ├── bronze/              # 生データ
│   ├── silver/              # クリーニング済み
│   ├── gold/                # ディメンション・ファクト
│   └── platinum/            # Replay風レポート
├── dbt_project.yml
├── profiles.yml
├── requirements.txt
└── README.md
```

## 関連リポジトリ

- [music-library-exporter-swift](https://github.com/Shakshi3104/music-library-exporter-swift) - Music.appライブラリをCSV/JSONにエクスポートするSwiftツール
