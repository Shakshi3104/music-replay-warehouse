# Music Replay Warehouse

macOSのMusic.appライブラリから再生回数のスナップショットを取得し、**Apple Music Replay風の振り返り分析**を自作するためのDWHプロジェクト。

## 背景

Apple Music Replayは便利だが、過去のデータが保持されない。そこで、ローカルのライブラリデータを蓄積し、自分だけの再生履歴分析を実現する。

## 出力例

```
┌─────────────────────────┬───────────────────────┬─────────────┬──────────────────────┐
│ total_listening_minutes │ total_listening_hours │ total_plays │ unique_tracks_played │
├─────────────────────────┼───────────────────────┼─────────────┼──────────────────────┤
│                31322.18 │                 522.0 │        7427 │                  357 │
└─────────────────────────┴───────────────────────┴─────────────┴──────────────────────┘
```

## 技術スタック

| 項目 | 選定 |
|------|------|
| データベース | DuckDB |
| データソース | Music Library XML / CSV（music-library-exporter-swift） |
| 変換ツール | dbt-duckdb |
| アーキテクチャ | メダリオンアーキテクチャ |

## アーキテクチャ

[メダリオンアーキテクチャ](https://www.databricks.com/glossary/medallion-architecture)を採用し、4層構成でデータを段階的に精製。

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

## ディレクトリ構成

```
music-replay-warehouse/
├── data/
│   └── snapshots/           # スナップショットデータ
│       └── YYYY-MM-DD/
├── docs/
│   └── design.md            # 設計書
├── models/
│   ├── bronze/              # 生データ
│   ├── silver/              # クリーニング済み
│   ├── gold/                # ディメンション・ファクト
│   └── platinum/            # Replay風レポート
├── scripts/
│   ├── extract_music_snapshots.py  # TimeMachineから抽出
│   ├── load_xml_snapshot.py        # XMLをDuckDBにロード
│   └── load_csv_snapshot.py        # CSVをDuckDBにロード
├── tools/
│   ├── test_itlibrary.swift        # iTunes Library Framework検証
│   └── test_timemachine_library.sh # TimeMachineライブラリ検証
├── dbt_project.yml
├── profiles.yml
├── requirements.txt
└── README.md
```

## 関連リポジトリ

- [music-library-exporter-swift](https://github.com/Shakshi3104/music-library-exporter-swift) - Music.appライブラリをCSV/JSONにエクスポートするSwiftツール

## ドキュメント

- [設計書](docs/design.md)

## 参考

- [spotify-dlt-duck-db](https://github.com/Shakshi3104/spotify-dlt-duck-db)
- [dbt×DuckDBでSpotify再生履歴を分析するローカルDWHを作る](https://zenn.dev/shakshi3104/articles/b997855b066d62)
- [dbt×DuckDBでシアトル図書館の貸出履歴を分析する](https://zenn.dev/shakshi3104/articles/88f1773d46d854)

## ライセンス

MIT
