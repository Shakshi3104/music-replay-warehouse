# Music Replay Warehouse

macOSのTimeMachineバックアップからiTunes/Musicライブラリの再生回数を追跡し、**Apple Music Replay風の年間振り返り**を自作するためのDWHプロジェクト。

## 目的

Apple Musicサブスクリプションなしで、以下のような年間統計を取得する：

- 年間総再生時間
- トップアーティストランキング
- トップソングランキング
- トップアルバムランキング

## 技術スタック

| 項目 | 選定 |
|------|------|
| データベース | DuckDB |
| データソース | Music Library.xml |
| 変換ツール | Python + dbt-duckdb |

## アーキテクチャ

[メダリオンアーキテクチャ](https://www.databricks.com/glossary/medallion-architecture)を採用。

| レイヤー | 役割 | 命名規則 |
|---------|------|----------|
| **Bronze** | 生データをそのままロード | `bronze_{source}` |
| **Silver** | クリーニング、型変換、基本整形 | `silver_{entity}` |
| **Gold** | ディメンション・ファクトモデル | `dim_{entity}`, `fact_{entity}` |
| **Platinum** | 分析用マート（Replay風レポート） | `platinum_{report}` |

## セットアップ

```bash
# 依存関係のインストール
pip install duckdb dbt-duckdb

# TimeMachineからデータを抽出
python scripts/extract_music_snapshots.py

# dbtモデルを実行
dbt run
```

## ディレクトリ構成

```
music-replay-warehouse/
├── docs/
│   └── design.md              # 設計書
├── models/
│   ├── bronze/                # 生データ
│   ├── silver/                # クリーニング済み
│   ├── gold/                  # ディメンション・ファクト
│   └── platinum/              # Replay風レポート
├── scripts/
│   └── extract_music_snapshots.py
├── dbt_project.yml
└── README.md
```
