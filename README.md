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
| データソース | TimeMachine内のMusic Library.xml |
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

## ドキュメント

- [設計書](docs/design.md)

## 参考

- [spotify-dlt-duck-db](https://github.com/Shakshi3104/spotify-dlt-duck-db)
- [dbt×DuckDBでSpotify再生履歴を分析するローカルDWHを作る](https://zenn.dev/shakshi3104/articles/b997855b066d62)
- [dbt×DuckDBでシアトル図書館の貸出履歴を分析する](https://zenn.dev/shakshi3104/articles/88f1773d46d854)

## ライセンス

MIT
