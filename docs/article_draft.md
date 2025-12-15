# Apple Music Replay風の再生履歴分析をdbt + DuckDBで自作した

> この記事は dbt Advent Calendar 2024 の X 日目の記事です。

## はじめに

Apple Music Replayは年間の再生統計を見せてくれる便利な機能ですが、過去のデータが保持されません。「去年は何を聴いていたっけ？」と振り返りたくても、データが残っていないのです。

そこで、ローカルのMusic.appライブラリからデータを抽出し、dbt + DuckDBで自分だけのReplay分析を作ることにしました。

## データ取得の苦労話

### iTunes Library Frameworkの壁

macOSには`iTunesLibrary`フレームワークがあり、Music.appのライブラリを読み取れます。TimeMachineのバックアップから過去のライブラリを読めば、再生回数の差分が取れるはず...と思ったのですが、**このフレームワークは現在のライブラリしか読めません**。

設定ファイル（`com.apple.Music.plist`）の`library-url`を書き換えて偽装を試みましたが、フレームワーク内部でキャッシュを参照しているようで、うまくいきませんでした。

### .musicdbは暗号化されている

現在のMusic.appはXMLではなく`.musicdb`というバイナリ形式でライブラリを保存しています。調べてみると、AES128-ECBで暗号化されており、復号キーは非公開。リバースエンジニアリングの試みはあるものの、実用レベルには達していません。

### 結論：XMLエクスポートを使う

結局、過去に手動でエクスポートしていたXMLファイルと、Swiftで作った現在のライブラリエクスポートツールを組み合わせることにしました。

## 本題：dbt + DuckDBでメダリオンアーキテクチャ

### なぜDuckDB？

- **インストール不要**：`pip install duckdb dbt-duckdb`で完結
- **高速**：分析用途に最適化されたカラムナDB
- **SQL互換**：PostgreSQLライクな構文
- **ファイルベース**：`.duckdb`ファイル1つで完結

### メダリオンアーキテクチャの設計

Databricksが提唱するメダリオンアーキテクチャを採用し、4層構成でデータを精製します。

```
Bronze  → Silver  → Gold      → Platinum
(生データ) (整形)   (モデル)    (分析)
```

| レイヤー | 役割 | モデル |
|---------|------|--------|
| Bronze | 生データをそのままロード | `bronze_itunes_library` |
| Silver | クリーニング、型変換 | `silver_tracks` |
| Gold | ディメンション・ファクト | `dim_track`, `fact_play_count_snapshot` |
| Platinum | 分析用マート | `platinum_top_songs`, `platinum_summary` |

### Bronze層：生データの取り込み

```sql
-- models/bronze/bronze_itunes_library.sql
{{ config(materialized='table') }}

SELECT
    snapshot_date,
    track_id,
    name,
    artist,
    album,
    play_count,
    persistent_id,
    ...
FROM {{ source('itunes', 'raw_itunes_library') }}
```

Pythonスクリプトで`raw_itunes_library`テーブルにロードしたデータを、そのままBronze層に取り込みます。

### Silver層：クリーニングと整形

```sql
-- models/silver/silver_tracks.sql
{{ config(materialized='table') }}

SELECT
    persistent_id AS track_persistent_id,
    snapshot_date,
    name AS title,
    artist AS artist_name,
    COALESCE(play_count, 0) AS play_count,
    ROUND(total_time / 1000.0 / 60.0, 2) AS duration_min,
    ...
FROM {{ ref('bronze_itunes_library') }}
WHERE persistent_id IS NOT NULL
```

- カラム名のリネーム（`name` → `title`）
- NULL値のハンドリング
- 再生時間の単位変換（ミリ秒 → 分）

### Gold層：ディメンションとファクト

#### fact_play_count_snapshot：再生回数の差分計算

```sql
-- models/gold/fact_play_count_snapshot.sql
{{ config(materialized='table') }}

SELECT
    track_persistent_id,
    snapshot_date,
    play_count,
    duration_min,

    -- 前回スナップショットからの差分
    play_count - LAG(play_count) OVER (
        PARTITION BY track_persistent_id
        ORDER BY snapshot_date
    ) AS play_count_delta,

    LAG(snapshot_date) OVER (
        PARTITION BY track_persistent_id
        ORDER BY snapshot_date
    ) AS prev_snapshot_date

FROM {{ ref('silver_tracks') }}
```

`LAG`ウィンドウ関数で前回スナップショットとの差分を計算。これが分析の核となるデータです。

### Platinum層：Replay風レポート

#### platinum_top_songs：トップソングランキング

```sql
-- models/platinum/platinum_top_songs.sql
{{ config(materialized='view') }}

WITH song_plays AS (
    SELECT
        d.title,
        d.artist_name,
        d.album_name,
        SUM(CASE WHEN f.play_count_delta > 0
            THEN f.play_count_delta ELSE 0 END) AS play_count,
        SUM(CASE WHEN f.play_count_delta > 0
            THEN f.play_count_delta * f.duration_min ELSE 0 END) AS listening_minutes
    FROM {{ ref('fact_play_count_snapshot') }} f
    LEFT JOIN {{ ref('dim_track') }} d
        ON f.track_persistent_id = d.track_persistent_id
    WHERE f.play_count_delta IS NOT NULL
    GROUP BY 1, 2, 3
)

SELECT
    title,
    artist_name,
    play_count,
    ROUND(listening_minutes, 0) AS listening_minutes,
    RANK() OVER (ORDER BY play_count DESC) AS rank
FROM song_plays
WHERE play_count > 0
ORDER BY rank
```

## 結果

### サマリー

```sql
SELECT * FROM platinum_summary;
```

```
┌─────────────────────────┬───────────────────────┬─────────────┬──────────────────────┐
│ total_listening_minutes │ total_listening_hours │ total_plays │ unique_tracks_played │
├─────────────────────────┼───────────────────────┼─────────────┼──────────────────────┤
│                31322.18 │                 522.0 │        7427 │                  357 │
└─────────────────────────┴───────────────────────┴─────────────┴──────────────────────┘
```

約13ヶ月で**522時間**、**7,427回**再生していました。

### トップソング

```sql
SELECT * FROM platinum_top_songs LIMIT 10;
```

| rank | title | artist_name | play_count |
|------|-------|-------------|------------|
| 1 | 勇気100% | なにわ男子 | 124 |
| 2 | コイスルヒカリ | なにわ男子 | 123 |
| 3 | Step and Go | 嵐 | 116 |
| 4 | PIKA☆☆NCHI DOUBLE | 嵐 | 114 |
| 5 | 風の向こうへ | 嵐 | 108 |

### トップアーティスト

```sql
SELECT * FROM platinum_top_artists LIMIT 5;
```

| rank | artist_name | play_count | listening_minutes |
|------|-------------|------------|-------------------|
| 1 | 嵐 | 4,907 | 21,428 |
| 2 | なにわ男子 | 1,747 | 6,729 |
| 3 | 櫻井翔 | 187 | 712 |

## まとめ

dbt + DuckDBの組み合わせは、ローカルでの分析に最適でした。

- **セットアップが簡単**：`pip install`で完結
- **SQLだけで完結**：メダリオンアーキテクチャの各層をSQLで定義
- **再現性**：`dbt run`一発で全モデルが再構築される

Apple Music Replayのような分析を自作したい方、ぜひ試してみてください。

## リポジトリ

- [music-replay-warehouse](https://github.com/Shakshi3104/music-replay-warehouse)
- [music-library-exporter-swift](https://github.com/Shakshi3104/music-library-exporter-swift)

## 参考

- [メダリオンアーキテクチャ - Databricks](https://www.databricks.com/glossary/medallion-architecture)
- [dbt-duckdb](https://github.com/duckdb/dbt-duckdb)
