# Music Replay Warehouse 設計書

## 1. プロジェクト概要

### 1.1 目的
macOSのTimeMachineバックアップに保存されたiTunes/Musicライブラリから再生回数のスナップショットを取得し、**Apple Music Replayのような年間振り返り機能**を自作する。

主な分析目標：
- 年間総再生時間
- トップアーティストランキング
- トップソングランキング
- トップアルバムランキング
- 月別・週別の再生トレンド

### 1.2 技術スタック
| 項目 | 選定 |
|------|------|
| **データベース** | DuckDB |
| **データソース** | TimeMachine内のiTunes/Musicライブラリ（`Music Library.xml`） |
| **変換ツール** | Python + dbt-duckdb |
| **可視化** | Jupyter Notebook / Evidence / Streamlit等 |

### 1.3 ライブラリファイルの場所
```
# 現在のライブラリ
~/Music/Music/Music Library.xml
# または旧iTunes
~/Music/iTunes/iTunes Music Library.xml

# TimeMachineバックアップ
/Volumes/[TimeMachine Volume]/Backups.backupdb/[Mac名]/[日付]/Macintosh HD/Users/[ユーザー名]/Music/...
```

---

## 2. データアーキテクチャ

### 2.1 メダリオンアーキテクチャ

[Databricksのメダリオンアーキテクチャ](https://www.databricks.com/glossary/medallion-architecture)を採用し、4層構成でデータを段階的に精製する。

```
models/
├── bronze/         # 生データ（TimeMachineからロード）
├── silver/         # クリーニング・整形済み中間テーブル
├── gold/           # ディメンション・ファクトテーブル
└── platinum/       # 分析用マート（Replay風レポート）
```

### 2.2 レイヤー定義

| レイヤー | 役割 | マテリアライゼーション | 命名規則 |
|---------|------|----------------------|----------|
| **Bronze** | 生データをそのままロード | table | `bronze_{source}` |
| **Silver** | クリーニング、型変換、基本整形 | table | `silver_{entity}` |
| **Gold** | ディメンション・ファクトモデル | table/view | `dim_{entity}`, `fact_{entity}` |
| **Platinum** | 分析用マート、レポート | view | `platinum_{report}` |

---

## 3. データモデル設計

### 3.1 Bronze Layer

TimeMachineからロードした生データ。変換なしでそのまま格納。

#### bronze_itunes_library

```sql
-- models/bronze/bronze_itunes_library.sql
{{ config(materialized='table') }}

SELECT
    snapshot_date,
    snapshot_path,
    track_id,
    name,
    artist,
    album_artist,
    album,
    genre,
    kind,
    total_time,
    disc_number,
    disc_count,
    track_number,
    track_count,
    year,
    date_added,
    play_count,
    play_date,
    play_date_utc,
    skip_count,
    skip_date,
    rating,
    loved,
    persistent_id,
    location
FROM {{ source('itunes', 'raw_library') }}
```

### 3.2 Silver Layer

基本的なクリーニングと整形を実施。

#### silver_tracks

```sql
-- models/silver/silver_tracks.sql
{{ config(materialized='table') }}

SELECT
    -- Keys
    persistent_id AS track_persistent_id,
    snapshot_date,

    -- Track Info
    track_id AS itunes_track_id,
    name AS title,
    artist AS artist_name,
    album_artist AS album_artist_name,
    album AS album_name,
    genre,
    year AS release_year,
    kind AS file_type,

    -- Duration
    total_time AS duration_ms,
    ROUND(total_time / 1000.0 / 60.0, 2) AS duration_min,

    -- Album Info
    disc_number,
    disc_count,
    track_number,
    track_count,

    -- Play Stats
    COALESCE(play_count, 0) AS play_count,
    play_date AS last_played_at,
    play_date_utc AS last_played_at_utc,

    -- Skip Stats
    COALESCE(skip_count, 0) AS skip_count,
    skip_date AS last_skipped_at,

    -- Rating
    rating AS rating_raw,
    CASE
        WHEN rating IS NULL THEN NULL
        ELSE rating / 20
    END AS rating_stars,
    COALESCE(loved, FALSE) AS is_loved,

    -- Metadata
    date_added AS added_at,
    location AS file_path,
    snapshot_path

FROM {{ ref('bronze_itunes_library') }}
WHERE persistent_id IS NOT NULL
```

### 3.3 Gold Layer

ディメンション（マスター）とファクト（メトリクス）に分離。

#### dim_track

トラックのマスター情報（最新状態）。

```sql
-- models/gold/dim_track.sql
{{ config(materialized='view') }}

WITH latest AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY track_persistent_id
            ORDER BY snapshot_date DESC
        ) AS rn
    FROM {{ ref('silver_tracks') }}
)

SELECT
    track_persistent_id,
    itunes_track_id,
    title,
    artist_name,
    album_artist_name,
    album_name,
    genre,
    release_year,
    file_type,
    duration_ms,
    duration_min,
    disc_number,
    disc_count,
    track_number,
    track_count,
    rating_raw,
    rating_stars,
    is_loved,
    added_at
FROM latest
WHERE rn = 1
```

#### dim_artist

アーティストのマスター情報。

```sql
-- models/gold/dim_artist.sql
{{ config(materialized='view') }}

SELECT
    artist_name,
    COUNT(DISTINCT track_persistent_id) AS track_count,
    COUNT(DISTINCT album_name) AS album_count,
    SUM(duration_min) AS total_duration_min
FROM {{ ref('dim_track') }}
WHERE artist_name IS NOT NULL
GROUP BY artist_name
```

#### dim_album

アルバムのマスター情報。

```sql
-- models/gold/dim_album.sql
{{ config(materialized='view') }}

SELECT
    album_name,
    album_artist_name,
    MIN(release_year) AS release_year,
    COUNT(DISTINCT track_persistent_id) AS track_count,
    SUM(duration_min) AS total_duration_min
FROM {{ ref('dim_track') }}
WHERE album_name IS NOT NULL
GROUP BY album_name, album_artist_name
```

#### fact_play_count_snapshot

日次再生回数スナップショット（差分計算付き）。

```sql
-- models/gold/fact_play_count_snapshot.sql
{{ config(materialized='table') }}

SELECT
    track_persistent_id,
    snapshot_date,
    play_count,
    skip_count,
    last_played_at,
    duration_min,

    -- 前回スナップショットからの差分
    play_count - LAG(play_count) OVER (
        PARTITION BY track_persistent_id
        ORDER BY snapshot_date
    ) AS play_count_delta,

    skip_count - LAG(skip_count) OVER (
        PARTITION BY track_persistent_id
        ORDER BY snapshot_date
    ) AS skip_count_delta,

    -- 前回スナップショット日
    LAG(snapshot_date) OVER (
        PARTITION BY track_persistent_id
        ORDER BY snapshot_date
    ) AS prev_snapshot_date

FROM {{ ref('silver_tracks') }}
```

### 3.4 Platinum Layer

Apple Music Replay風のレポート用マート。

#### platinum_yearly_summary

年間サマリー。

```sql
-- models/platinum/platinum_yearly_summary.sql
{{ config(materialized='view') }}

SELECT
    EXTRACT(YEAR FROM f.snapshot_date) AS year,

    -- 総再生時間（分）
    SUM(
        CASE WHEN f.play_count_delta > 0
        THEN f.play_count_delta * f.duration_min
        ELSE 0 END
    ) AS total_listening_minutes,

    -- 総再生時間（時間）
    ROUND(SUM(
        CASE WHEN f.play_count_delta > 0
        THEN f.play_count_delta * f.duration_min
        ELSE 0 END
    ) / 60.0, 1) AS total_listening_hours,

    -- 総再生回数
    SUM(CASE WHEN f.play_count_delta > 0 THEN f.play_count_delta ELSE 0 END) AS total_plays,

    -- ユニークトラック数
    COUNT(DISTINCT CASE WHEN f.play_count_delta > 0 THEN f.track_persistent_id END) AS unique_tracks_played,

    -- ユニークアーティスト数
    COUNT(DISTINCT CASE WHEN f.play_count_delta > 0 THEN d.artist_name END) AS unique_artists_played

FROM {{ ref('fact_play_count_snapshot') }} f
LEFT JOIN {{ ref('dim_track') }} d
    ON f.track_persistent_id = d.track_persistent_id
GROUP BY 1
```

#### platinum_top_artists

トップアーティストランキング。

```sql
-- models/platinum/platinum_top_artists.sql
{{ config(materialized='view') }}

WITH artist_plays AS (
    SELECT
        EXTRACT(YEAR FROM f.snapshot_date) AS year,
        d.artist_name,
        SUM(CASE WHEN f.play_count_delta > 0 THEN f.play_count_delta ELSE 0 END) AS play_count,
        SUM(
            CASE WHEN f.play_count_delta > 0
            THEN f.play_count_delta * f.duration_min
            ELSE 0 END
        ) AS listening_minutes
    FROM {{ ref('fact_play_count_snapshot') }} f
    LEFT JOIN {{ ref('dim_track') }} d
        ON f.track_persistent_id = d.track_persistent_id
    WHERE d.artist_name IS NOT NULL
    GROUP BY 1, 2
)

SELECT
    year,
    artist_name,
    play_count,
    ROUND(listening_minutes, 0) AS listening_minutes,
    RANK() OVER (PARTITION BY year ORDER BY play_count DESC) AS rank
FROM artist_plays
WHERE play_count > 0
```

#### platinum_top_songs

トップソングランキング。

```sql
-- models/platinum/platinum_top_songs.sql
{{ config(materialized='view') }}

WITH song_plays AS (
    SELECT
        EXTRACT(YEAR FROM f.snapshot_date) AS year,
        d.title,
        d.artist_name,
        d.album_name,
        SUM(CASE WHEN f.play_count_delta > 0 THEN f.play_count_delta ELSE 0 END) AS play_count,
        SUM(
            CASE WHEN f.play_count_delta > 0
            THEN f.play_count_delta * f.duration_min
            ELSE 0 END
        ) AS listening_minutes
    FROM {{ ref('fact_play_count_snapshot') }} f
    LEFT JOIN {{ ref('dim_track') }} d
        ON f.track_persistent_id = d.track_persistent_id
    GROUP BY 1, 2, 3, 4
)

SELECT
    year,
    title,
    artist_name,
    album_name,
    play_count,
    ROUND(listening_minutes, 0) AS listening_minutes,
    RANK() OVER (PARTITION BY year ORDER BY play_count DESC) AS rank
FROM song_plays
WHERE play_count > 0
```

#### platinum_top_albums

トップアルバムランキング。

```sql
-- models/platinum/platinum_top_albums.sql
{{ config(materialized='view') }}

WITH album_plays AS (
    SELECT
        EXTRACT(YEAR FROM f.snapshot_date) AS year,
        d.album_name,
        COALESCE(d.album_artist_name, d.artist_name) AS artist_name,
        SUM(CASE WHEN f.play_count_delta > 0 THEN f.play_count_delta ELSE 0 END) AS play_count,
        SUM(
            CASE WHEN f.play_count_delta > 0
            THEN f.play_count_delta * f.duration_min
            ELSE 0 END
        ) AS listening_minutes
    FROM {{ ref('fact_play_count_snapshot') }} f
    LEFT JOIN {{ ref('dim_track') }} d
        ON f.track_persistent_id = d.track_persistent_id
    WHERE d.album_name IS NOT NULL
    GROUP BY 1, 2, 3
)

SELECT
    year,
    album_name,
    artist_name,
    play_count,
    ROUND(listening_minutes, 0) AS listening_minutes,
    RANK() OVER (PARTITION BY year ORDER BY play_count DESC) AS rank
FROM album_plays
WHERE play_count > 0
```

---

## 4. ERダイアグラム

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              BRONZE                                          │
├──────────────────────────────────────────────────────────────────────────────┤
│  bronze_itunes_library                                                       │
│  ┌─────────────────────────────┐                                             │
│  │ snapshot_date (PK)          │                                             │
│  │ track_id (PK)               │                                             │
│  │ persistent_id               │                                             │
│  │ name, artist, album, ...    │                                             │
│  │ play_count, skip_count      │                                             │
│  └──────────────┬──────────────┘                                             │
└─────────────────│────────────────────────────────────────────────────────────┘
                  │
                  ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                              SILVER                                          │
├──────────────────────────────────────────────────────────────────────────────┤
│  silver_tracks                                                               │
│  ┌─────────────────────────────┐                                             │
│  │ track_persistent_id (PK)    │                                             │
│  │ snapshot_date (PK)          │                                             │
│  │ title, artist_name          │                                             │
│  │ album_name, genre           │                                             │
│  │ duration_min                │                                             │
│  │ play_count, skip_count      │                                             │
│  │ rating_stars, is_loved      │                                             │
│  └──────────────┬──────────────┘                                             │
└─────────────────│────────────────────────────────────────────────────────────┘
                  │
        ┌─────────┴─────────┐
        ▼                   ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                              GOLD                                            │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐   │
│  │ dim_track       │  │ dim_artist      │  │ fact_play_count_snapshot    │   │
│  │ ─────────────── │  │ ─────────────── │  │ ─────────────────────────── │   │
│  │ track_persistent│  │ artist_name(PK) │  │ track_persistent_id (FK)    │   │
│  │ _id (PK)        │  │ track_count     │  │ snapshot_date (PK)          │   │
│  │ title           │  │ album_count     │  │ play_count                  │   │
│  │ artist_name     │  └─────────────────┘  │ play_count_delta            │   │
│  │ album_name      │                       │ skip_count                  │   │
│  │ duration_min    │  ┌─────────────────┐  │ duration_min                │   │
│  │ rating_stars    │  │ dim_album       │  └─────────────────────────────┘   │
│  └────────┬────────┘  │ ─────────────── │                                    │
│           │           │ album_name (PK) │                                    │
│           │           │ artist_name     │                                    │
│           │           │ track_count     │                                    │
│           │           └─────────────────┘                                    │
└───────────│──────────────────────────────────────────────────────────────────┘
            │
            ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                              PLATINUM                                        │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────┐  ┌────────────────────────┐                      │
│  │ platinum_yearly_summary│  │ platinum_top_artists   │                      │
│  │ ────────────────────── │  │ ────────────────────── │                      │
│  │ year                   │  │ year                   │                      │
│  │ total_listening_minutes│  │ artist_name            │                      │
│  │ total_plays            │  │ play_count             │                      │
│  │ unique_tracks_played   │  │ rank                   │                      │
│  └────────────────────────┘  └────────────────────────┘                      │
│                                                                              │
│  ┌────────────────────────┐  ┌────────────────────────┐                      │
│  │ platinum_top_songs     │  │ platinum_top_albums    │                      │
│  │ ────────────────────── │  │ ────────────────────── │                      │
│  │ year                   │  │ year                   │                      │
│  │ title, artist_name     │  │ album_name             │                      │
│  │ play_count             │  │ artist_name            │                      │
│  │ rank                   │  │ play_count, rank       │                      │
│  └────────────────────────┘  └────────────────────────┘                      │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. データ取り込みパイプライン

### 5.1 TimeMachineからのデータ抽出スクリプト

```python
#!/usr/bin/env python3
"""
extract_music_snapshots.py
TimeMachineからMusicライブラリのスナップショットを抽出
"""

import os
import plistlib
import duckdb
from pathlib import Path
from datetime import datetime

# 設定
TIMEMACHINE_VOLUME = "/Volumes/TimeMachine"
MAC_NAME = "your-mac-name"
USERNAME = "your-username"
OUTPUT_DB = "music_replay.duckdb"

# ライブラリパス候補
LIBRARY_PATHS = [
    f"Users/{USERNAME}/Music/Music/Music Library.xml",
    f"Users/{USERNAME}/Music/iTunes/iTunes Music Library.xml",
    f"Users/{USERNAME}/Music/iTunes/iTunes Library.xml",
]

def find_backups(timemachine_path: str, mac_name: str) -> list[tuple[datetime, str]]:
    """TimeMachineのバックアップ一覧を取得"""
    backup_base = Path(timemachine_path) / "Backups.backupdb" / mac_name
    backups = []

    for backup_dir in backup_base.iterdir():
        if backup_dir.name == "Latest":
            continue
        try:
            date_str = backup_dir.name
            backup_date = datetime.strptime(date_str, "%Y-%m-%d-%H%M%S")
            backups.append((backup_date, str(backup_dir)))
        except ValueError:
            continue

    return sorted(backups)

def find_library_file(backup_path: str, library_paths: list[str]) -> str | None:
    """バックアップ内のライブラリファイルを探す"""
    for volume in ["Macintosh HD", "Macintosh HD - Data", "Data"]:
        for lib_path in library_paths:
            full_path = Path(backup_path) / volume / lib_path
            if full_path.exists():
                return str(full_path)
    return None

def parse_music_library(library_path: str) -> list[dict]:
    """MusicライブラリXMLをパース"""
    with open(library_path, 'rb') as f:
        plist = plistlib.load(f)

    tracks = []
    for track_id, track_data in plist.get('Tracks', {}).items():
        tracks.append({
            'track_id': int(track_id),
            'name': track_data.get('Name'),
            'artist': track_data.get('Artist'),
            'album_artist': track_data.get('Album Artist'),
            'album': track_data.get('Album'),
            'genre': track_data.get('Genre'),
            'kind': track_data.get('Kind'),
            'total_time': track_data.get('Total Time'),
            'disc_number': track_data.get('Disc Number'),
            'disc_count': track_data.get('Disc Count'),
            'track_number': track_data.get('Track Number'),
            'track_count': track_data.get('Track Count'),
            'year': track_data.get('Year'),
            'date_added': track_data.get('Date Added'),
            'play_count': track_data.get('Play Count', 0),
            'play_date': track_data.get('Play Date'),
            'play_date_utc': track_data.get('Play Date UTC'),
            'skip_count': track_data.get('Skip Count', 0),
            'skip_date': track_data.get('Skip Date'),
            'rating': track_data.get('Rating'),
            'loved': track_data.get('Loved', False),
            'persistent_id': track_data.get('Persistent ID'),
            'location': track_data.get('Location'),
        })

    return tracks

def load_to_duckdb(db_path: str, snapshot_date: datetime,
                   snapshot_path: str, tracks: list[dict]):
    """DuckDBにロード"""
    con = duckdb.connect(db_path)

    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_itunes_library (
            snapshot_date DATE NOT NULL,
            snapshot_path VARCHAR NOT NULL,
            track_id INTEGER NOT NULL,
            name VARCHAR,
            artist VARCHAR,
            album_artist VARCHAR,
            album VARCHAR,
            genre VARCHAR,
            kind VARCHAR,
            total_time INTEGER,
            disc_number INTEGER,
            disc_count INTEGER,
            track_number INTEGER,
            track_count INTEGER,
            year INTEGER,
            date_added TIMESTAMP,
            play_count INTEGER,
            play_date BIGINT,
            play_date_utc TIMESTAMP,
            skip_count INTEGER,
            skip_date TIMESTAMP,
            rating INTEGER,
            loved BOOLEAN,
            persistent_id VARCHAR,
            location VARCHAR,
            PRIMARY KEY (snapshot_date, track_id)
        )
    """)

    for track in tracks:
        con.execute("""
            INSERT OR REPLACE INTO raw_itunes_library VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        """, [
            snapshot_date.date(),
            snapshot_path,
            track['track_id'],
            track['name'],
            track['artist'],
            track['album_artist'],
            track['album'],
            track['genre'],
            track['kind'],
            track['total_time'],
            track['disc_number'],
            track['disc_count'],
            track['track_number'],
            track['track_count'],
            track['year'],
            track['date_added'],
            track['play_count'],
            track['play_date'],
            track['play_date_utc'],
            track['skip_count'],
            track['skip_date'],
            track['rating'],
            track['loved'],
            track['persistent_id'],
            track['location'],
        ])

    con.close()
    print(f"Loaded {len(tracks)} tracks from {snapshot_date.date()}")

def main():
    backups = find_backups(TIMEMACHINE_VOLUME, MAC_NAME)
    print(f"Found {len(backups)} backups")

    for backup_date, backup_path in backups:
        library_path = find_library_file(backup_path, LIBRARY_PATHS)
        if not library_path:
            print(f"No library found in {backup_date.date()}")
            continue

        tracks = parse_music_library(library_path)
        load_to_duckdb(OUTPUT_DB, backup_date, library_path, tracks)

if __name__ == "__main__":
    main()
```

### 5.2 dbt プロジェクト構成

```yaml
# dbt_project.yml
name: 'music_replay_warehouse'
version: '1.0.0'
profile: 'music_replay_warehouse'

model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]

models:
  music_replay_warehouse:
    bronze:
      +materialized: table
    silver:
      +materialized: table
    gold:
      +materialized: view
    platinum:
      +materialized: view
```

```yaml
# profiles.yml
music_replay_warehouse:
  outputs:
    dev:
      type: duckdb
      path: 'music_replay.duckdb'
  target: dev
```

```yaml
# models/sources.yml
version: 2

sources:
  - name: itunes
    schema: main
    tables:
      - name: raw_library
        identifier: raw_itunes_library
        description: TimeMachineからロードされた生のiTunesライブラリデータ
```

---

## 6. Replay風レポートクエリ例

### 6.1 年間サマリー

```sql
-- 2025年の振り返り
SELECT
    year,
    total_listening_minutes,
    total_listening_hours,
    total_plays,
    unique_tracks_played,
    unique_artists_played
FROM platinum_yearly_summary
WHERE year = 2025;
```

**出力例:**
```
year | total_listening_minutes | total_listening_hours | total_plays | unique_tracks | unique_artists
2025 | 40308                   | 671.8                 | 12543       | 1823          | 245
```

### 6.2 トップアーティスト TOP 3

```sql
SELECT rank, artist_name, play_count, listening_minutes
FROM platinum_top_artists
WHERE year = 2025 AND rank <= 3
ORDER BY rank;
```

**出力例:**
```
rank | artist_name      | play_count | listening_minutes
1    | Mrs. GREEN APPLE | 523        | 1842
2    | aiko             | 412        | 1534
3    | My Hair is Bad   | 387        | 1421
```

### 6.3 トップソング TOP 3

```sql
SELECT rank, title, artist_name, play_count
FROM platinum_top_songs
WHERE year = 2025 AND rank <= 3
ORDER BY rank;
```

**出力例:**
```
rank | title       | artist_name      | play_count
1    | 倍倍FIGHT!  | CANDY TUNE       | 89
2    | StaRt       | Mrs. GREEN APPLE | 76
3    | ライラック  | Mrs. GREEN APPLE | 71
```

### 6.4 トップアルバム TOP 3

```sql
SELECT rank, album_name, artist_name, play_count
FROM platinum_top_albums
WHERE year = 2025 AND rank <= 3
ORDER BY rank;
```

**出力例:**
```
rank | album_name           | artist_name    | play_count
1    | RED                  | Ryosuke Yamada | 234
2    | Doo-Wops & Hooligans | Bruno Mars     | 198
3    | 「untitled」         | 嵐             | 187
```

---

## 7. 運用考慮事項

### 7.1 スナップショット頻度
- TimeMachineのバックアップ頻度に依存（通常1時間ごと）
- 日次での集約で十分な精度が得られる

### 7.2 データ量見積もり
| 項目 | 見積もり |
|------|---------|
| 1スナップショットあたりのトラック数 | 5,000〜50,000曲 |
| 1年分のスナップショット（日次） | 約365 × 50,000 × 500B ≈ 9GB |

### 7.3 注意点
- `persistent_id`がトラックの一意識別子
- `play_count`は累積値なので、差分計算が必要
- 曲の削除・再追加で`persistent_id`が変わる可能性あり

---

## 8. 今後の拡張案

- 月別トレンドグラフの可視化（`platinum_monthly_trend`）
- ジャンル別分析（`platinum_top_genres`）
- 時間帯別リスニングパターン
- Streamlit/Evidenceでのダッシュボード化
