#!/usr/bin/env python3
"""
load_csv_snapshot.py
music-library-exporter-swiftが出力したCSVファイルをDuckDBにロード
"""

import sys
import csv
import duckdb
from pathlib import Path
from datetime import datetime
import argparse


def parse_csv_library(csv_path: str) -> tuple[datetime, list[dict]]:
    """CSVファイルをパース"""
    tracks = []
    snapshot_date = None

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # 最初の行からスナップショット日時を取得
            if snapshot_date is None:
                try:
                    snapshot_date = datetime.fromisoformat(row['snapshot_date'].replace('Z', '+00:00'))
                except (KeyError, ValueError):
                    print("Warning: snapshot_dateのパースに失敗しました。現在時刻を使用します。")
                    snapshot_date = datetime.now()

            tracks.append({
                'persistent_id': row.get('persistent_id', ''),
                'name': row.get('title', ''),
                'artist': row.get('artist', '') or None,
                'album_artist': row.get('album_artist', '') or None,
                'album': row.get('album', '') or None,
                'genre': row.get('genre', '') or None,
                'kind': row.get('kind', '') or None,
                'total_time': int(row['total_time']) if row.get('total_time') else None,
                'disc_number': int(row['disc_number']) if row.get('disc_number') else None,
                'disc_count': int(row['disc_count']) if row.get('disc_count') else None,
                'track_number': int(row['track_number']) if row.get('track_number') else None,
                'track_count': int(row['track_count']) if row.get('track_count') else None,
                'year': int(row['year']) if row.get('year') else None,
                'date_added': datetime.fromisoformat(row['date_added'].replace('Z', '+00:00')) if row.get('date_added') else None,
                'play_count': int(row['play_count']) if row.get('play_count') else 0,
                'play_date_utc': datetime.fromisoformat(row['last_played_date'].replace('Z', '+00:00')) if row.get('last_played_date') else None,
                'skip_count': int(row['skip_count']) if row.get('skip_count') else 0,
                'skip_date': datetime.fromisoformat(row['skip_date'].replace('Z', '+00:00')) if row.get('skip_date') else None,
                'rating': int(row['rating']) if row.get('rating') else None,
                'loved': row.get('loved', '').lower() == 'true',
                'location': row.get('location', '') or None,
            })

    return snapshot_date, tracks


def create_table_if_not_exists(con: duckdb.DuckDBPyConnection):
    """テーブルを作成"""
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_itunes_library (
            snapshot_date DATE NOT NULL,
            snapshot_path VARCHAR NOT NULL,
            track_id BIGINT NOT NULL,
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


def load_to_duckdb(db_path: str, snapshot_date: datetime,
                   snapshot_path: str, tracks: list[dict]):
    """DuckDBにロード"""
    if not tracks:
        print("No tracks to load.")
        return

    con = duckdb.connect(db_path)
    create_table_if_not_exists(con)

    # 既存のデータを削除してから挿入する
    con.execute("DELETE FROM raw_itunes_library WHERE snapshot_date = ?", [snapshot_date.date()])

    # CSVにはtrack_idがないので、persistent_idのハッシュを使用
    for i, track in enumerate(tracks):
        # persistent_idから数値IDを生成（16進数の下位8桁を整数化）
        persistent_id = track['persistent_id']
        try:
            track_id = int(persistent_id[-8:], 16) if persistent_id else i
        except ValueError:
            track_id = i

        con.execute("""
            INSERT INTO raw_itunes_library VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        """, [
            snapshot_date.date(),
            snapshot_path,
            track_id,
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
            None,  # play_date (CSVには含まれない)
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
    parser = argparse.ArgumentParser(description='CSVスナップショットをDuckDBにロード')
    parser.add_argument('csv_path', help='CSVファイルのパス')
    parser.add_argument('--db', default='music_replay.duckdb', help='DuckDBファイルのパス')
    args = parser.parse_args()

    csv_path = Path(args.csv_path)
    if not csv_path.exists():
        print(f"エラー: ファイルが見つかりません: {csv_path}")
        sys.exit(1)

    # DBファイルのパスを解決
    db_path = Path(args.db)
    if not db_path.is_absolute():
        # プロジェクトルートに作成
        db_path = Path(__file__).parent.parent / args.db

    print(f"CSVファイル: {csv_path}")
    print(f"DBファイル: {db_path}")

    snapshot_date, tracks = parse_csv_library(str(csv_path))
    if snapshot_date and tracks:
        load_to_duckdb(str(db_path), snapshot_date, str(csv_path), tracks)
    else:
        print("エラー: CSVのパースに失敗しました")
        sys.exit(1)


if __name__ == "__main__":
    main()
