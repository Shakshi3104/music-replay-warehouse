#!/usr/bin/env python3
"""
load_xml_snapshot.py
iTunes/Music Library XMLファイルをDuckDBにロード
"""

import sys
import plistlib
import duckdb
from pathlib import Path
from datetime import datetime
import argparse


def parse_music_library(library_path: str) -> tuple[datetime, list[dict]]:
    """MusicライブラリXMLをパース"""
    try:
        with open(library_path, 'rb') as f:
            plist = plistlib.load(f)
    except Exception as e:
        print(f"Error parsing plist file {library_path}: {e}")
        return None, []

    # スナップショット日時を取得
    snapshot_date = plist.get('Date')
    if not snapshot_date:
        # ファイル名から日付を推測
        print("Warning: XMLにDate要素がありません。ファイルの更新日時を使用します。")
        snapshot_date = datetime.fromtimestamp(Path(library_path).stat().st_mtime)

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
            'play_count': track_data.get('Play Count'),
            'play_date': track_data.get('Play Date'),
            'play_date_utc': track_data.get('Play Date UTC'),
            'skip_count': track_data.get('Skip Count'),
            'skip_date': track_data.get('Skip Date'),
            'rating': track_data.get('Rating'),
            'loved': track_data.get('Loved'),
            'persistent_id': track_data.get('Persistent ID'),
            'location': track_data.get('Location'),
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

    for track in tracks:
        con.execute("""
            INSERT INTO raw_itunes_library VALUES (
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
    parser = argparse.ArgumentParser(description='XMLスナップショットをDuckDBにロード')
    parser.add_argument('xml_path', help='XMLファイルのパス')
    parser.add_argument('--db', default='music_replay.duckdb', help='DuckDBファイルのパス')
    args = parser.parse_args()

    xml_path = Path(args.xml_path)
    if not xml_path.exists():
        print(f"エラー: ファイルが見つかりません: {xml_path}")
        sys.exit(1)

    # DBファイルのパスを解決
    db_path = Path(args.db)
    if not db_path.is_absolute():
        # プロジェクトルートに作成
        db_path = Path(__file__).parent.parent / args.db

    print(f"XMLファイル: {xml_path}")
    print(f"DBファイル: {db_path}")

    snapshot_date, tracks = parse_music_library(str(xml_path))
    if snapshot_date and tracks:
        load_to_duckdb(str(db_path), snapshot_date, str(xml_path), tracks)
    else:
        print("エラー: XMLのパースに失敗しました")
        sys.exit(1)


if __name__ == "__main__":
    main()
