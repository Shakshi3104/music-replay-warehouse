#!/usr/bin/env python3
"""
extract_music_snapshots.py
TimeMachineからMusicライブラリのスナップショットを抽出
"""

import os
import sys
import plistlib
import duckdb
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# .envファイルから環境変数を読み込む
load_dotenv()

# --- 環境変数から設定を読み込む ---
TIMEMACHINE_VOLUME = os.getenv("TIMEMACHINE_VOLUME")
MAC_NAME = os.getenv("MAC_NAME")
USERNAME = os.getenv("USERNAME")
OUTPUT_DB = "music_replay.duckdb"
# ----------------------------------------------------

# --- 設定のバリデーション ---
if not TIMEMACHINE_VOLUME:
    print("エラー: 環境変数 TIMEMACHINE_VOLUME が設定されていません。")
    print(".envファイルに 'TIMEMACHINE_VOLUME=\"/Volumes/YourTimeMachineDisk\"' のように記述してください。")
    sys.exit(1)
if not MAC_NAME or not USERNAME:
    print("エラー: 環境変数 MAC_NAME または USERNAME が見つかりません。")
    print(".envファイルが正しく設定されているか確認してください。")
    sys.exit(1)
# ---------------------------

# ライブラリパス候補
LIBRARY_PATHS = [
    f"Users/{USERNAME}/Music/Music/Music Library.xml",
    f"Users/{USERNAME}/Music/iTunes/iTunes Music Library.xml",
    f"Users/{USERNAME}/Music/iTunes/iTunes Library.xml",
]

def find_backups(timemachine_path: str, mac_name: str) -> list[tuple[datetime, str]]:
    """TimeMachineのバックアップ一覧を取得"""
    backup_base = Path(timemachine_path) / "Backups.backupdb" / mac_name
    if not backup_base.exists():
        print(f"エラー: バックアップディレクトリが見つかりません: {backup_base}")
        print(".envファイルの TIMEMACHINE_VOLUME と MAC_NAME の設定を確認してください。")
        return []
    
    backups = []
    for backup_dir in backup_base.iterdir():
        if backup_dir.name == "Latest":
            continue
        try:
            # フォルダ名からハイフンを取り除いてパース
            date_str = backup_dir.name.replace("-", "")
            # タイムゾーンを考慮しないnaiveなdatetimeとしてパース
            backup_date = datetime.strptime(date_str, "%Y%m%d%H%M%S")
            backups.append((backup_date, str(backup_dir)))
        except ValueError:
            # "YYYY-MM-DD-HHMMSS.local" のような形式に対応
            try:
                date_str = backup_dir.name.split('.')[0]
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
    try:
        with open(library_path, 'rb') as f:
            plist = plistlib.load(f)
    except Exception as e:
        print(f"Error parsing plist file {library_path}: {e}")
        return []

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

    return tracks

def load_to_duckdb(db_path: str, snapshot_date: datetime,
                   snapshot_path: str, tracks: list[dict]):
    """DuckDBにロード"""
    if not tracks:
        return
        
    con = duckdb.connect(db_path)

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
    # DuckDBファイルをプロジェクトルートに作成
    db_file = Path(__file__).parent.parent / OUTPUT_DB
    
    backups = find_backups(TIMEMACHINE_VOLUME, MAC_NAME)
    if not backups:
        return
        
    print(f"Found {len(backups)} backups")

    for backup_date, backup_path in backups:
        print(f"\nProcessing backup from {backup_date}...")
        library_path = find_library_file(backup_path, LIBRARY_PATHS)
        if not library_path:
            print(f"-> No library file found in this backup.")
            continue
        
        print(f"-> Found library file: {library_path}")
        tracks = parse_music_library(library_path)
        
        if tracks:
            load_to_duckdb(str(db_file), backup_date, library_path, tracks)

if __name__ == "__main__":
    main()
