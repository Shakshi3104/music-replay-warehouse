#!/bin/bash
# TimeMachineのライブラリを偽装して読み込むテストスクリプト
#
# 使い方:
#   ./test_timemachine_library.sh "/Volumes/.timemachine/[UUID]/[日付].backup/[日付].backup/Data/Users/[ユーザー]/Music/Music/Music Library.musiclibrary"

set -e

if [ -z "$1" ]; then
    echo "使い方: $0 <TimeMachineのライブラリパス>"
    echo ""
    echo "例:"
    echo "  $0 \"/Volumes/.timemachine/919E623E-E870-4B56-9536-2E43F7FD72CC/2025-01-03-095001.backup/2025-01-03-095001.backup/Data/Users/user/Music/Music/Music Library.musiclibrary\""
    exit 1
fi

LIBRARY_PATH="$1"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ライブラリパスの存在確認
if [ ! -d "$LIBRARY_PATH" ]; then
    echo "エラー: ライブラリが見つかりません: $LIBRARY_PATH"
    exit 1
fi

# Music.appが起動中か確認
if pgrep -x Music > /dev/null; then
    echo "エラー: Music.appが起動中です。終了してから再実行してください。"
    exit 1
fi

echo "=== Music.app設定のバックアップ ==="
BACKUP_FILE="/tmp/Music_backup_$(date +%Y%m%d_%H%M%S).plist"
defaults export com.apple.Music "$BACKUP_FILE"
echo "バックアップ: $BACKUP_FILE"

echo ""
echo "=== 設定を変更 ==="
# URLエンコード
ENCODED_PATH=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$LIBRARY_PATH', safe='/'))")
defaults write com.apple.Music library-url "file://$ENCODED_PATH/"
defaults delete com.apple.Music library-bookmark 2>/dev/null || true

# AMPLibraryAgentを再起動
killall -9 AMPLibraryAgent 2>/dev/null || true
sleep 1

echo "library-url: $(defaults read com.apple.Music library-url)"

echo ""
echo "=== Swiftスクリプトをコンパイル ==="
SWIFT_FILE="$SCRIPT_DIR/test_itlibrary.swift"
BINARY_FILE="/tmp/test_itlibrary"

if [ ! -f "$SWIFT_FILE" ]; then
    echo "エラー: $SWIFT_FILE が見つかりません"
    defaults import com.apple.Music "$BACKUP_FILE"
    exit 1
fi

swiftc "$SWIFT_FILE" -o "$BINARY_FILE" -framework iTunesLibrary

echo ""
echo "=== ライブラリ読み込みテスト ==="
"$BINARY_FILE"

echo ""
echo "=== 設定を復元 ==="
defaults import com.apple.Music "$BACKUP_FILE"
echo "復元完了"

echo ""
echo "=== 完了 ==="
