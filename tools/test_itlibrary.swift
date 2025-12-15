#!/usr/bin/env swift
import iTunesLibrary
import Foundation

do {
    let library = try ITLibrary(apiVersion: "1.1")

    print("=== ライブラリ情報 ===")
    print("トラック数: \(library.allMediaItems.count)")
    print("プレイリスト数: \(library.allPlaylists.count)")
    print("メディアフォルダ: \(library.mediaFolderLocation?.path ?? "nil")")

    print("\n=== 最初の10曲 ===")
    for (index, item) in library.allMediaItems.prefix(10).enumerated() {
        let playCount = item.playCount
        print("\(index + 1). \(item.title) - \(item.artist?.name ?? "Unknown") [再生回数: \(playCount)]")
    }

    print("\n=== サマリー ===")
    let totalPlayCount = library.allMediaItems.reduce(0) { $0 + $1.playCount }
    print("総再生回数: \(totalPlayCount)")

} catch {
    print("エラー: \(error)")
}
