{{ config(materialized='view') }}

SELECT
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
    COUNT(DISTINCT CASE WHEN f.play_count_delta > 0 THEN d.artist_name END) AS unique_artists_played,

    -- ユニークアルバム数
    COUNT(DISTINCT CASE WHEN f.play_count_delta > 0 THEN d.album_name END) AS unique_albums_played

FROM {{ ref('fact_play_count_snapshot') }} f
LEFT JOIN {{ ref('dim_track') }} d
    ON f.track_persistent_id = d.track_persistent_id
WHERE f.play_count_delta IS NOT NULL
