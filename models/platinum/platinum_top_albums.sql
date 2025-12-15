{{ config(materialized='view') }}

WITH album_plays AS (
    SELECT
        d.album_name,
        COALESCE(d.album_artist_name, d.artist_name) AS artist_name,
        SUM(CASE WHEN f.play_count_delta > 0 THEN f.play_count_delta ELSE 0 END) AS play_count,
        SUM(
            CASE WHEN f.play_count_delta > 0
            THEN f.play_count_delta * f.duration_min
            ELSE 0 END
        ) AS listening_minutes,
        COUNT(DISTINCT f.track_persistent_id) AS unique_tracks
    FROM {{ ref('fact_play_count_snapshot') }} f
    LEFT JOIN {{ ref('dim_track') }} d
        ON f.track_persistent_id = d.track_persistent_id
    WHERE d.album_name IS NOT NULL
      AND f.play_count_delta IS NOT NULL
    GROUP BY 1, 2
)

SELECT
    album_name,
    artist_name,
    play_count,
    ROUND(listening_minutes, 0) AS listening_minutes,
    unique_tracks,
    RANK() OVER (ORDER BY play_count DESC) AS rank
FROM album_plays
WHERE play_count > 0
ORDER BY rank
