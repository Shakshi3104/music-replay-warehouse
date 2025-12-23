{{ config(materialized='view') }}

WITH song_plays AS (
    SELECT
        f.track_persistent_id,
        d.title,
        d.artist_name,
        d.album_name,
        d.duration_min,
        SUM(CASE WHEN f.play_count_delta > 0 THEN f.play_count_delta ELSE 0 END) AS play_count,
        SUM(
            CASE WHEN f.play_count_delta > 0
            THEN f.play_count_delta * f.duration_min
            ELSE 0 END
        ) AS listening_minutes
    FROM {{ ref('fact_play_count_snapshot') }} f
    LEFT JOIN {{ ref('dim_track') }} d
        ON f.track_persistent_id = d.track_persistent_id
    WHERE f.play_count_delta IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    title,
    artist_name,
    album_name,
    duration_min,
    play_count,
    ROUND(listening_minutes, 0) AS listening_minutes,
    RANK() OVER (ORDER BY play_count DESC) AS rank
FROM song_plays
WHERE play_count > 0
ORDER BY rank
