{{ config(materialized='view') }}

WITH
-- Import CTEs
fact_snapshots AS (
    SELECT
        track_persistent_id,
        play_count_delta,
        duration_min
    FROM {{ ref('fact_play_count_snapshot') }}
    WHERE play_count_delta IS NOT NULL
),

dim_tracks AS (
    SELECT
        track_persistent_id,
        title,
        artist_name,
        album_name,
        duration_min
    FROM {{ ref('dim_track') }}
),

-- Functional CTEs
joined_data AS (
    SELECT
        f.track_persistent_id,
        f.play_count_delta,
        d.title,
        d.artist_name,
        d.album_name,
        d.duration_min
    FROM fact_snapshots AS f
    LEFT JOIN dim_tracks AS d
        ON f.track_persistent_id = d.track_persistent_id
),

song_plays AS (
    SELECT
        track_persistent_id,
        title,
        artist_name,
        album_name,
        duration_min,
        SUM(
            CASE
                WHEN play_count_delta > 0 THEN play_count_delta
                ELSE 0
            END
        ) AS play_count,
        SUM(
            CASE
                WHEN play_count_delta > 0 THEN play_count_delta * duration_min
                ELSE 0
            END
        ) AS listening_minutes
    FROM joined_data
    GROUP BY
        track_persistent_id,
        title,
        artist_name,
        album_name,
        duration_min
),

ranked_songs AS (
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
),

final AS (
    SELECT * FROM ranked_songs
)

SELECT * FROM final
ORDER BY rank
