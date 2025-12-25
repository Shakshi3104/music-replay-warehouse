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
        artist_name
    FROM {{ ref('dim_track') }}
    WHERE artist_name IS NOT NULL
),

-- Functional CTEs
joined_data AS (
    SELECT
        f.track_persistent_id,
        f.play_count_delta,
        f.duration_min,
        d.artist_name
    FROM fact_snapshots AS f
    LEFT JOIN dim_tracks AS d
        ON f.track_persistent_id = d.track_persistent_id
    WHERE d.artist_name IS NOT NULL
),

artist_plays AS (
    SELECT
        artist_name,
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
        ) AS listening_minutes,
        COUNT(DISTINCT track_persistent_id) AS unique_tracks
    FROM joined_data
    GROUP BY artist_name
),

ranked_artists AS (
    SELECT
        artist_name,
        play_count,
        ROUND(listening_minutes, 0) AS listening_minutes,
        unique_tracks,
        RANK() OVER (ORDER BY play_count DESC) AS rank
    FROM artist_plays
    WHERE play_count > 0
),

final AS (
    SELECT * FROM ranked_artists
)

SELECT * FROM final
ORDER BY rank
