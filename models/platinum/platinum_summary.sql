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
        artist_name,
        album_name
    FROM {{ ref('dim_track') }}
),

-- Functional CTE
joined_data AS (
    SELECT
        f.track_persistent_id,
        f.play_count_delta,
        f.duration_min,
        d.artist_name,
        d.album_name
    FROM fact_snapshots AS f
    LEFT JOIN dim_tracks AS d
        ON f.track_persistent_id = d.track_persistent_id
),

summary AS (
    SELECT
        -- 総再生時間（分）
        SUM(
            CASE
                WHEN play_count_delta > 0 THEN play_count_delta * duration_min
                ELSE 0
            END
        ) AS total_listening_minutes,

        -- 総再生時間（時間）
        ROUND(
            SUM(
                CASE
                    WHEN play_count_delta > 0 THEN play_count_delta * duration_min
                    ELSE 0
                END
            ) / 60.0,
            1
        ) AS total_listening_hours,

        -- 総再生回数
        SUM(
            CASE
                WHEN play_count_delta > 0 THEN play_count_delta
                ELSE 0
            END
        ) AS total_plays,

        -- ユニークトラック数
        COUNT(DISTINCT
            CASE
                WHEN play_count_delta > 0 THEN track_persistent_id
                ELSE NULL
            END
        ) AS unique_tracks_played,

        -- ユニークアーティスト数
        COUNT(DISTINCT
            CASE
                WHEN play_count_delta > 0 THEN artist_name
                ELSE NULL
            END
        ) AS unique_artists_played,

        -- ユニークアルバム数
        COUNT(DISTINCT
            CASE
                WHEN play_count_delta > 0 THEN album_name
                ELSE NULL
            END
        ) AS unique_albums_played
    FROM joined_data
),

final AS (
    SELECT * FROM summary
)

SELECT * FROM final
