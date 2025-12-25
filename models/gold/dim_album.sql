{{ config(materialized='view') }}

WITH
-- Import CTE
tracks AS (
    SELECT
        album_name,
        album_artist_name,
        release_year,
        track_persistent_id,
        duration_min
    FROM {{ ref('dim_track') }}
    WHERE album_name IS NOT NULL
),

-- Functional CTE
album_summary AS (
    SELECT
        album_name,
        album_artist_name,
        MIN(release_year) AS release_year,
        COUNT(DISTINCT track_persistent_id) AS track_count,
        SUM(duration_min) AS total_duration_min
    FROM tracks
    GROUP BY
        album_name,
        album_artist_name
),

final AS (
    SELECT * FROM album_summary
)

SELECT * FROM final
