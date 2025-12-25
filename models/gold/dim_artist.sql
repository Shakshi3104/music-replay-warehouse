{{ config(materialized='view') }}

WITH
-- Import CTE
tracks AS (
    SELECT
        artist_name,
        track_persistent_id,
        album_name,
        duration_min
    FROM {{ ref('dim_track') }}
    WHERE artist_name IS NOT NULL
),

-- Functional CTE
artist_summary AS (
    SELECT
        artist_name,
        COUNT(DISTINCT track_persistent_id) AS track_count,
        COUNT(DISTINCT album_name) AS album_count,
        SUM(duration_min) AS total_duration_min
    FROM tracks
    GROUP BY artist_name
),

final AS (
    SELECT * FROM artist_summary
)

SELECT * FROM final
