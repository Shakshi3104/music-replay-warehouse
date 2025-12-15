{{ config(materialized='view') }}

SELECT
    artist_name,
    COUNT(DISTINCT track_persistent_id) AS track_count,
    COUNT(DISTINCT album_name) AS album_count,
    SUM(duration_min) AS total_duration_min
FROM {{ ref('dim_track') }}
WHERE artist_name IS NOT NULL
GROUP BY artist_name
