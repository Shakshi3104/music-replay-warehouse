{{ config(materialized='view') }}

SELECT
    album_name,
    album_artist_name,
    MIN(release_year) AS release_year,
    COUNT(DISTINCT track_persistent_id) AS track_count,
    SUM(duration_min) AS total_duration_min
FROM {{ ref('dim_track') }}
WHERE album_name IS NOT NULL
GROUP BY album_name, album_artist_name
