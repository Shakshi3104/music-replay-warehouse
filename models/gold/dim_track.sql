{{ config(materialized='view') }}

WITH latest AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY track_persistent_id
            ORDER BY snapshot_date DESC
        ) AS rn
    FROM {{ ref('silver_tracks') }}
)

SELECT
    track_persistent_id,
    itunes_track_id,
    title,
    artist_name,
    album_artist_name,
    album_name,
    genre,
    release_year,
    file_type,
    duration_ms,
    duration_min,
    disc_number,
    disc_count,
    track_number,
    track_count,
    rating_raw,
    rating_stars,
    is_loved,
    added_at
FROM latest
WHERE rn = 1
