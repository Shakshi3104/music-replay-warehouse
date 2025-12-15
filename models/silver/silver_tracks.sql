{{ config(materialized='table') }}

SELECT
    -- Keys
    persistent_id AS track_persistent_id,
    snapshot_date,

    -- Track Info
    track_id AS itunes_track_id,
    name AS title,
    artist AS artist_name,
    album_artist AS album_artist_name,
    album AS album_name,
    genre,
    year AS release_year,
    kind AS file_type,

    -- Duration
    total_time AS duration_ms,
    ROUND(total_time / 1000.0 / 60.0, 2) AS duration_min,

    -- Album Info
    disc_number,
    disc_count,
    track_number,
    track_count,

    -- Play Stats
    COALESCE(play_count, 0) AS play_count,
    play_date AS last_played_at,
    play_date_utc AS last_played_at_utc,

    -- Skip Stats
    COALESCE(skip_count, 0) AS skip_count,
    skip_date AS last_skipped_at,

    -- Rating
    rating AS rating_raw,
    CASE
        WHEN rating IS NULL THEN NULL
        ELSE rating / 20
    END AS rating_stars,
    COALESCE(loved, FALSE) AS is_loved,

    -- Metadata
    date_added AS added_at,
    location AS file_path,
    snapshot_path

FROM {{ ref('bronze_itunes_library') }}
WHERE persistent_id IS NOT NULL
