{{ config(materialized='table') }}

SELECT
    snapshot_date,
    snapshot_path,
    track_id,
    name,
    artist,
    album_artist,
    album,
    genre,
    kind,
    total_time,
    disc_number,
    disc_count,
    track_number,
    track_count,
    year,
    date_added,
    play_count,
    play_date,
    play_date_utc,
    skip_count,
    skip_date,
    rating,
    loved,
    persistent_id,
    location
FROM {{ source('itunes', 'raw_itunes_library') }}
