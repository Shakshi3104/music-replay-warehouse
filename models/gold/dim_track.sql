{{ config(materialized='view') }}

WITH
-- Import CTE
silver_tracks AS (
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
        added_at,
        snapshot_date
    FROM {{ ref('silver_tracks') }}
),

-- Functional CTE
ranked_tracks AS (
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
        added_at,
        ROW_NUMBER() OVER (
            PARTITION BY track_persistent_id
            ORDER BY snapshot_date DESC
        ) AS rn
    FROM silver_tracks
),

latest_tracks AS (
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
    FROM ranked_tracks
    WHERE rn = 1
),

final AS (
    SELECT * FROM latest_tracks
)

SELECT * FROM final
