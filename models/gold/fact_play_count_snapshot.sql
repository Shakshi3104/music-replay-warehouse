{{ config(materialized='table') }}

SELECT
    track_persistent_id,
    snapshot_date,
    play_count,
    skip_count,
    last_played_at_utc,
    duration_min,

    -- 前回スナップショットからの差分
    play_count - LAG(play_count) OVER (
        PARTITION BY track_persistent_id
        ORDER BY snapshot_date
    ) AS play_count_delta,

    skip_count - LAG(skip_count) OVER (
        PARTITION BY track_persistent_id
        ORDER BY snapshot_date
    ) AS skip_count_delta,

    -- 前回スナップショット日
    LAG(snapshot_date) OVER (
        PARTITION BY track_persistent_id
        ORDER BY snapshot_date
    ) AS prev_snapshot_date

FROM {{ ref('silver_tracks') }}
