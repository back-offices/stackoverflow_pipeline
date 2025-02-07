
-- Select source data from ephemeral model
WITH badges_base AS (
    SELECT *
    FROM {{ ref('extr_badges') }}
),

-- Deduplicate based on id
badges_dedup AS (
    SELECT * EXCEPT (row_num)
    FROM (
        SELECT
            *,
            ROW_NUMBER()
                OVER (PARTITION BY badge_id ORDER BY source_partition_timestamp DESC)
                AS row_num
        FROM badges_base
    )
    WHERE row_num = 1
)

-- Final select and add insertion_timestamp
SELECT
    *,
    CURRENT_TIMESTAMP() AS insertion_timestamp
FROM badges_dedup