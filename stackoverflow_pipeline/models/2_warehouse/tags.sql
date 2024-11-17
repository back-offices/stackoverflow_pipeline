
-- Select source data from ephemeral model
WITH tags_base AS (
    SELECT *
    FROM {{ ref('extr_tags') }}
),

-- Deduplicate based on id
tags_dedup AS (
    SELECT * EXCEPT (row_num)
    FROM (
        SELECT
            *,
            ROW_NUMBER()
                OVER (PARTITION BY tag_id ORDER BY source_partition_timestamp DESC)
                AS row_num
        FROM tags_base
    )
    WHERE row_num = 1
)

-- Final select and add insertion_timestamp
SELECT
    *,
    CURRENT_TIMESTAMP() AS insertion_timestamp
FROM tags_dedup