
-- Select source data from ephemeral model
WITH comments_base AS (
    SELECT *
    FROM {{ ref('extr_comments') }}
),

-- Deduplicate based on id
comments_dedup AS (
    SELECT * EXCEPT (row_num)
    FROM (
        SELECT
            *,
            ROW_NUMBER()
                OVER (PARTITION BY comment_id ORDER BY source_partition_timestamp DESC)
                AS row_num
        FROM comments_base
    )
    WHERE row_num = 1
)

-- Final select and add insertion_timestamp
SELECT
    *,
    CURRENT_TIMESTAMP() AS insertion_timestamp
FROM comments_dedup