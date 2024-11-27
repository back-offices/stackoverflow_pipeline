
-- Select source data from ephemeral model
WITH users_base AS (
    SELECT *
    FROM {{ ref('extr_users') }}
),

-- Deduplicate based on id
users_dedup AS (
    SELECT * EXCEPT (row_num)
    FROM (
        SELECT
            *,
            ROW_NUMBER()
                OVER (PARTITION BY user_id ORDER BY source_partition_timestamp DESC)
                AS row_num
        FROM users_base
    )
    WHERE row_num = 1
)

-- Final select and add insertion_timestamp
SELECT
    *,
    CURRENT_TIMESTAMP() AS insertion_timestamp
FROM users_dedup