
-- Select source data from ephemeral model
WITH posts_answers_base AS (
    SELECT *
    FROM {{ ref('extr_posts_answers') }}
),

-- Deduplicate based on id
posts_answers_dedup AS (
    SELECT * EXCEPT (row_num)
    FROM (
        SELECT
            *,
            ROW_NUMBER()
                OVER (PARTITION BY post_answer_id ORDER BY source_partition_timestamp DESC)
                AS row_num
        FROM posts_answers_base
    )
    WHERE row_num = 1
)

-- Final select and add insertion_timestamp
SELECT
    *,
    CURRENT_TIMESTAMP() AS insertion_timestamp
FROM posts_answers_dedup