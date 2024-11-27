
-- Select source data from ephemeral model
WITH posts_questions_base AS (
    SELECT *
    FROM {{ ref('extr_posts_questions') }}
),

-- Deduplicate based on id
posts_questions_dedup AS (
    SELECT * EXCEPT (row_num)
    FROM (
        SELECT
            *,
            ROW_NUMBER()
                OVER (PARTITION BY post_question_id ORDER BY source_partition_timestamp DESC)
                AS row_num
        FROM posts_questions_base
    )
    WHERE row_num = 1
)

-- Final select and add insertion_timestamp
SELECT
    *,
    CURRENT_TIMESTAMP() AS insertion_timestamp
FROM posts_questions_dedup