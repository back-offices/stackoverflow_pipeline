
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
    FARM_FINGERPRINT(
        CONCAT(
            COALESCE(CAST(post_question_id AS STRING),''),
            COALESCE(CAST(created_at AS STRING),''),
            COALESCE(CAST(owner_user_id AS STRING),'')
        )
    ) as post_question_pk,
    *,
    CURRENT_TIMESTAMP() AS insertion_timestamp
FROM posts_questions_dedup