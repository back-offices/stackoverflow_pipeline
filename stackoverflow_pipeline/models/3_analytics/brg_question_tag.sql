    SELECT
        FARM_FINGERPRINT(
            CONCAT(
                COALESCE(CAST(a.post_question_id AS STRING),''),
                COALESCE(CAST(a.created_at AS STRING),''),
                COALESCE(CAST(b.tag_id AS STRING),'')
            )
        ) as brg_question_tag_pk,
        a.post_question_id,
        tag_name,
        b.tag_id,
        a.created_at
    
    FROM {{ ref('posts_questions') }} AS a, UNNEST(a.tags_array) AS tag_name
    
    LEFT JOIN {{ ref('tags') }} AS b
    ON tag_name = b.tag_name