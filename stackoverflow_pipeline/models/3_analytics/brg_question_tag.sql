    SELECT
        a.post_question_id,
        tag_name,
        b.tag_id
    
    FROM {{ ref('posts_questions') }} AS a, UNNEST(a.tags_array) AS tag_name
    
    LEFT JOIN {{ ref('tags') }} AS b
    ON tag_name = b.tag_name