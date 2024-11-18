SELECT
    post_answer_pk,
    post_answer_id,
    accepted_answer_id,
    parent_id,
    owner_user_id AS user_id,
    CAST(created_at AS DATE) AS date_key,
    post_answer_title,
    LENGTH(post_answer_title) AS title_length,
    post_answer_body,
    LENGTH(post_answer_body) AS body_length,
    answer_count,
    comment_count,
    favorite_count,
    score,
    view_count,
    last_edited_at,
    created_at

FROM {{ ref('posts_answers') }}