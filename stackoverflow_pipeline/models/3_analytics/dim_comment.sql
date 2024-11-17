SELECT
    comment_pk,
    comment_id,
    post_id,
    user_id,
    comment_text,
    user_display_name,
    score,
    created_at

FROM {{ ref('comments') }}