SELECT
    user_pk,
    user_id,
    user_display_name,
    about_me,
    age,
    user_location,
    reputation,
    up_votes,
    down_votes,
    views,
    profile_image_url,
    website_url,
    last_accessed_at,
    created_at

FROM {{ ref('users') }}