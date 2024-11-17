SELECT
    badge_pk,
    badge_id,
    user_id,
    badge_name,
    badge_date AS badge_date_awarded,
    badge_class,
    is_tag_based

FROM {{ ref('badges') }}