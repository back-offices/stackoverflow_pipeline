SELECT
    tag_id,
    tag_name,
    count,
    insertion_timestamp

FROM {{ ref('tags') }}