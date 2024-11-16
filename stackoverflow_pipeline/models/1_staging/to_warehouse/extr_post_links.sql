SELECT
    CAST(id AS INTEGER) AS post_link_id,
    CAST(link_type_id AS INTEGER) AS link_type_id,
    CAST(post_id AS INTEGER) AS post_id,
    CAST(related_post_id AS INTEGER) AS related_post_id,
    CAST(creation_date AS TIMESTAMP) AS created_at,
    CAST(CURRENT_TIMESTAMP() AS TIMESTAMP) AS source_partition_timestamp

FROM {{ source('stackoverflow','post_links') }}