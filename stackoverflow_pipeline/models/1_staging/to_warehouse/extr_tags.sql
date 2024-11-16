SELECT
    CAST(id AS INTEGER) AS tag_id,
    CAST(tag_name AS STRING) AS tag_name,
    CAST(count AS INTEGER) AS count,
    CAST(excerpt_post_id AS INTEGER) AS excerpt_post_id,
    CAST(wiki_post_id AS INTEGER) AS wiki_post_id,
    CAST(CURRENT_TIMESTAMP() AS TIMESTAMP) AS source_partition_timestamp

FROM {{ source('0_stackoverflow','tags') }}