SELECT
    CAST(id AS INTEGER) AS comment_id,
    CAST(text AS STRING) AS comment_text,
    CAST(post_id AS INTEGER) AS post_id,
    CAST(user_id AS INTEGER) AS user_id,
    CAST(user_display_name AS STRING) AS user_display_name,
    CAST(score AS INTEGER) AS score,
    CAST(creation_date AS TIMESTAMP) AS created_at,
    CAST(CURRENT_TIMESTAMP() AS TIMESTAMP) AS source_partition_timestamp

FROM {{ source('0_stackoverflow','comments') }}