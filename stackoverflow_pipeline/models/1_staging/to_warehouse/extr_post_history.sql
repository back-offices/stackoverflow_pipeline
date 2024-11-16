SELECT
    CAST(id AS INTEGER) AS post_history_id,
    CAST(post_id AS INTEGER) AS post_id,
    CAST(post_history_type_id AS INTEGER) AS post_history_type_id,
    CAST(revision_guid AS STRING) AS revision_guid
    CAST(user_id AS INTEGER) AS user_id,
    CAST(text AS STRING) AS post_history_text,
    CAST(comment AS STRING) AS history_comment,
    CAST(creation_date AS TIMESTAMP) AS created_at,
    CAST(CURRENT_TIMESTAMP() AS TIMESTAMP) AS source_partition_timestamp

FROM {{ source('stackoverflow','post_history') }}