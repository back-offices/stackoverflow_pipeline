SELECT
    CAST(id AS INTEGER) AS vote_id,
    CAST(post_id AS INTEGER) AS post_id,
    CAST(vote_type_id AS INTEGER) AS vote_type_id,
    CAST(creation_date AS TIMESTAMP) AS created_at,
    CAST(CURRENT_TIMESTAMP() AS TIMESTAMP) AS source_partition_timestamp
    
FROM {{ source('0_stackoverflow','votes') }}