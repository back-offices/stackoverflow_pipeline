SELECT
    CAST(id AS INTEGER) AS user_id,
    CAST(display_name AS STRING) AS user_display_name,
    CAST(about_me AS STRING) AS about_me,
    CAST(age AS STRING) AS age,
    CAST(location AS STRING) AS user_location,
    CAST(reputation AS INTEGER) AS reputation,
    CAST(up_votes AS INTEGER) AS up_votes,
    CAST(down_votes AS INTEGER) AS down_votes,
    CAST(views AS INTEGER) AS views,
    CAST(profile_image_url AS STRING) AS profile_image_url,
    CAST(website_url AS STRING) AS website_url,
    CAST(last_access_date AS TIMESTAMP) AS last_accessed_at,
    CAST(creation_date AS TIMESTAMP) AS created_at,
    CAST(CURRENT_TIMESTAMP() AS TIMESTAMP) AS source_partition_timestamp

FROM {{ source('stackoverflow','users') }}