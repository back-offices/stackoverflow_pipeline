SELECT
    FARM_FINGERPRINT(
        CONCAT(
            COALESCE(CAST(id AS STRING),''),
            COALESCE(CAST(name AS STRING),''),
            COALESCE(CAST(user_id AS STRING),'')
        )
    ) as badge_pk,
    CAST(id AS INTEGER) AS badge_id,
    CAST(name AS STRING) AS badge_name,
    CAST(date AS TIMESTAMP) AS badge_date,
    CAST(user_id AS INTEGER) AS user_id,
    CAST(class AS INTEGER) AS badge_class,
    CAST(tag_based AS BOOLEAN) AS is_tag_based,
    CAST(CURRENT_TIMESTAMP() AS TIMESTAMP) AS source_partition_timestamp

FROM {{ source('0_stackoverflow','badges') }}