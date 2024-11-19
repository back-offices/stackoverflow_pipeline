WITH time_diff AS

(

SELECT
    e.year AS created_year,
    a.post_question_id,
    c.tag_name,
    MIN(a.created_at) AS question_created_at,
    MIN(b.created_at) AS answer_created_at,
    CAST(TIMESTAMP_DIFF(MIN(b.created_at), MIN(a.created_at), MINUTE) AS FLOAT64) AS avg_answer_time_min,
    CAST(TIMESTAMP_DIFF(MIN(b.created_at), MIN(a.created_at), HOUR) AS FLOAT64) AS avg_answer_time_hour,
    CAST(TIMESTAMP_DIFF(MIN(b.created_at), MIN(a.created_at), DAY) AS FLOAT64) AS avg_answer_time_day,

FROM {{ref('fct_question')}} AS a

LEFT JOIN {{ref('fct_answer')}} AS b
ON a.post_question_id = b.parent_id

INNER JOIN {{ref('brg_question_tag')}} AS c
ON a.post_question_id = c.post_question_id

LEFT JOIN {{ref('dim_date')}} AS e
ON a.date_key = e.date_key

WHERE a.accepted_answer_id IS NULL
AND a.created_at IS NOT NULL
AND b.created_at IS NOT NULL

GROUP BY
    a.post_question_id,
    c.tag_name,
    e.year

ORDER BY
    a.post_question_id DESC, c.tag_name DESC

)

SELECT
    created_year,
    tag_name,
    AVG(avg_answer_time_min) AS answer_time_min,
    AVG(avg_answer_time_hour) AS answer_time_hour,
    AVG(avg_answer_time_day) AS answer_time_day

FROM time_diff

GROUP BY created_year, tag_name

ORDER BY created_year DESC,tag_name ASC