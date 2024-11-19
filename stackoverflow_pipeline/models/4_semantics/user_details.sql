SELECT
    e.year AS created_year,
    b.tag_name,
    c.age AS age,
    c.user_location AS user_location,
    COUNT(DISTINCT(c.user_id)) AS user_count,
    COUNT(DISTINCT(a.post_question_id)) AS total_questions,
    COUNT(DISTINCT CASE WHEN a.accepted_answer_id IS NOT NULL THEN a.post_question_id END) AS total_answers_accepted, 
    SUM(a.answer_count) AS total_answers,

FROM {{ref('fct_question')}} AS a

INNER JOIN {{ref('brg_question_tag')}} AS b
ON a.post_question_id = b.post_question_id

LEFT JOIN {{ref('dim_user')}} AS c
ON a.user_id = c.user_id

LEFT JOIN {{ref('dim_date')}} AS e
ON a.date_key = e.date_key

WHERE a.accepted_answer_id IS NULL

GROUP BY b.tag_name,created_year,age, user_location

ORDER BY created_year DESC, total_questions DESC, age DESC