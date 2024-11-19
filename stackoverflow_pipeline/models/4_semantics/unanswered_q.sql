SELECT
    e.year AS created_year,
    c.tag_name,
    COUNT(DISTINCT(a.post_question_id)) AS total_unanswered_questions,
    AVG(a.body_length) AS avg_body_length_question,
    AVG(f.avg_body_length_answers) AS avg_body_length_answer,
    SUM(a.view_count) AS views,
    AVG(a.score) AS avg_score

FROM {{ref('fct_question')}} AS a

INNER JOIN {{ref('brg_question_tag')}} AS b
ON a.post_question_id = b.post_question_id

INNER JOIN {{ref('dim_tag')}} AS c
ON b.tag_id = c.tag_id

LEFT JOIN {{ref('dim_date')}} AS e
ON a.date_key = e.date_key

LEFT JOIN {{ref('fct_answer_agg')}} AS f
ON a.post_question_id = f.parent_id

WHERE a.accepted_answer_id IS NULL

GROUP BY c.tag_name,created_year

ORDER BY created_year DESC, total_unanswered_questions DESC