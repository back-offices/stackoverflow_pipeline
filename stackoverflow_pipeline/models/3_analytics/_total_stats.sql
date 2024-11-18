{{ config(materialized='view') }}

SELECT
    c.tag_name,
    COUNT(a.post_question_id) AS total_questions,
    COUNT(DISTINCT CASE WHEN a.accepted_answer_id IS NOT NULL THEN a.post_question_id END) AS total_answers_accepted, 
    SUM(a.answer_count) AS total_answers,
    AVG(a.body_length) AS avg_body_length,
    AVG(a.view_count) AS avg_views,
    COUNT(d.comment_text) AS total_comments,
    AVG(a.score) AS avg_score

FROM {{ref('fct_question')}} AS a

INNER JOIN {{ref('brg_question_tag')}} AS b
ON a.post_question_id = b.post_question_id

INNER JOIN {{ref('dim_tag')}} AS c
ON b.tag_id = c.tag_id

INNER JOIN {{ref('dim_comment')}} AS d
ON a.post_question_id = d.post_id

GROUP BY c.tag_name

ORDER BY total_questions DESC,total_answers_accepted DESC