SELECT
    parent_id,
    AVG(body_length) AS avg_body_length_answers,
    COUNT(DISTINCT(post_answer_id)) AS total_answers

FROM {{ref('fct_answer')}}

GROUP BY parent_id