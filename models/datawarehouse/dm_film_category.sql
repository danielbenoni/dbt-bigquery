{{ config(materialized='incremental', unique_key = 'film_category_id') }}
SELECT CONCAT(a.film_id, '|', b.category_id) as film_category_id,
       a.film_id, 
	   b.category_name as category, 
	   GREATEST(a.last_update, b.last_update) as last_update
FROM stage.film_category a
INNER JOIN {{ref('dm_category')}} b on b.category_id = a.category_id
{% if is_incremental() %}
WHERE GREATEST(a.last_update, b.last_update) > (SELECT MAX(last_update) FROM {{this}})
{% endif %}