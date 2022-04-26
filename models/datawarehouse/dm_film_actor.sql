{{ config(materialized='incremental', unique_key = 'film_actor_id') }}
SELECT  CONCAT(a.film_id, '|', a.actor_id) as film_actor_id,
		a.film_id,
		b.first_name,
		b.last_name, 
		GREATEST(a.last_update, b.last_update) as last_update
FROM stage.film_actor a
INNER JOIN {{ref('dm_actor')}} b on b.actor_id = a.actor_id
{% if is_incremental() %}
WHERE GREATEST(a.last_update, b.last_update) > (SELECT MAX(last_update) FROM {{this}})
{% endif %}