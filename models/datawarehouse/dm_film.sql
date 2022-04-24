{{ config(materialized='incremental', unique_key = 'film_id') }}
SELECT 	a.film_id, 
		a.title, 
		a.description, 
		a.release_year, 
		b.name as language, 
		a.rental_duration, 
		a.rental_rate, 
		a.length, 
		a.replacement_cost, 
		a.rating, 
		GREATEST(a.last_update, b.last_update) as last_update
FROM stage.film a
INNER JOIN stage.language b on b.language_id = a.language_id
{% if is_incremental() %}
WHERE GREATEST(a.last_update,b.last_update) > (SELECT MAX(last_update) FROM {{this}})
{% endif %}