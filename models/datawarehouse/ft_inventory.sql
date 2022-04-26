{{ config(materialized='incremental', unique_key = 'film_id') }}
SELECT 	a.inventory_id, 
		a.film_id, 
		a.store_id, 
		a.last_update
FROM stage.inventory a
{% if is_incremental() %}
WHERE a.last_update > (SELECT MAX(last_update) FROM {{this}})
{% endif %}