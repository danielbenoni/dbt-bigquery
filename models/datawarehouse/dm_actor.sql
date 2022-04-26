{{ config(materialized='incremental', unique_key = 'actor_id') }}
SELECT  a.actor_id, 
		a.first_name, 
		a.last_name, 
		a.last_update
FROM stage.actor a
{% if is_incremental() %}
WHERE a.last_update > (SELECT MAX(last_update) FROM {{this}})
{% endif %}