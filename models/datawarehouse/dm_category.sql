{{ config(materialized='incremental', unique_key = 'category_id') }}
SELECT  a.category_id, 
		a.name as category_name, 
		a.last_update
FROM stage.category a
{% if is_incremental() %}
WHERE a.last_update > (SELECT MAX(last_update) FROM {{this}})
{% endif %}