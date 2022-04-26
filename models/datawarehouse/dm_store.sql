{{ config(materialized='incremental', unique_key = 'store_id') }}
SELECT  a.store_id, 
		a.manager_staff_id, 
		a.address_id, 
		a.last_update
FROM stage.store a
{% if is_incremental() %}
WHERE a.last_update > (SELECT MAX(last_update) FROM {{this}})
{% endif %}