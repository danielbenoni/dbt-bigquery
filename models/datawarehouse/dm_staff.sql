{{ config(materialized='incremental', unique_key = 'staff_id') }}
SELECT  staff_id, 
		first_name, 
		last_name, 
		address_id, 
		email, 
		store_id, 
		active, 
		username, 
		password, 
		last_update, 
		picture
FROM stage.staff
{% if is_incremental() %}
WHERE a.last_update > (SELECT MAX(last_update) FROM {{this}})
{% endif %}