{{ config(materialized='incremental', unique_key = 'customer_id') }}
SELECT  a.customer_id, 
		a.store_id, 
		a.first_name, 
		a.last_name, 
		a.email, 
		a.address_id, 
		a.activebool as active, 
		a.create_date, 
		a.last_update, 
FROM stage.customer a
{% if is_incremental() %}
WHERE a.last_update > (SELECT MAX(last_update) FROM {{this}})
{% endif %}