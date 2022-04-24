{{ config(materialized='incremental', unique_key = 'address_id') }}
--Comentario Teste
SELECT 	a.address_id, 
		a.address, 
		a.address2, 
		a.district,  
		a.postal_code, 
		a.phone,
		b.city,
		c.country,
		GREATEST(a.last_update,b.last_update,c.last_update) as last_update
FROM stage.address a
INNER JOIN stage.city b ON a.city_id = b.city_id
INNER JOIN stage.country c ON b.country_id = c.country_id
{% if is_incremental() %}
WHERE GREATEST(a.last_update,b.last_update,c.last_update) > (SELECT MAX(last_update) FROM {{this}})
{% endif %}