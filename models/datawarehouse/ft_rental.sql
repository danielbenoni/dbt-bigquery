{{ config(materialized='incremental', unique_key = 'rental_id') }}
SELECT  a.rental_id, 
		a.rental_date, 
		c.film_id, 
		a.customer_id,
		a.staff_id,
		b.staff_id as staff_id_payment,
		b.amount as payment_amount,
		b.payment_date,
		a.return_date, 
		GREATEST(a.last_update, b.last_update, c.last_update) as last_update
FROM stage.rental a
INNER JOIN stage.payment b on b.rental_id = a.rental_id and b.customer_id = a.customer_id
INNER JOIN {{ref('ft_inventory')}} c as c.inventory_id = a.inventory_id
{% if is_incremental() %}
WHERE GREATEST(a.last_update, b.last_update, c.last_update) > (SELECT MAX(last_update) FROM {{this}})
{% endif %}