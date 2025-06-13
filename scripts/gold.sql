create view gold.dim_customers as
select
	ROW_NUMBER() over (order by ci.customer_id) as customer_key,
	ci.customer_id,
	ci.customer_key as customer_number,
	ci.customer_firstname,
	ci.customer_lastname,
		la.country_name,
	ci.customer_marital_status,
	case when ci.customer_gender != 'N/A' then ca.gender
		else coalesce(ca.gender, 'N/A')
	end as gender,
	ca.birthdate,
	ci.customer_create_date
	from silver.crm_customer_info ci
	left join silver.erp_customer_az12 ca
	on ci.customer_key = ca.customer_id
	left join silver.erp_location_a101 la
	on ci.customer_key = la.country_id

create view gold.dim_products as
select
	ROW_NUMBER() over (order by pn.product_start_date, pn.product_key) as product_key,
	pn.product_id,
	pn.product_key as product_number,
	pn.product_name,
	pn.category_id,
	pc.category_name,
	pc.subcategory_name,
	pn.product_cost,
	pn.product_line,
	pn.product_start_date
	from silver.crm_product_info pn
	inner join silver.erp_category_g1v2 pc
	on pn.product_key = pc.category_id
	where product_end_date is null

create view gold.fact_sales as
select
	sd.sales_order_number,
	pr.product_key,
	cu.customer_key,
	sd.sales_order_date,
	sd.sales_ship_date,
	sd.sales_due_date,
	sd.sales_sales as sales_amount,
	sd.sales_quantity,
	sd.sales_price
	from silver.crm_sales_details sd
	left join gold.dim_products pr
	on sd.sales_product_key = pr.product_number
	left join gold.dim_customers cu
	on sd.sales_customer_id = cu.customer_id