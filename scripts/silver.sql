if OBJECT_ID ('silver.crm_customer_info','U') is not null
	drop table silver.crm_customer_info;
create table silver.crm_customer_info(
customer_id int,
customer_key nvarchar(50),
customer_firstname nvarchar(50),
customer_lastname nvarchar(50),
customer_marital_status nvarchar(50),
customer_gender nvarchar(50),
customer_create_date date,
dwh_create_date datetime2 default getdate());

if OBJECT_ID ('silver.crm_product_info','U') is not null
	drop table silver.crm_product_info;
create table silver.crm_product_info(
product_id int,
product_key nvarchar(50),
category_id nvarchar(50),
product_name nvarchar(50),
product_cost int,
product_line nvarchar(50),
product_start_date date,
product_end_date date,
dwh_create_date datetime2 default getdate());

select * from silver.crm_product_info
select * from silver.erp_category_g1v2

if OBJECT_ID ('silver.crm_sales_details','U') is not null
	drop table silver.crm_sales_details;
create table silver.crm_sales_details(
sales_order_number nvarchar(50),
sales_product_key nvarchar(50),
sales_customer_id int,
sales_order_date date,
sales_ship_date date,
sales_due_date date,
sales_sales int,
sales_quantity int,
sales_price int,
dwh_create_date datetime2 default getdate());

if OBJECT_ID ('silver.erp_customer_az12','U') is not null
	drop table silver.erp_customer_az12;
create table silver.erp_customer_az12(
customer_id nvarchar(50),
birthdate date,
gender nvarchar(50),
dwh_create_date datetime2 default getdate());

if OBJECT_ID ('silver.erp_location_a101','U') is not null
	drop table silver.erp_location_a101;
create table silver.erp_location_a101(
country_id nvarchar(50),
country_name nvarchar(50),
dwh_create_date datetime2 default getdate());

if OBJECT_ID ('silver.erp_category_g1v2','U') is not null
	drop table silver.erp_category_g1v2;
create table silver.erp_category_g1v2(
category_id nvarchar(50),
category_name nvarchar(50),
subcategory_name nvarchar(50),
maintenance nvarchar(50),
dwh_create_date datetime2 default getdate());

create or alter procedure silver.load_silver
as
begin
declare @start_time Datetime, @end_time Datetime, @batch_start_time Datetime, @batch_end_time datetime;
	begin try
		set @batch_start_time = GETDATE();
		print '======================================================';
		print 'Loading Silver Layer';
		print '======================================================';

		print '------------------------------------------------------';
		print 'Loading CRM Tables';
		print '------------------------------------------------------';

		set @start_time = getdate();
		print '>> Truncating Table : silver.crm_customer_info'
		truncate table silver.crm_customer_info;
		print '>> Inserting Data Into : silver.crm_customer_info'
		insert into silver.crm_customer_info(
		customer_id,
		customer_key,
		customer_firstname,
		customer_lastname,
		customer_marital_status,
		customer_gender,
		customer_create_date
		)
		select customer_id,
		customer_key,
		trim(customer_firstname),
		trim(customer_lastname),
		case when upper(trim(customer_marital_status)) = 'M' then 'Married'
			when upper(trim(customer_marital_status)) = 'S' then 'Single'
			else 'N/A'
		end customer_marital_status,
		case when upper(trim(customer_gender)) = 'F' then 'Female'
			when upper(trim(customer_gender)) = 'M' then 'Male'
			else 'N/A'
		end customer_gender,
		customer_create_date
		from (
			select *, row_number() over (
				partition by customer_id
				order by customer_create_date desc)
			as flag_last from bronze.crm_customer_info
			where customer_id is not null
		)t where flag_last = 1;
		set @end_time = getdate();
		print '>> Load Duration : ' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------------'

		set @start_time = getdate();
		print '>> Truncating Table : silver.crm_product_info'
		truncate table silver.crm_product_info;
		print '>> Inserting Data Into : silver.crm_product_info'
		insert into silver.crm_product_info(
		product_id,
		product_key,
		category_id,
		product_name,
		product_cost,
		product_line,
		product_start_date,
		product_end_date)
		select
		product_id,
		replace(SUBSTRING(product_key,1, 5) ,'-','_') as category_id,
		substring(product_key, 7, len(product_key)) as product_key,
		product_name,
		isnull(product_cost, 0) as product_cost,
		case upper(trim(product_line))
			when 'M' then 'Mountain'
			when 'R' then 'Road'
			when 'S' then 'Other Sales'
			when 'T' then 'Touring'
			else 'N/A'
		end as product_line,
		cast(product_start_date as date) as product_start_date,
		cast(lead(product_start_date) over (partition by product_key order by product_start_date)-1 as date) as product_end_date
		from bronze.crm_product_info;
		set @end_time = getdate();
		print '>> Load Duration : ' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------------'

		set @start_time = getdate();
		print '>> Truncating Table : silver.crm_sales_details'
		truncate table silver.crm_sales_details;
		print '>> Inserting Data Into : silver.crm_sales_details'
		insert into silver.crm_sales_details(
		sales_order_number,
		sales_product_key,
		sales_customer_id,
		sales_order_date,
		sales_ship_date,
		sales_due_date,
		sales_sales,
		sales_quantity,
		sales_price
		)
		select
		sales_order_number,
		sales_product_key,
		sales_customer_id,
		case when sales_order_date = 0 or len(sales_order_date) != 8 then null
			else cast(cast(sales_order_date as varchar) as date)
		end as sales_order_date,
		case when sales_ship_date = 0 or len(sales_ship_date) != 8 then null
			else cast(cast(sales_ship_date as varchar) as date)
		end as sales_ship_date,
		case when sales_due_date = 0 or len(sales_due_date) != 8 then null
			else cast(cast(sales_due_date as varchar) as date)
		end as sales_due_date,
		case when sales_sales is null or sales_sales <= 0 or sales_sales != sales_quantity * abs(sales_price)
				then sales_quantity*abs(sales_price)
			else sales_sales
		end as sales_sales,
		sales_quantity,
		case when sales_price is null or sales_price <= 0
				then sales_sales / nullif(sales_quantity,0)
			else sales_price
		end as sales_price
		from bronze.crm_sales_details
		set @end_time = getdate();
		print '>> Load Duration : ' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------------'
		
		print '------------------------------------------------------';
		print 'Loading ERP Tables';
		print '------------------------------------------------------';

		set @start_time = getdate();
		print '>> Truncating Table : silver.erp_customer_az12'
		truncate table silver.erp_customer_az12;
		print '>> Inserting Data Into : silver.erp_customer_az12'
		insert into silver.erp_customer_az12(
		customer_id,
		birthdate,
		gender
		)
		select
		case when customer_id like 'NAS%' then substring(customer_id, 4, len(customer_id))
			else customer_id
		end customer_id,
		case when birthdate > getdate() then null
			else birthdate
		end birthdate,
		case when upper(trim(gender)) in ('M','Male') then 'Male'
			when upper(trim(gender)) in ('F', 'Female') then 'Female'
			else 'N/A'
		end gender
		from bronze.erp_customer_az12
		set @end_time = getdate();
		print '>> Load Duration : ' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------------'

		set @start_time = getdate();
		print '>> Truncating Table : silver.erp_location_a101'
		truncate table silver.erp_location_a101;
		print '>> Inserting Data Into : silver.erp_location_a101'
		insert into silver.erp_location_a101(
		country_id,
		country_name
		)
		select
		replace(country_id,'-','')country_id,
		case when trim(country_name) = 'DE' then 'Germany'
			when trim(country_name) in ('US', 'USA') then 'United States'
			when trim(country_name) = '' or country_name is null then 'N/A'
			else trim(country_name)
		end as country_name
		from bronze.erp_location_a101
		order by country_name
		set @end_time = getdate();
		print '>> Load Duration : ' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------------'

		set @start_time = getdate();
		print '>> Truncating Table : silver.erp_category_g1v2'
		truncate table silver.erp_category_g1v2;
		print '>> Inserting Data Into : silver.erp_category_g1v2'
		insert into silver.erp_category_g1v2(
		category_id, category_name, subcategory_name, maintenance
		)
		select category_id, category_name, subcategory_name, maintenance
		from bronze.erp_category_g1v2
		set @end_time = getdate();
		print '>> Load Duration : ' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------------'
		set @batch_end_time = getdate();
		print '======================================================';
		print 'Loading Silver Layer is Completed';
		print 'Total Load Duration : ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds';
		print '======================================================';
	end try
	begin catch
		print '======================================================';
		print 'Error occured during loading Silver Layer';
		print 'Error Message : ' + Error_Message();
		print 'Error Message : ' + cast(Error_Number() as nvarchar);
		print 'Error Message : ' + cast(Error_State() as nvarchar);
		print '======================================================';
	end catch
end

exec silver.load_silver

select * from silver.crm_sales_details