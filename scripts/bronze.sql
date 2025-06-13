create database Crime_DataWarehouse

use Crime_DataWarehouse

create schema bronze
go
create schema silver
go
create schema gold
go

if OBJECT_ID ('bronze.crm_customer_info','U') is not null
	drop table bronze.crm_customer_info;
create table bronze.crm_customer_info(
customer_id int,
customer_key nvarchar(50),
customer_firstname nvarchar(50),
customer_lastname nvarchar(50),
customer_marital_status nvarchar(50),
customer_gender nvarchar(50),
customer_create_date date)

if OBJECT_ID ('bronze.crm_product_info','U') is not null
	drop table bronze.crm_product_info;
create table bronze.crm_product_info(
product_id int,
product_key nvarchar(50),
product_number nvarchar(50),
product_cost int,
product_line nvarchar(50),
product_start_date datetime,
product_end_date datetime)

if OBJECT_ID ('bronze.crm_sales_details','U') is not null
	drop table bronze.crm_sales_details;
create table bronze.crm_sales_details(
sales_order_number nvarchar(50),
sales_product_key nvarchar(50),
sales_customer_id int,
sales_order_date int,
sales_ship_date int,
sales_due_date int,
sales_sales int,
sales_quantity int,
sales_price int)

if OBJECT_ID ('bronze.erp_customer_az12','U') is not null
	drop table bronze.erp_customer_az12;
create table bronze.erp_customer_az12(
customer_id nvarchar(50),
birthdate date,
gender nvarchar(50))

if OBJECT_ID ('bronze.erp_location_a101','U') is not null
	drop table bronze.erp_location_a101;
create table bronze.erp_location_a101(
country_id nvarchar(50),
country_name nvarchar(50))

if OBJECT_ID ('bronze.erp_category_g1v2','U') is not null
	drop table bronze.erp_category_g1v2;
create table bronze.erp_category_g1v2(
category_id nvarchar(50),
category_name nvarchar(50),
subcategory_name nvarchar(50),
maintenance nvarchar(50))

create or alter procedure bronze.load_bronze as
begin
	declare @start_time Datetime, @end_time Datetime, @batch_start_time Datetime, @batch_end_time datetime;
	begin try
		set @batch_start_time = GETDATE();
		print '======================================================';
		print 'Loading Bronze Layer';
		print '======================================================';

		print '------------------------------------------------------';
		print 'Loading CRM Tables';
		print '------------------------------------------------------';

		set @start_time = getdate();
		print '>> Truncating Table: bronze.crm_customer_info';
		truncate table bronze.crm_customer_info;

		print '>> Inserting Table : bronze.crm_customer_info';
		bulk insert bronze.crm_customer_info
		from 'D:\RIshabh Software\Data Warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> Load Duration : ' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------------'

		set @start_time = getdate();
		print '>> Truncating Table: bronze.crm_product_info';
		truncate table bronze.crm_product_info;
		print '>> Inserting Table : bronze.crm_product_info';
		bulk insert bronze.crm_product_info
		from 'D:\RIshabh Software\Data Warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> Load Duration : ' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------------'

		set @start_time = getdate();
		print '>> Truncating Table: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;
		print '>> Inserting Table : bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'D:\RIshabh Software\Data Warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> Load Duration : ' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------------'

		print '------------------------------------------------------';
		print 'Loading ERP Tables';
		print '------------------------------------------------------';

		set @start_time = getdate();
		print '>> Truncating Table: bronze.erp_customer_az12';
		truncate table bronze.erp_customer_az12;
		print '>> Inserting Table : bronze.erp_customer_az12';
		bulk insert bronze.erp_customer_az12
		from 'D:\RIshabh Software\Data Warehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> Load Duration : ' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------------'

		set @start_time = getdate();
		print '>> Truncating Table: bronze.erp_location_a101';
		truncate table bronze.erp_location_a101;
		print '>> Inserting Table : bronze.erp_location_a101';
		bulk insert bronze.erp_location_a101
		from 'D:\RIshabh Software\Data Warehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> Load Duration : ' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------------'

		set @start_time = getdate();
		print '>> Truncating Table: bronze.erp_category_g1v2';
		truncate table bronze.erp_category_g1v2;
		print '>> Inserting Table : bronze.erp_category_g1v2';
		bulk insert bronze.erp_category_g1v2
		from 'D:\RIshabh Software\Data Warehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> Load Duration : ' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------------'

		set @batch_end_time = getdate();
		print '======================================================';
		print 'Loading Bronze Layer is Completed';
		print 'Total Load Duration : ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds';
		print '======================================================';
	end try
	begin catch
		print '======================================================';
		print 'Error occured during loading Bronze Layer';
		print 'Error Message : ' + Error_Message();
		print 'Error Message : ' + cast(Error_Number() as nvarchar);
		print 'Error Message : ' + cast(Error_State() as nvarchar);
		print '======================================================';
	end catch
end

exec bronze.load_bronze
