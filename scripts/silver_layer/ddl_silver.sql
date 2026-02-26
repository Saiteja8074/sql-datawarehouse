/*
-----------------------------------------------------------------------------------------------------------------------------
                                       DDL SCRIPT:SILVER LAYER
-----------------------------------------------------------------------------------------------------------------------------
*/
-- create crm_cust_info table in silver 
create table silver.crm_cust_info(
cst_id int not null,
cst_key varchar(50),
cst_firstname varchar(50),
cst_lastname varchar(50),
cst_marital_status varchar(50),
cst_gndr varchar(50),
cst_create_date date);

-- check for duplicates or nulls in primary key 
select cst_id,count(*)
from bronze.crm_cust_info
group by cst_id
having count(*)>1 or cst_id is null;

-- check for unwanted spaces
select cst_firstname
from bronze.crm_cust_info
where cst_firstname!=trim(cst_firstname);

select cst_lastname
from bronze.crm_cust_info
where cst_lastname!=trim(cst_lastname);

select cst_gndr
from bronze.crm_cust_info
where cst_gndr!=trim(cst_gndr);

-- data standardization & consistency
select distinct cst_gndr
from bronze.crm_cust_info;

select distinct cst_marital_status
from bronze.crm_cust_info; 

-- truncate and insert
truncate table silver.crm_cust_info;

-- insert clean data to silver.crm_cust_info table
insert into silver.crm_cust_info(cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
select cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
case when upper(trim(cst_marital_status))='S' then 'Single'
     when upper(trim(cst_marital_status))='M' then'Married'
     else 'N/A'
end as cst_marital_status,
case when upper(trim(cst_gndr))='F' then 'Female'
     when upper(trim(cst_gndr))='M' then 'Male'
     else 'N/A'
end as cst_gndr,
cst_create_date
 from(
select * ,
row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info) r
where flag_last=1 and cst_id is not null;

-- create table for crm_prd_info in silver
create table silver.crm_prd_info(
prd_id int,
cat_id varchar(50),
prd_key varchar(50),
prd_nm varchar(50),
prd_cost int,		
prd_line varchar(50),
prd_start_dt date,				
prd_end_dt date);

-- check for nulls or duplicates in primary key
select prd_id,
count(*)
from bronze.crm_prd_info
group by prd_id
having count(*)>1 or prd_id is null;

-- check for unwanted spaces
select prd_nm
from bronze.crm_prd_info
where prd_nm!=trim(prd_nm);

-- check for nulls or negative cost
select prd_cost from bronze.crm_prd_info
where prd_cost<0 or prd_cost is null;

-- data standardization
select distinct prd_line
from bronze.crm_prd_info;

-- truncate and insert 
truncate table silver.crm_prd_info;

-- insert values to silver.crm_prd_info table
insert into silver.crm_prd_info
(prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
select
prd_id,
replace(substring(prd_key,1,5),'-','_') as cat_id,
substring(prd_key,7,length(prd_key)) as prd_key,
prd_nm,
ifnull(prd_cost,0) as prd_cost,
case when upper(trim(prd_line))='M' then 'Mountains'
     when upper(trim(prd_line))='R' then 'Road'
     when upper(trim(prd_line))='S' then 'Sea'
     when upper(trim(prd_line))='T' then 'Touring'
     else 'N/A'
end as prd_line,
cast(prd_start_dt as date) as prd_start_dt,
cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) as date) as prd_end_dt
from bronze.crm_prd_info;

-- create silver.crm_sales_details
create table silver.crm_sales_details(
sls_ord_num	varchar(50),
sls_prd_key	varchar(50),
sls_cust_id int,
sls_order_dt date,
sls_ship_dt	date,
sls_due_dt date,
sls_sales int,
sls_quantity int,	
sls_price int);

-- check for invalid date
select sls_order_dt
from bronze.crm_sales_details
where sls_order_dt<=0 or length(sls_order_dt)!=8;

-- truncate and insert
truncate table silver.crm_sales_details;

-- insert clean values into silver.crm_sales_details
insert into silver.crm_sales_details(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
select
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt<=0 or length(sls_order_dt)!=8 then null
    else cast(sls_order_dt as date)
    end as sls_order_dt,
case when sls_ship_dt<=0 or length(sls_ship_dt)!=8 then null
    else cast(sls_ship_dt as date)
    end as sls_ship_dt,
case when sls_due_dt<=0 or length(sls_due_dt)!=8 then null
    else cast(sls_due_dt as date)
    end as sls_due_dt,
case when sls_sales<=0 or sls_sales is null or sls_sales != sls_quantity * abs(sls_price)
     then sls_quantity*abs(sls_price)
     else sls_sales end as sls_sales,
sls_quantity,
case when sls_price is null or sls_price<=0
     then sls_sales / nullif(sls_quantity,0)
     else sls_price end as sls_price
from bronze.crm_sales_details;

-- create table silver.erp_cust_az12
create table silver.erp_cust_az12(
cid	varchar(50),
bdate date,
gen varchar(50));

-- identify out of range dates
select distinct bdate
from bronze.erp_cust_az12
where bdate<'1924-01-01' or bdate>curdate();

-- check for data standardization 
select distinct gen
from bronze.erp_cust_az12;

-- truncate and insert
truncate table silver.erp_cust_az12;

-- insert data into silver.erp_cust_az12
insert into silver.erp_cust_az12(cid,bdate,gen)
select
case when cid like 'NAS%' then substring(cid,4,length(cid))
else cid
end as cid,
case when bdate>curdate() then null
else bdate 
end as bdate,
case when upper(trim(gen)) in ('M','Male') then 'Male'
	when upper(trim(gen)) in ('F','Female')then 'Female'
    else 'n/a'
    end as gen
from bronze.erp_cust_az12;

-- create table for erp_loc
create table silver.erp_loc_a101(
cid varchar(50),
cntry varchar(50));

-- check for data standardization
select distinct cntry from bronze.erp_loc_a101;

-- truncate and insert
truncate table silver.erp_loc_a101;

-- insert data into silver.erp_loc
insert into silver.erp_loc_a101(cid,cntry)
select
replace(cid,'-','') as cid,
case when trim(cntry) in ('US','USA') then 'United States'
    when trim(cntry) ='DE' then 'Germany'
    when trim(cntry)='' or cntry is null then 'n/a'
    else trim(cntry) end as cntry
from bronze.erp_loc_a101;

-- create table silver erp_px_cat
create table silver.erp_px_cat_g1v2(
id varchar(50),
cat	varchar(50),
subcat varchar(50),
maintenance varchar(50));

-- truncate and insert 
truncate table silver.erp_px_cat_g1v2;

-- insert data into silver erp_px_cat
insert into silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
select 
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2;
