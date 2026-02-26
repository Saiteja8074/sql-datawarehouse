/*
------------------------------------------------------------------------------------------------------------------------------
                                           DDL SCRIPT:GOLD LAYER
------------------------------------------------------------------------------------------------------------------------------
*/
-- create view for customers
create view gold.dim_customers as 
select 
row_number() over (order by ci.cst_id) as Customer_Key,
ci.cst_id as Customer_Id,
ci.cst_key as Customer_Number,
ci.cst_firstname as First_Name,
ci.cst_lastname as Last_Name,
la.cntry as Country,
ci.cst_marital_status as Marital_Status,
case when ci.cst_gndr!= 'n/a' then ci.cst_gndr
    else coalesce(ca.gen,'N/A')
end as Gender,
ca.bdate as Birth_Date,
ci.cst_create_date as Create_Date
from silver.crm_cust_info ci 
left join silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key=la.cid;

-- create view for products
create view gold.dim_products as
select 
row_number() over (order by pr.prd_start_dt,pr.prd_key) as Product_Key,
pr.prd_id as Product_Id,
pr.prd_key as Product_Number,
pr.prd_nm as Product_Name,
pr.cat_id as Category_Id,
px.cat as Category,
px.subcat as SubCategory,
px.maintenance as Maintenance,
pr.prd_cost as Cost,
pr.prd_line as Product_Line,
pr.prd_start_dt as Start_Date
from silver.crm_prd_info pr
left join silver.erp_px_cat_g1v2 px
on pr.cat_id=px.id
where prd_end_dt is null;
 
 -- create view for sales
 create view gold.fact_sales as
select
s.sls_ord_num as order_number,
p.product_key,
c.customer_id,
s.sls_order_dt as order_date,
s.sls_ship_dt as shipping_date,
s.sls_due_dt as due_date,
s.sls_sales as sales,
s.sls_quantity as quantity,
s.sls_price as price
from silver.crm_sales_details s
left join gold.dim_products p
on s.sls_prd_key=p.product_number
left join gold.dim_customers c
on s.sls_cust_id=c.customer_id;
