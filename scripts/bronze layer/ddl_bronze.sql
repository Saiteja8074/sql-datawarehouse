/*
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                                                                       DDL SCRIPT:create bronze tables
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
*/
create database datawarehouse;
use datawarehouse;
create schema bronze;
create schema silver;
create schema gold;

create table bronze.crm_cust_info(
cst_id int,
cst_key varchar(50),
cst_firstname varchar(50),
cst_lastname varchar(50),
cst_marital_status varchar(50),
cst_gndr varchar(50),
cst_create_date date);

create table bronze.crm_prd_info(
prd_id int,
prd_key varchar(50),
prd_nm varchar(50),
prd_cost int,		
prd_line varchar(50),
prd_start_dt date,				
prd_end_dt date);


create table bronze.crm_sales_details(
sls_ord_num	varchar(50),
sls_prd_key	varchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt	int,
sls_due_dt int,
sls_sales int,
sls_quantity int,	
sls_price int);


create table bronze.erp_cust_az12(
CID	varchar(50),
BDATE date,
GEN varchar(50));


create table bronze.erp_loc_a101(
CID varchar(50),
CNTRY varchar(50));

create table bronze.erp_px_cat_g1v2(
ID varchar(50),
CAT	varchar(50),
SUBCAT varchar(50),
MAINTENANCE varchar(50));


ALTER TABLE bronze.crm_cust_info
MODIFY cst_id INT NULL;

truncate table bronze.crm_cust_info;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.4/Uploads/cust_info.csv'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@cst_id, cst_key, cst_firstname, cst_lastname,
 cst_marital_status, cst_gndr, @cst_create_date)
SET
  cst_id = NULLIF(@cst_id, ''),
  cst_create_date = STR_TO_DATE(@cst_create_date, '%m/%d/%Y');
  
truncate table bronze.crm_prd_info;
  
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.4/Uploads/prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(prd_id, prd_key, prd_nm, @prd_cost, prd_line, @prd_start_dt, @prd_end_dt)
SET
  prd_cost     = NULLIF(@prd_cost, ''),
  prd_start_dt = CASE
                    WHEN @prd_start_dt = '' THEN NULL
                    ELSE STR_TO_DATE(@prd_start_dt, '%Y-%m-%d')
                 END,
  prd_end_dt   = CASE
                    WHEN @prd_end_dt = '' THEN NULL
                    ELSE STR_TO_DATE(@prd_end_dt, '%Y-%m-%d')
                 END;
                 
                 
 truncate table bronze.crm_sales_details;             
 
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.4/Uploads/sales_details.csv'
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(
  sls_ord_num,
  sls_prd_key,
  sls_cust_id,
  sls_order_dt,
  sls_ship_dt,
  sls_due_dt,
  @sls_sales,
  sls_quantity,
  @sls_price
)
SET
  sls_sales = NULLIF(@sls_sales, ''),
  sls_price = NULLIF(@sls_price, '');
  
truncate table bronze.erp_cust_az12;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.4/Uploads/cust_az12.csv'
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(
  CID,
  @BDATE,
  GEN
)
SET
  BDATE = NULLIF(@BDATE, '');
  
truncate table bronze.erp_loc_a101;
  
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.4/Uploads/loc_a101.csv'
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(CID, CNTRY);

truncate table bronze.erp_px_cat_g1v2;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.4/Uploads/px_cat_g1v2.csv'
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(ID, CAT, SUBCAT, MAINTENANCE);
