-- We have to create a warehouse, then choose the corresponding warehouse, then create databases and tables accordingly.
create warehouse MYEBAYANALYTICS;
-- Snowflake warehouse is not case sensitive
-- We use Warehouses as intermediate to integrate wth visyualization tools for reports if the data is stored in Local system.
show databases;
-- In Hive Warehouse the size is not mandatory for datatypes, same goes for snowflake.

-- Example 01
create table mytable1(id int, city varchar(33), country varchar);

insert into mytable1 values(123, 'hyder', 'india');

select * from mytable1;

-- Example 02 Creation of Table
CREATE OR REPLACE TABLE employees (
EMPLOYEE_ID number,
FIRST_NAME varchar(25),
LAST_NAME varchar(25),
EMAIL varchar(25),
PHONE_NUMBER varchar(15),
HIRE_DATE DATE,
JOB_ID varchar(15),
SALARY  number(12,2),
COMMISSION_PCT  real,
MANAGER_ID number,
DEPARTMENT_ID number
);
