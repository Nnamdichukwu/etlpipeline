--first create a database
create database sourcedb;
-- Next is to set mysql to use that database
use sourcedb;
-- create the sales_team table with the columns and datatypes
create table user_table(id varchar(255) primary key,address varchar(255), inserted_at timestamp);
-- load the csv directly into that table created
LOAD DATA   INFILE '/sample_us_users.csv' INTO TABLE  user_table FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
