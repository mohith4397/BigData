					--<<PETROL_DATA>>--
--CREATE TABLE

create table petrol 
(distributer_id STRING,
distributer_name STRING,
amt_IN STRING,
amt_OUT STRING,
vol_IN INT,
vol_OUT INT,
year INT)
row format delimited 
fields terminated by ',' stored as textfile;

--LOAD DATA
LOAD DATA LOCAL INPATH '/home/mohith/DatasetsandCodes/HIVE/hive2/petrol_data.txt' INTO TABLE petrol;
 
--1. In real life what is the total amount of petrol in volume sold by every distributor?
SELECT distributer_name,COUNT(vol_OUT) FROM petrol GROUP BY distributer_name;

--2. Which are the top 10 distributors ID's for selling petrol and also display the amount of petrol sold in volume by them individually?
SELECT distributer_id,vol_OUT FROM petrol ORDER BY vol_OUT DESC limit 10;

--3. Find real life 10 distributor name who sold petrol in the least amount.
SELECT distributer_name,amt_OUT FROM petrol;

--4. The constraint to this query is the difference between volumeIN and volumeOuT is illegal in real life if greater than 500. As we see all distributors are receiving patrols on every next cycle.
--List all distributors who have this difference, along with the year and the difference which they have in that year.
--Hint: (vol_IN-vol_OUT)>500
SELECT distributer_name,(vol_IN-vol_OUT) AS difference FROM petrol WHERE (vol_IN-vol_OUT)>500 ;

