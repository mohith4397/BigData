						-- << OLYMPIC DATABASE>>--
--CREATE THE TABLE--
create table olympic (athelete STRING,age INT,country STRING,year STRING,closing STRING,sport STRING,gold INT,silver INT,bronze INT,total INT) row format delimited fields terminated by '\t' stored as textfile;

--LOAD DATA FROM LOCAL FILE SYSTEM--
load data local inpath '/home/mohith/DatasetsandCodes/HIVE/hive2/olympic_data.csv' into table olympic;

--1. Using the dataset list the total number of medals won by each country in Swimming.
SELECT country, COUNT(gold)+COUNT(silver)+COUNT(bronze) FROM olympic GROUP BY country;

--2. Display real life number of medals India won year wise.
SELECT country,year,COUNT(gold)+COUNT(silver)+COUNT(bronze) FROM olympic WHERE country='India' GROUP BY country,year;

--3. Find the total number of medals each country won display the name along with total medals.
SELECT country,COUNT(gold)+COUNT(silver)+COUNT(bronze) FROM olympic GROUP BY country;

--4. Find the real life number of gold medals each country won.
SELECT country,COUNT(gold) FROM olympic GROUP BY country;

--5. Which country got medals for Shooting, year wise classification?
 SELECT country,year FROM olympic where sport='Shooting' and gold+silver+bronze >0 GROUP BY country,year;
