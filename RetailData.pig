--			<<Retail Data>>			--
--LOAD DATA
retail2000 = load '/home/mohith/DatasetsandCodes/PIG/RetailCaseStudy/2000.txt' using PigStorage(',') as (cat_id:int,cat_name:chararray,jan:int,feb:int,march:int,april:int,may:int,june:int,july:int,aug:int,sep:int,oct:int,nov:int,dec:int);
retail2001 = load '/home/mohith/DatasetsandCodes/PIG/RetailCaseStudy/2001.txt' using PigStorage(',') as (cat_id:int,cat_name:chararray,jan:int,feb:int,march:int,april:int,may:int,june:int,july:int,aug:int,sep:int,oct:int,nov:int,dec:int);
retail2002 = load '/home/mohith/DatasetsandCodes/PIG/RetailCaseStudy/2002.txt' using PigStorage(',') as (cat_id:int,cat_name:chararray,jan:int,feb:int,march:int,april:int,may:int,june:int,july:int,aug:int,sep:int,oct:int,nov:int,dec:int);

--CALCUALTE YEARLY RETURNS
t0= foreach retail2000 generate $0,$1,($2+$3+$4+$5+$6+$7+$8+$9+$10+$11+$12+$13);
t1= foreach retail2001 generate $0,$1,($2+$3+$4+$5+$6+$7+$8+$9+$10+$11+$12+$13);
t2= foreach retail2002 generate $0,$1,($2+$3+$4+$5+$6+$7+$8+$9+$10+$11+$12+$13);

---join bag1 by bag2 by bag3
join_T0_T1_T2= join t0 by $0, t1 by $0, t2 by $0; 
retail_join= foreach join_T0_T1_T2 generate $0,$1,$2,$5,$8;
dump retail_join;

-- growth calculation
growth= foreach retail_join generate $0,$1,ROUND_TO(($3-$2)*100/$2,2),ROUND_TO(($4-$3)*100/$3,2);
--average growth  calculation
average= foreach  growth generate $0,$1,ROUND_TO(($2+$3)/2,2);dump average;

--1. Analyze the entire data set and arrive at products that have experienced a consolidated yearly avg growth of 10% or more in sales since 2000.
avg_growth_10= filter average by $2>=10;dump avg_growth_10;

--2. Analyze the entire data set and arrive at products that have experienced a consolidated yearly avg drop of 5% or more since 2000.
avg_growth_5= filter average by $2<=-5;dump avg_growth_5;

--3. Find top 5 products and bottom 5 products of overall sales for 3 years.
gross_sales= foreach retail_join generate $0,$1,($2+$3+$4) as grandtotal;

sorted_gross_sales= order gross_sales by grandtotal desc ;
top5= limit sorted_gross_sales  5;

sorted_gross_sales= order gross_sales by grandtotal asc ;
bottom5= limit sorted_gross_sales  5;

