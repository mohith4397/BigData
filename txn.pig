txn= LOAD '/home/mohith/DatasetsandCodes/Mapreduce/txns1.txt' USING PigStorage(',') AS (txnno:int,txndate:chararray,custno:int,amount:double,category:chararray,product:chararray,city:chararray,state:chararray,type:chararray);
groupbytype= group txn by type;
salesbytype= foreach  groupbytype generate group as type, SUM(txn.amount) as typetotal;
totalsalesgroup= group salesbytype all;
totalsales = foreach totalsalesgroup generate SUM(salesbytype.typetotal) as total;
percentage_step1= foreach salesbytype generate $0,$1,totalsales.$0 as percentage;
percentage_final= foreach percentage_step1 generate $0,(($1*100)/$2);
dump percentage_final;
