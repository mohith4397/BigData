							--<<Retail Data>>			
--LOAD DATA
retail2000 = load '/home/mohith/DatasetsandCodes/PIG/RetailCaseStudy/2000.txt' using PigStorage(',') as (cat_id:int,cat_name:chararray,january:int,february:int,march:int,april:int,may:int,june:int,july:int,august:int,september:int,october:int,november:int,december:int);
retail2001 = load '/home/mohith/DatasetsandCodes/PIG/RetailCaseStudy/2001.txt' using PigStorage(',') as (cat_id:int,cat_name:chararray,january:int,february:int,march:int,april:int,may:int,june:int,july:int,august:int,september:int,october:int,november:int,december:int);
retail2002 = load '/home/mohith/DatasetsandCodes/PIG/RetailCaseStudy/2002.txt' using PigStorage(',') as (cat_id:int,cat_name:chararray,january:int,february:int,march:int,april:int,may:int,june:int,july:int,august:int,september:int,october:int,november:int,december:int);

--CREATE A SINGLE TUPLE WITH SUMS OF MONTHLY EARNINGS OF YEAR 2000

retail2000_group_all= group retail2000 all;
retail2000_monthlysum= foreach retail2000_group_all generate 
SUM(retail2000.january),
SUM(retail2000.february),
SUM(retail2000.march),
SUM(retail2000.april),
SUM(retail2000.may),
SUM(retail2000.june),
SUM(retail2000.july),
SUM(retail2000.august),
SUM(retail2000.september),
SUM(retail2000.october),
SUM(retail2000.november),
SUM(retail2000.december);

--CREATE A SINGLE TUPLE WITH SUMS OF MONTHLY EARNINGS OF YEAR 2001

retail2001_group_all= group retail2001 all;
retail2001_monthlysum= foreach retail2001_group_all generate 
SUM(retail2001.january),
SUM(retail2001.february),
SUM(retail2001.march),
SUM(retail2001.april),
SUM(retail2001.may),
SUM(retail2001.june),
SUM(retail2001.july),
SUM(retail2001.august),
SUM(retail2001.september),
SUM(retail2001.october),
SUM(retail2001.november),
SUM(retail2001.december);

--CREATE A SINGLE TUPLE WITH SUMS OF MONTHLY EARNINGS OF YEAR 2002

retail2002_group_all= group retail2002 all;
retail2002_monthlysum= foreach retail2002_group_all generate 
SUM(retail2002.january),
SUM(retail2002.february),
SUM(retail2002.march),
SUM(retail2002.april),
SUM(retail2002.may),
SUM(retail2002.june),
SUM(retail2002.july),
SUM(retail2002.august),
SUM(retail2002.september),
SUM(retail2002.october),
SUM(retail2002.november),
SUM(retail2002.december);

--CREATE 12 TUPLES FROM EVERY MONTHLY EARNINGS.(REWRITE ROWWISE DATA TO COLUMNWISE DATA)
monthly_2000 = foreach retail2000_monthlysum generate FLATTEN(TOBAG(*));
monthly_2001 = foreach retail2001_monthlysum generate FLATTEN(TOBAG(*));
monthly_2002 = foreach retail2002_monthlysum generate FLATTEN(TOBAG(*));

--ADD RANK(INDEX_NO) TO THE ABOVE SET FOR JOININING BY INDEX_NO

rank2000 = rank monthly_2000;
rank2001 = rank monthly_2001;
rank2002 = rank monthly_2002;

--JOIN THREE DATASETS BY RANK
monthlysalesyearwise= join rank2000 by $0, rank2001 by $0, rank2002 by $0;
monthlysalesyearwise= foreach monthlysalesyearwise generate $1,$3,$5;

--ADD TWO MORE COLUMNS(MONTHLY GROWTH AT THE END OF 2001,MONTHLY GROWTH AT THE END OF 2002)
monthlygrowth= foreach monthlysalesyearwise generate $0,$1,$2,ROUND_TO(($1-$0)/$1,2),ROUND_TO(($2-$1)/$2,2);

--ADD ONE MORE COLUMN TO THE ABOVE DATASET BY CALCULATIG THE AVERAGE MONTHLY GROWTH
answer= foreach  monthlygrowth generate $0,$1,$2,$3,$4,ROUND_TO(($3+$4)/2,2);
dump answer;

