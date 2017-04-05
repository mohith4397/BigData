-- Join weblog and gateway tables and calculate  the average success transaction for a user.Display the tuples greater than 90.

gateway= load '/home/mohith/DatasetsandCodes/PIG/gateway' using PigStorage() as (bank:chararray,successrate:float);
weblog= load '/home/mohith/DatasetsandCodes/PIG/weblog' using PigStorage() as (name:chararray,bank:chararray,time:float);
gatewayplusweblog= join gateway by bank,weblog by bank;
gatewayplusweblog= foreach gatewayplusweblog generate $2,$0,$1;
dump gatewayplusweblog;
groupbyname= group gatewayplusweblog by name;
dump groupbyname;
describe groupbyname;
step1= foreach groupbyname generate group,AVG(gatewayplusweblog.$2);
answer= filter step1 by $1>90;
dump answer;

