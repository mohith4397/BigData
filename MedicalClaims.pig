-- Calculate Average Claims by person and department.
medical_data= load '/home/mohith/DatasetsandCodes/PIG/medical' using PigStorage() as (name:chararray,dept:chararray,claim:float);

groupbycat= group medical_data by name;
total_claim= foreach groupbycat generate group,SUM(medical_data.claim) as gross,COUNT(medical_data.claim) as NOofClaims;
avg_claim_by_person=  foreach total_claim generate group,gross/NOofClaims;

groupbycat= group medical_data by dept;
total_claim= foreach groupbycat generate group,SUM(medical_data.claim) as gross,COUNT(medical_data.claim) as NOofClaims;
avg_claim_by_dept=  foreach total_claim generate group,gross/NOofClaims;

dump avg_claim_by_dept;
dump avg_claim_by_person;
