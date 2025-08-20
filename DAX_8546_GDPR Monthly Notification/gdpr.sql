-- GDPR Monthly Notification
-- We will have to export the export file which gary provides us every month on s3 and add in union 

CREATE TABLE daas.dax_11985_gdpr_export_by_gary as
select DISTINCT * , 'december_file' as source_file FROM daas.gdpr_dec
union
SELECT DISTINCT * , 'january_file' as source_file FROM daas.gdpr_january
UNION
select DISTINCT * , 'february_file' as source_file FROM daas.gdpr_february
UNION
select DISTINCT * , 'march_file' as source_file FROM daas.gdpr_march
UNION
select DISTINCT * , 'april_file' as source_file FROM daas.gdpr_april
UNION
select DISTINCT * , 'may_file' as source_file FROM daas.gdpr_may
UNION
select DISTINCT * , 'june_july_file' as source_file FROM daas.gdpr_june_july
; 


--
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ 
-- Now we will take union of previous siz months delivery ( data which we provided to gery )
--

CREATE TABLE daas.previously_data_delivered_past_6_months as
SELECT DISTINCT * , 'december_export' as source_file from daas.gdpr_exported_december
UNION
SELECT DISTINCT *, 'january_export' as source_file from daas.gdpr_exported_january
UNION
SELECT DISTINCT * , 'february_export' as source_file from daas.exported_february
UNION
SELECT DISTINCT * , 'march_export' as source_file from daas.exported_march
UNION
SELECT DISTINCT *,  'april_export' as source_file from daas.exported_april
UNION
SELECT DISTINCT * , 'may_export' as source_file from daas.exported_may
;

--
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--

--CREATE OR REPLACE TABLE si_dataops_prod.daas.dax_11985_july_segment_ml_20250820 AS
SELECT DISTINCT T1.EmailAddress , CASE WHEN person_source = 'siq' THEN 'MV' WHEN person_source = 'research' THEN 'HV' END AS contact_status
, t1.ExportedBy
FROM
(
	SELECT T1.EmailAddress , T1.ExportedBy, t1.MatchId
	FROM daas.dax_11985_gdpr_export_by_gary AS T1
	INNER JOIN daas.master_gdpr_opt_out AS T2 ON LOWER(TRIM(T1.EmailAddress)) = LOWER(TRIM(T2.email))
) AS T1 
JOIN daas.person_top_positions_20250616_Fixed_GR AS T2 ON TRIM(SPLIT_PART(T1.MatchId, '-', 3)) = T2.person_id
--
UNION
--
SELECT DISTINCT EmailAddress , CASE WHEN SPLIT_PART(MatchId,'-', 2) = 'pr' THEN 'HV' WHEN SPLIT_PART(MatchId,'-', 2) = 'pm' THEN 'MV' END AS contact_status
, Exportedby
from
(
	SELECT DISTINCT TRIM('-' FROM REGEXP_SUBSTR(MatchId, '-[0-9]+')) AS person_id , T1.EmailAddress , T1.Exportedby ,MatchId
	from daas.dax_11985_gdpr_export_by_gary AS T1
	JOIN daas.master_gdpr_opt_out AS T2
	ON LOWER(TRIM(T1.EmailAddress)) = LOWER(TRIM(T2.email))
)
as subquerry
WHERE person_id NOT IN ( SELECT DISTINCT person_id from daas.person_top_positions_20250616_Fixed_GR )
;



--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- data which has already been delivered by us ,  we will suppress it.

CREATE TABLE daas.DAX_11985_TEMP_table_data_remove as
WITH CTE AS
(
	SELECT ExportedBy , EXPLODE(SPLIT(exported_contacts, ',')) AS email FROM daas.previously_data_delivered_past_6_months
	GROUP BY 1 ,2 
)
SELECT DISTINCT T1.*
FROM daas.dax_11985_july_segment_ml_20250820 AS T1
JOIN CTE AS T2 ON TRIM(LOWER(T1.EmailAddress)) = TRIM(LOWER(T2.email)) AND T1.ExportedBy = T2.ExportedBy
;

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 

ALTER TABLE daas.dax_11985_july_segment_ml_20250820 ADD COLUMN previously_delivered BOOLEAN ;

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 

UPDATE daas.dax_11985_july_segment_ml_20250820 SET previously_delivered = FALSE ; 

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 

MERGE INTO daas.dax_11985_july_segment_ml_20250820 AS T1 
USING
(
	select DISTINCT * from daas.DAX_11985_TEMP_table_data_remove
) as T2 ON TRIM(LOWER(T1.EmailAddress)) = TRIM(LOWER(T2.EmailAddress)) AND T1.ExportedBy = T2.ExportedBy
WHEN MATCHED THEN UPDATE SET previously_delivered = TRUE
; 

-- 10174
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--

DROP TABLE daas.DAX_11985_TEMP_table_data_remove ;

--
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--

CREATE OR REPLACE TABLE si_dataops_prod.daas.DAX_11820_july_segment_unique_emails_ml_20250819 AS
SELECT DISTINCT
	 EmailAddress
	,contact_status
	,ExportedBy 
FROM si_dataops_prod.daas.dax_11985_july_segment_ml_20250820
WHERE previously_delivered IS FALSE
;

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 
 	
SELECT COUNT(DISTINCT ExportedBy) from daas.DAX_11820_july_segment_unique_emails_ml_20250819 limit 100 ; 
	
--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Checking for the length as length should not exceed 65, 536 iF the length exceeds the limit then we will run the pyspark code on the notebook in data bricks

SELECT ExportedBy, exported_contacts , LENGTH(exported_contacts) FROM 
(
	WITH prep AS
	(  
	SELECT DISTINCT *, COUNT(EmailAddress) OVER(PARTITION BY exportedby) AS cnt 
	FROM si_dataops_prod.daas.dax_11985_july_segment_ml_20250820
	) 
	SELECT ExportedBy, ARRAY_JOIN(COLLECT_SET(EmailAddress), ', ') AS exported_contacts--public.group_concat(exported_contact) OVER(PARTITION BY PurchasedBy) AS exported_contacts
	FROM prep
	--WHERE cnt < 20
	GROUP BY 1    
	--UNION 
	--SELECT DISTINCT ExportedBy, WORK_EMAIL AS exported_contacts
	--FROM prep
	--WHERE cnt >= 20 
) AS SUBQUERRY 
ORDER BY LENGTH(exported_contacts) DESC 
; 

-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- if length does not exceed 65, 536 then we will deliver the data with this query else we will deliver the data from another query which is in the next lines

WITH prep AS
(
SELECT DISTINCT *, COUNT(EmailAddress) OVER(PARTITION BY exportedby) AS cnt 
FROM si_dataops_prod.daas.dax_11985_july_segment_ml_20250820
) 
SELECT ExportedBy, ARRAY_JOIN(COLLECT_SET(EmailAddress), ', ') AS exported_contacts--public.group_concat(exported_contact) OVER(PARTITION BY PurchasedBy) AS exported_contacts
FROM prep
GROUP BY 1    
; 

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--

--from pyspark.sql import functions as F
--from pyspark.sql import types as T
--
--# Step 1: Run your query into a DataFrame
--prep_df = spark.sql("""
--WITH prep AS (
--    SELECT DISTINCT *, COUNT(EmailAddress) OVER(PARTITION BY ExportedBy) AS cnt 
--    FROM si_dataops_prod.daas.dax_11985_july_segment_ml_20250820
--) 
--SELECT ExportedBy, ARRAY_JOIN(COLLECT_SET(EmailAddress), ',') AS exported_contacts
--FROM prep
--GROUP BY ExportedBy
--""")
--
--MAX_LEN = 65536
--
--# Step 2: Python function to split safely
--def split_contacts(contacts):
--    if contacts is None:
--        return [""]
--    
--    emails = contacts.split(",")
--    lines = []
--    current_line = ""
--
--    for email in emails:
--        email = email.strip()
--        if not email:
--            continue
--
--        if len(current_line) + len(email) + 1 > MAX_LEN:
--            lines.append(current_line.rstrip(","))
--            current_line = email
--        else:
--            current_line = (current_line + "," + email) if current_line else email
--
--    if current_line:
--        lines.append(current_line.rstrip(","))
--
--    return lines
--
--# Step 3: Register as UDF
--split_udf = F.udf(split_contacts, T.ArrayType(T.StringType()))
--
--# Step 4: Apply UDF → explode into multiple rows
--result_df = (
--    prep_df.withColumn("exported_contacts", split_udf(F.col("exported_contacts")))
--           .withColumn("exported_contacts", F.explode("exported_contacts"))
--)
--
--# Step 5: Save to new table
--result_df.write.mode("overwrite").saveAsTable(
--    "si_dataops_prod.daas.dax_11985_july_segment_ml_20250820_final"
--)
--; 

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- We will deliver the data after running the above code ( basically this code will make a new table which follow the rules of any cell that should exceed length )

SELECT * FROM daas.dax_11985_july_segment_ml_20250820_final ;

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--
















































































































