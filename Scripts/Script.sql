SELECT
County
,Booking_ID
,Booking_Key
,CII
,Booking_Date
,Release_Date
,Case_Filed_Date
,Case_Disposition_Date
,Pretrial_Period_End_Date
FROM "DWH_PROD"."REPORTING"."AGG_PRETRIAL_KPI"
--FROM DWH_PROD.DWH.Agg_Pretrial_KPI
WHERE CII IS NOT NULL;


SELECT *
from
(SELECT county, risk_level, risk_level_sort, CASE WHEN RECID_ARRESTED_FLAG_DOJ = 1 OR HAS_NEW_CRIMINAL_ACTIVITY = 'Yes' THEN 1 ELSE 0 END AS recid_all, count(*) AS n, n*100/sum(n) over(PARTITION BY (risk_level, county)) AS perc
FROM DWH_UAT.REPORTING.AGG_PRETRIAL_KPI_DOJ_ENHANCED
WHERE CII IS NOT NULL AND HAS_PRETRIAL_PERIOD_END = 'Yes' AND IS_BOOKING_ASSESSED = 'Yes' AND IS_RELEASED_PRETRIAL = 'Yes'
GROUP BY recid_all, risk_level, risk_level_sort, county
ORDER BY county, risk_level_sort)
WHERE recid_all = 1;

SELECT county, count(*) AS N
FROM DWH_UAT.REPORTING.AGG_PRETRIAL_KPI_DOJ_ENHANCED
WHERE CII IS NOT NULL 
AND IS_RELEASE_ELIGIBLE = 'Yes'
	AND HAS_PRETRIAL_PERIOD_END = 'Yes'
	AND IS_BOOKING_ASSESSED = 'Yes'
	AND IS_RELEASED_PRETRIAL = 'Yes'
GROUP BY county
ORDER BY N desc;



SELECT county, 
	count(*) AS Bookings, 
	SUM(BOOKING_ASSESSED_COUNT) AS Assessed_Bookings, 
	SUM(PRETRIAL_PERIOD_END_COUNT*BOOKING_ASSESSED_COUNT) AS Pretrial_Complete,
	SUM(RELEASED_PRETRIAL_COUNT*PRETRIAL_PERIOD_END_COUNT*BOOKING_ASSESSED_COUNT) AS Released_Pretrial
FROM DWH_UAT.REPORTING.AGG_PRETRIAL_KPI_DOJ_ENHANCED
GROUP BY county
ORDER BY Bookings DESC;

SELECT TOOL_NAME, 
	count(*) AS Assessed_Bookings, 
	SUM(PRETRIAL_PERIOD_END_COUNT) AS Pretrial_Complete,
	SUM(RELEASED_PRETRIAL_COUNT*PRETRIAL_PERIOD_END_COUNT) AS Released_Pretrial
FROM DWH_UAT.REPORTING.AGG_PRETRIAL_KPI_DOJ_ENHANCED
WHERE IS_BOOKING_ASSESSED = 'Yes'
GROUP BY TOOL_NAME 
ORDER BY Assessed_Bookings DESC;

SELECT county_name AS county, count(*) AS Assessments
FROM DWH_PROD.DATA_SCIENCE.CJS_DF_STD
GROUP BY county_name
ORDER BY Assessments desc;

SELECT doj_agg.county, doj_agg.bookings, df.assessments, doj_agg.assessed_bookings, doj_agg.pretrial_complete, doj_agg.released_pretrial
FROM (SELECT county, 
	count(*) AS Bookings, 
	SUM(BOOKING_ASSESSED_COUNT) AS Assessed_Bookings, 
	SUM(PRETRIAL_PERIOD_END_COUNT*BOOKING_ASSESSED_COUNT) AS Pretrial_Complete,
	SUM(RELEASED_PRETRIAL_COUNT*PRETRIAL_PERIOD_END_COUNT*BOOKING_ASSESSED_COUNT) AS Released_Pretrial
FROM DWH_UAT.REPORTING.AGG_PRETRIAL_KPI_DOJ_ENHANCED
GROUP BY county
ORDER BY Bookings DESC) AS doj_agg
LEFT JOIN (SELECT county_name , count(*) AS Assessments
FROM DWH_PROD.DATA_SCIENCE.CJS_DF_STD
GROUP BY county_name
ORDER BY Assessments DESC) AS df on doj_agg.county = df.county_name;

-- race count by county



SELECT *
FROM (SELECT county,
	case        
        when ethnicity='Hispanic' OR race IN ('Hispanic', 'CUBA', 'LAT', 'HISPANIC', 'MEX', 'HIS') then 'Hispanic/Latino'
        when race IN ('ASIA', 'Filipino', 'Korean' ,'Loatian','Other Asian', 
                      'PACIFIC ISLANDER', 'PACIFIC ISLANDR', 'GUAMANIAN' ,  'Guamanian',  'CAMBODIAN','KOREAN','CHINESE','FILIPINO','SAMOAN','Cambodian',
                      'ASIAN INDIAN','Hawaiian','VIETNAMESE','Japanese','HAWAIIAN','Pacific Islander','Samoan','Vietnamese','LAOTIAN',
                      'JAPANESE','Chinese','Laotian' ,'Asian Indian') then 'AsianPI'    
        when race IN ('Black', 'BLACK' ,'AFR') then 'BlackAA'    
        when race IN ('White', 'WHITE' ,'ENG')  then 'White'   
        when race IN ('American Indian', 'AMERICAN INDIAN')  then 'American Indian'
        else 'Other/Unk/Undef'                                         
 	end as race_eth,
 	count(*) AS N
 from "DWH_UAT"."REPORTING"."AGG_PRETRIAL_KPI"
  WHERE CII IS NOT NULL AND HAS_PRETRIAL_PERIOD_END = 'Yes' AND IS_BOOKING_ASSESSED = 'Yes' AND IS_RELEASED_PRETRIAL = 'Yes' 
 GROUP BY race_eth, COUNTY ) AS race_std
 	pivot (sum(N) FOR race_eth IN ('Hispanic/Latino', 'AsianPI', 'BlackAA', 'White', 'American Indian', 'Other/Unk/Undef')) AS pvt_tab;
 

-- race count by tool

SELECT *
FROM (SELECT tool_name,
	case        
        when ethnicity='Hispanic' OR race IN ('Hispanic', 'CUBA', 'LAT', 'HISPANIC', 'MEX', 'HIS') then 'Hispanic/Latino'
        when race IN ('ASIA', 'Filipino', 'Korean' ,'Loatian','Other Asian', 
                      'PACIFIC ISLANDER', 'PACIFIC ISLANDR', 'GUAMANIAN' ,  'Guamanian',  'CAMBODIAN','KOREAN','CHINESE','FILIPINO','SAMOAN','Cambodian',
                      'ASIAN INDIAN','Hawaiian','VIETNAMESE','Japanese','HAWAIIAN','Pacific Islander','Samoan','Vietnamese','LAOTIAN',
                      'JAPANESE','Chinese','Laotian' ,'Asian Indian') then 'AsianPI'    
        when race IN ('Black', 'BLACK' ,'AFR') then 'BlackAA'    
        when race IN ('White', 'WHITE' ,'ENG')  then 'White'   
        when race IN ('American Indian', 'AMERICAN INDIAN')  then 'American Indian'
        else 'Other/Unk/Undef'                                         
 	end as race_eth,
 	count(*) AS N
 from "DWH_UAT"."REPORTING"."AGG_PRETRIAL_KPI"
  WHERE CII IS NOT NULL AND HAS_PRETRIAL_PERIOD_END = 'Yes' AND IS_BOOKING_ASSESSED = 'Yes' AND IS_RELEASED_PRETRIAL = 'Yes' 
 GROUP BY race_eth, tool_name ) AS race_std
 	pivot (sum(N) FOR race_eth IN ('Hispanic/Latino', 'AsianPI', 'BlackAA', 'White', 'American Indian', 'Other/Unk/Undef')) AS pvt_tab;
 
 
select county, tool_name, count(*)
from "DWH_PROD"."REPORTING"."AGG_PRETRIAL_KPI"
where is_booking_assessed = 'Yes'
group by county, tool_name
ORDER BY county;

SELECT dim_assessment_tool_key, risk_score_raw, count(*)
FROM "42_SANTABARBARA-DWH_PROD".SECURE_SHARE.FACT_PRETRIAL_ASSESSMENT
--WHERE DIM_ASSESSMENT_TOOL_KEY IN (36, 38)
GROUP BY DIM_ASSESSMENT_TOOL_KEY , RISK_SCORE_RAW 
ORDER BY DIM_ASSESSMENT_TOOL_KEY, RISK_SCORE_RAW ;

-- vprai tool distribution
SELECT CASE WHEN RISK_SCORE_RAW >=10 THEN 'Over 10' 
						WHEN RISK_SCORE_RAW >=5 THEN 'Score 5-9'
						WHEN RISK_SCORE_RAW = 4 THEN 'Score 4'
						WHEN RISK_SCORE_RAW = 3 THEN 'Score 3'
						WHEN RISK_SCORE_RAW = 2 THEN 'Score 2'
						WHEN RISK_SCORE_RAW IN (0,1) THEN 'Score 0-1' END AS risk_score_group, count(*)
FROM DWH_PROD.DATA_SCIENCE.CJS_DF_STD
WHERE tool_name IN ('VPRAI', 'Virginia Pretrial Risk Assessment')
GROUP BY risk_score_group
ORDER BY risk_score_group;

