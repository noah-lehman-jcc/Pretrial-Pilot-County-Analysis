-- Dropoff tables for looking at numbers available for validation studies

-- by county
SELECT county, 
	count(*) AS Bookings, 
	SUM(BOOKING_ASSESSED_COUNT) AS Assessed_Bookings, 
	SUM(PRETRIAL_PERIOD_END_COUNT*BOOKING_ASSESSED_COUNT) AS Pretrial_Complete,
	SUM(RELEASED_PRETRIAL_COUNT*PRETRIAL_PERIOD_END_COUNT*BOOKING_ASSESSED_COUNT) AS Released_Pretrial
FROM DWH_UAT.REPORTING.AGG_PRETRIAL_KPI_DOJ_ENHANCED
GROUP BY county
ORDER BY Bookings DESC;

-- by tool
SELECT TOOL_NAME, 
	count(*) AS Assessed_Bookings, 
	SUM(PRETRIAL_PERIOD_END_COUNT) AS Pretrial_Complete,
	SUM(RELEASED_PRETRIAL_COUNT*PRETRIAL_PERIOD_END_COUNT) AS Released_Pretrial
FROM DWH_UAT.REPORTING.AGG_PRETRIAL_KPI_DOJ_ENHANCED
WHERE IS_BOOKING_ASSESSED = 'Yes'
GROUP BY TOOL_NAME 
ORDER BY Assessed_Bookings DESC;

-- including all assessments, including those not matched to bookings
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


---------------------------------------

-- look at subgroup counts for bias validation studies to see if subgroups are of sufficient size

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
 

-- race count by tools
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
