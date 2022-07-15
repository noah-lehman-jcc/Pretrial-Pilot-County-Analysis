SELECT county_name, total, missing_gender, round(100*missing_gender/total) AS missing_gender_perc, missing_race, round(100*missing_race/total) AS missing_race_perc
FROM (SELECT county_name, count(*) AS total, sum(CASE WHEN sex IN ('Other/Unknown', 'Unknown', 'Other Unknown', 'Undefined', 'Unk' )THEN 1 ELSE 0 end) AS missing_gender, sum(CASE WHEN race_standard = 'Other/Unknown' THEN 1 ELSE 0 end) AS missing_race
FROM DWH_PROD.DATA_SCIENCE.CJS_DF_STD
GROUP BY county_name)
ORDER BY missing_race_perc DESC, missing_gender_perc DESC;

SELECT RACE_STANDARD 
FROM DWH_PROD.DATA_SCIENCE.CJS_DF_STD
GROUP BY RACE_STANDARD ;

SELECT SEX 
FROM DWH_PROD.DATA_SCIENCE.CJS_DF_STD
GROUP BY sex;

SELECT county_name, total, missing_gender, round(100*missing_gender/total) AS missing_gender_perc, missing_race, round(100*missing_race/total) AS missing_race_perc,
	missing_both, round(100*missing_both/total) AS missing_both_perc
FROM (SELECT county_name, count(*) AS total, 
		sum(CASE WHEN sex IN ('Other/Unknown', 'Unknown', 'Other Unknown', 'Undefined', 'Unk' )THEN 1 ELSE 0 end) AS missing_gender, 
		sum(CASE WHEN race_standard = 'Other/Unknown' THEN 1 ELSE 0 end) AS missing_race,
		sum(CASE WHEN sex IN ('Other/Unknown', 'Unknown', 'Other Unknown', 'Undefined', 'Unk' ) AND race_standard = 'Other/Unknown' THEN 1 ELSE 0 end) AS missing_both
FROM DWH_PROD.DATA_SCIENCE.CJS_DF_STD
GROUP BY county_name)
ORDER BY missing_race_perc DESC, missing_gender_perc DESC;