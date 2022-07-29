SELECT COUNTY, count(*)
FROM DWH_PROD.DATA_SCIENCE.JOIN_DF
GROUP BY COUNTY;

SELECT county, count(*)
FROM DWH_PROD.REPORTING.AGG_PRETRIAL_KPI
GROUP BY county;

SELECT county, count(*)
FROM DWH_PROD.REPORTING.AGG_PRETRIAL_KPI_DOJ_ENHANCED 
GROUP BY county;

/*SELECT county, count(*)
FROM DWH_PROD.REPORTING.AGG_PRETRIAL_KPI_DOJ_ENHANCED 
GROUP BY county;*/

SELECT *, (n_join_df - n_agg_doj) AS dif, round(100*dif/n_agg_doj) AS perc_off
FROM (SELECT COUNTY, count(*) AS n_join_df
FROM DWH_PROD.DATA_SCIENCE.JOIN_DF
GROUP BY COUNTY) AS join_df
LEFT JOIN (SELECT county, count(*) AS n_agg_doj
FROM DWH_PROD.REPORTING.AGG_PRETRIAL_KPI_DOJ_ENHANCED 
GROUP BY county) AS agg_doj
ON join_df.county = agg_doj.county
ORDER BY perc_off desc;

SELECT doj.pretrial_period_end_date_doj, join_df.*
from "DWH_PROD"."DATA_SCIENCE"."JOIN_DF" AS join_df
LEFT JOIN DWH_PROD.REPORTING.AGG_PRETRIAL_KPI_DOJ_ENHANCED AS doj
	ON join_df.COUNTY = doj.COUNTY AND join_df.BOOKING_KEY = doj.BOOKING_KEY;
	
-- check join_df_std

SELECT *, (n_join_df - n_agg_doj) AS dif, round(100*dif/n_agg_doj) AS perc_off
FROM (SELECT COUNTY, count(*) AS n_join_df
FROM DWH_PROD.DATA_SCIENCE.JOIN_DF_STD
GROUP BY COUNTY) AS join_df
LEFT JOIN (SELECT county, count(*) AS n_agg_doj
FROM DWH_PROD.REPORTING.AGG_PRETRIAL_KPI_DOJ_ENHANCED 
GROUP BY county) AS agg_doj
ON join_df.county = agg_doj.county
ORDER BY perc_off desc;

-- arrest_felony_flag, arrest_misd_flag, arrest_violent_psa_flag, arrest_property_flag, arrest_drug_flag, arrest_dui_flag, arrest_dv_flag
                    
SELECT avg(arrest_violent_psa_flag), avg(arrest_property_flag), avg(arrest_drug_flag), avg(arrest_dui_flag), avg(arrest_dv_flag)
FROM DWH_PROD.DATA_SCIENCE.JOIN_DF_STD
GROUP BY county;

SELECT county, condition_em_flag, count(*)
FROM DWH_PROD.DATA_SCIENCE.JOIN_DF_STD
GROUP BY county, CONDITION_EM_FLAG 
;

SELECT county, arrest_violent_psa_flag, count(*)
FROM DWH_PROD.DATA_SCIENCE.JOIN_DF_STD
GROUP BY county, arrest_violent_psa_flag
;

SELECT county, 
	count(*) AS n, 
	round(100*avg(doj_matched)) AS doj_matched, 
	round(100*avg(recid_arrested_flag_doj)) AS recid_arrested, 
	round(100*avg(recid_filed_flag_doj)) AS recid_filed, 
	round(100*avg(recid_convicted_flag_doj)) AS recid_convicted,
	round(100*avg(recid_arrest_violent_psa_flag_doj)) AS recid_violent
FROM DWH_PROD.DATA_SCIENCE.JOIN_DF_STD
WHERE is_released_pretrial = 'Yes' AND pretrial_period_end_count_standard = 1
GROUP BY county;