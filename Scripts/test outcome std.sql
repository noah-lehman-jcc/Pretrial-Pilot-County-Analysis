SELECT *, (n_full_df - n_agg_doj) AS dif, round(100*dif/n_agg_doj) AS perc_off
FROM (SELECT COUNTY, count(*) AS n_full_df
FROM DWH_PROD.DATA_SCIENCE.CJS_OUTCOME_STD
GROUP BY COUNTY) AS full_df
LEFT JOIN (SELECT county, count(*) AS n_agg_doj
FROM DWH_PROD.REPORTING.AGG_PRETRIAL_KPI_DOJ_ENHANCED 
GROUP BY county) AS agg_doj
ON full_df.county = agg_doj.county
ORDER BY perc_off desc;


SELECT *
FROM DWH_PROD.DATA_SCIENCE.CJS_OUTCOME_STD
WHERE booking_key IN 
(SELECT BOOKING_KEY FROM (SELECT booking_key, count(*) AS n
FROM DWH_PROD.DATA_SCIENCE.CJS_OUTCOME_STD
GROUP BY booking_key) WHERE n>1);


SELECT *, round(100*doj_matched/total) AS percent_doj_matched
FROM( SELECT county, count(*) AS total, sum(doj_matched) AS doj_matched
FROM DWH_PROD.DATA_SCIENCE.CJS_OUTCOME_STD
GROUP BY county);
