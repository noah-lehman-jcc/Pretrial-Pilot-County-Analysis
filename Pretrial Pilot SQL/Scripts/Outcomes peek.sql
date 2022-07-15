SELECT tool, tot, risk_score_comb, perc_doj_arrest_recid
FROM (SELECT CASE WHEN df.tool_name IN ('VPRAI', 'Virginia Pretrial Risk Assessment', 
									'Virginia Pretrial Risk Assessment_Old', 'Tyler Supervision and VPRAI') THEN 'VPRAI'
		    WHEN df.tool_name IN ('ORAS', 'ORAS-PAT') THEN 'ORAS'
		    WHEN df.tool_name IN ('Other', 'Unknown') THEN 'Other/Unknown'
		    ELSE df.tool_name END AS tool,
	CASE WHEN tool != 'PSA' THEN risk_score_raw
		 WHEN tool = 'PSA' THEN score_new_criminal_activity END AS risk_score_comb, 
	count(*) AS n,
	sum(n) OVER 
	 (PARTITION BY
	 	tool, risk_score_comb
	 	) AS tot,
	(100*n)/tot AS perc_doj_arrest_recid,
          recid_arrested_flag_doj 
FROM DWH_PROD.DATA_SCIENCE.CJS_DF_STD AS df
LEFT JOIN DWH_UAT.REPORTING.AGG_PRETRIAL_KPI_DOJ_ENHANCED AS doj ON df.booking_key = doj.booking_key
WHERE recid_arrested_flag_doj IS NOT NULL AND risk_score_comb IS NOT NULL 
GROUP BY tool, risk_score_comb, recid_arrested_flag_doj
ORDER BY tool, risk_score_comb, recid_arrested_flag_doj)
WHERE recid_arrested_flag_doj = 1;

SELECT count(*)
FROM DWH_PROD.DATA_SCIENCE.CJS_DF_ENH AS df; --360,867

SELECT count(*)
FROM DWH_PROD.DATA_SCIENCE.CJS_DF_ENH AS df
WHERE BOOKING_KEY IS NOT NULL;  --324,301

SELECT doj_matched, count(*)
FROM DWH_PROD.DATA_SCIENCE.CJS_DF_ENH AS df
WHERE BOOKING_KEY IS NOT NULL
GROUP BY DOJ_MATCHED ;  -- 51,742 MATCHED, 272,559 NOT MATCHED 

SELECT count(*)
FROM DWH_PROD.DATA_SCIENCE.CJS_DF_ENH AS df
WHERE 
	BOOKING_KEY IS NOT NULL 
	AND RELEASE_ELIGIBLE_COUNT = 1
	AND BOOKING_ASSESSED_COUNT = 1
	AND RELEASED_PRETRIAL_COUNT = 1
	AND (CASE WHEN PRETRIAL_PERIOD_END_DATE_DOJ IS NOT NULL THEN 1
					  WHEN PRETRIAL_PERIOD_END_COUNT = 1 THEN 1
					  ELSE 0 END) = 1; -- 33,299

					  
----- using new view

-- for non-psa tools - recid
SELECT tool, RISK_SCORE_RAW, tot, perc_recid_county_or_doj
FROM (SELECT CASE WHEN tool_name IN ('VPRAI', 'Virginia Pretrial Risk Assessment', 
									'Virginia Pretrial Risk Assessment_Old', 'Tyler Supervision and VPRAI') THEN 'VPRAI'
		    WHEN tool_name IN ('ORAS', 'ORAS-PAT') THEN 'ORAS'
		    WHEN tool_name IN ('Other', 'Unknown') THEN 'Other/Unknown'
		    ELSE tool_name END AS tool,
		    RISK_SCORE_RAW,
		    RECID_COUNTY_OR_DOJ, 
		    count(*) AS n,
			sum(n) OVER 
	 		(PARTITION BY
	 			tool, RISK_SCORE_RAW 
	 		) AS tot,
			(100*n)/tot AS perc_recid_county_or_doj
	FROM DWH_PROD.DATA_SCIENCE.CJS_DF_ENH AS df
	WHERE 
		BOOKING_KEY IS NOT NULL 
		AND RELEASE_ELIGIBLE_COUNT = 1
		AND BOOKING_ASSESSED_COUNT = 1
		AND RELEASED_PRETRIAL_COUNT = 1
		AND (CASE WHEN PRETRIAL_PERIOD_END_DATE_DOJ IS NOT NULL THEN 1
					  WHEN PRETRIAL_PERIOD_END_COUNT = 1 THEN 1
					  ELSE 0 END) = 1
		AND tool != 'PSA'
		AND RISK_SCORE_RAW IS NOT NULL 
	GROUP BY tool, RISK_SCORE_RAW, 
			   RECID_COUNTY_OR_DOJ
			   )
WHERE recid_county_or_doj = 1
ORDER BY tool, RISK_SCORE_RAW; 

-- non-psa tools FTA
SELECT tool, RISK_SCORE_RAW, tot, perc_fta_county_or_doj
FROM (SELECT CASE WHEN tool_name IN ('VPRAI', 'Virginia Pretrial Risk Assessment', 
									'Virginia Pretrial Risk Assessment_Old', 'Tyler Supervision and VPRAI') THEN 'VPRAI'
		    WHEN tool_name IN ('ORAS', 'ORAS-PAT') THEN 'ORAS'
		    WHEN tool_name IN ('Other', 'Unknown') THEN 'Other/Unknown'
		    ELSE tool_name END AS tool,
		    RISK_SCORE_RAW,
		    count(*) AS n,
			sum(n) OVER 
	 		(PARTITION BY
	 			tool, RISK_SCORE_RAW 
	 		) AS tot,
			(100*n)/tot AS perc_fta_county_or_doj,
		    FTA_COUNTY_OR_DOJ 
	FROM DWH_PROD.DATA_SCIENCE.CJS_DF_ENH AS df
	WHERE 
		BOOKING_KEY IS NOT NULL 
		AND RELEASE_ELIGIBLE_COUNT = 1
		AND BOOKING_ASSESSED_COUNT = 1
		AND RELEASED_PRETRIAL_COUNT = 1
		AND (CASE WHEN PRETRIAL_PERIOD_END_DATE_DOJ IS NOT NULL THEN 1
						  WHEN PRETRIAL_PERIOD_END_COUNT = 1 THEN 1
						  ELSE 0 END) = 1
		AND tool != 'PSA'
		AND RISK_SCORE_RAW IS NOT NULL 
	GROUP BY tool, RISK_SCORE_RAW, 
				   FTA_COUNTY_OR_DOJ 
				   )
WHERE fta_county_or_doj = 1
ORDER BY tool, RISK_SCORE_RAW; 

-- FOR psa nca
SELECT tool, score_new_criminal_activity, tot, perc_recid_county_or_doj
FROM (SELECT CASE WHEN tool_name IN ('VPRAI', 'Virginia Pretrial Risk Assessment', 
									'Virginia Pretrial Risk Assessment_Old', 'Tyler Supervision and VPRAI') THEN 'VPRAI'
		    WHEN tool_name IN ('ORAS', 'ORAS-PAT') THEN 'ORAS'
		    WHEN tool_name IN ('Other', 'Unknown') THEN 'Other/Unknown'
		    ELSE tool_name END AS tool,
		    SCORE_NEW_CRIMINAL_ACTIVITY,
		    RECID_COUNTY_OR_DOJ,
		    count(*) AS n,
			sum(n) OVER 
	 		(PARTITION BY
	 			tool, SCORE_NEW_CRIMINAL_ACTIVITY 
	 		) AS tot,
			(100*n)/tot AS perc_recid_county_or_doj
	FROM DWH_PROD.DATA_SCIENCE.CJS_DF_ENH AS df
	WHERE 
		BOOKING_KEY IS NOT NULL 
		AND RELEASE_ELIGIBLE_COUNT = 1
		AND BOOKING_ASSESSED_COUNT = 1
		AND RELEASED_PRETRIAL_COUNT = 1
		AND (CASE WHEN PRETRIAL_PERIOD_END_DATE_DOJ IS NOT NULL THEN 1
						  WHEN PRETRIAL_PERIOD_END_COUNT = 1 THEN 1
						  ELSE 0 END) = 1
		AND TOOL = 'PSA'
		AND SCORE_NEW_CRIMINAL_ACTIVITY IS NOT NULL 
	GROUP BY tool, SCORE_NEW_CRIMINAL_ACTIVITY, RECID_COUNTY_OR_DOJ
	)
WHERE recid_county_or_doj = 1
ORDER BY tool, SCORE_NEW_CRIMINAL_ACTIVITY; 

-- for PSA fta
SELECT tool, score_failure_to_appear, tot, perc_fta_county_or_doj
FROM (SELECT CASE WHEN tool_name IN ('VPRAI', 'Virginia Pretrial Risk Assessment', 
									'Virginia Pretrial Risk Assessment_Old', 'Tyler Supervision and VPRAI') THEN 'VPRAI'
		    WHEN tool_name IN ('ORAS', 'ORAS-PAT') THEN 'ORAS'
		    WHEN tool_name IN ('Other', 'Unknown') THEN 'Other/Unknown'
		    ELSE tool_name END AS tool,
		    SCORE_FAILURE_TO_APPEAR,
		    FTA_COUNTY_OR_DOJ,
		    count(*) AS n,
			sum(n) OVER 
	 		(PARTITION BY
	 			tool, SCORE_FAILURE_TO_APPEAR
	 		) AS tot,
			(100*n)/tot AS perc_fta_county_or_doj
	FROM DWH_PROD.DATA_SCIENCE.CJS_DF_ENH AS df
	WHERE 
		BOOKING_KEY IS NOT NULL 
		AND RELEASE_ELIGIBLE_COUNT = 1
		AND BOOKING_ASSESSED_COUNT = 1
		AND RELEASED_PRETRIAL_COUNT = 1
		AND (CASE WHEN PRETRIAL_PERIOD_END_DATE_DOJ IS NOT NULL THEN 1
						  WHEN PRETRIAL_PERIOD_END_COUNT = 1 THEN 1
						  ELSE 0 END) = 1
		AND tool = 'PSA'
		AND SCORE_FAILURE_TO_APPEAR IS NOT NULL 
	GROUP BY tool, SCORE_FAILURE_TO_APPEAR, FTA_COUNTY_OR_DOJ 
	)
WHERE fta_county_or_doj = 1
ORDER BY tool, SCORE_FAILURE_TO_APPEAR ; 

SELECT release_type, count(*)
FROM DWH_PROD.DATA_SCIENCE.OUTCOME_DF
GROUP BY RELEASE_TYPE;

SELECT CASE WHEN pretrial_period_end_date_doj IS NULL THEN 0 ELSE 1 END AS has_pretrial_end_doj, pretrial_period_end_count, PRETRIAL_PERIOD_END_COUNT_STANDARD, count(*)
FROM DWH_PROD.DATA_SCIENCE.CJS_OUTCOME_STD 
GROUP BY has_pretrial_end_doj, PRETRIAL_PERIOD_END_COUNT, PRETRIAL_PERIOD_END_COUNT_STANDARD ;

SELECT release_type, count(*) AS n, sum(n) OVER (PARTITION BY 1) AS tot, (100*n)/tot AS perc
FROM DWH_PROD.DATA_SCIENCE.CJS_OUTCOME_STD 
GROUP BY release_type
ORDER BY perc desc;

