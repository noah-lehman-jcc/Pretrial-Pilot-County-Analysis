SELECT COUNTY, charge_level_standard, count(booking_count) AS N, AVG(time_to_end) AS avg_days_to_end, year(BOOKING_DATE) AS book_year
FROM (
	SELECT COUNTY, CHARGE_LEVEL, CHARGE_LEVEL_SORT, BOOKING_TYPE, RELEASE_TYPE, BOOKING_DATE, RELEASE_DATE, ARRAIGNMENT_DATE, PRETRIAL_PERIOD_END_DATE,  
	 IS_BOOKING, BOOKING_COUNT, IS_RELEASE_ELIGIBLE, RELEASE_ELIGIBLE_COUNT,  IS_RELEASED, RELEASED_COUNT,
	 IS_RELEASED_PREARRAIGNMENT, RELEASED_PREARRAIGNMENT_COUNT, IS_RELEASED_PRETRIAL, RELEASED_PRETRIAL_COUNT, 
	 HAS_PRETRIAL_PERIOD_END, PRETRIAL_PERIOD_END_COUNT, IS_UNMATCHED_BOOKING, UNMATCHED_BOOKING_COUNT, DAYS_IN_PRETRIAL_PERIOD, 
	 CASE_FILED_DATE, CASE_DISPOSITION_DATE, (RELEASE_DATE - BOOKING_DATE) AS time_to_release, (PRETRIAL_PERIOD_END_DATE - BOOKING_DATE) AS time_to_end,
	 (release_date - pretrial_period_end_date) AS test,
	 CASE
          WHEN charge_level in ('Felony', 'F') THEN 'F'
          WHEN charge_level in ('Misdemeanor', 'M') THEN 'M'
          WHEN charge_level in ('Infraction', 'I') THEN 'I'
          ELSE 'Other/Unknown'
     END AS charge_level_standard
	FROM DWH_PROD.REPORTING.AGG_PRETRIAL_KPI
	)
WHERE is_release_eligible = 'Yes' 
	AND is_released_pretrial = 'No' 
--	AND release_date IS NOT NULL  -- don'T need this IF we ARE looking AT tiem TO dispo instead OF time TO release
	AND is_unmatched_booking = 'No' 
	AND charge_level_standard IN ('F', 'M') 
	AND HAS_PRETRIAL_PERIOD_END = 'Yes'
GROUP BY COUNTY, charge_level_standard, book_year
ORDER BY COUNTY, charge_level_standard, book_year;
------------------------------------------------------------

-- age categories for Deirdre

SELECT age_cat, N 
FROM (SELECT CASE WHEN individual_age >= 18 AND INDIVIDUAL_AGE  <= 25 THEN '18-25' 
			WHEN INDIVIDUAL_AGE >= 26 AND INDIVIDUAL_AGE  <= 35 THEN '26-35' 
			WHEN INDIVIDUAL_AGE >= 36 AND INDIVIDUAL_AGE  <= 45 THEN '36-45'
			WHEN INDIVIDUAL_AGE >= 46 AND INDIVIDUAL_AGE  <= 55 THEN '46-55'
			WHEN INDIVIDUAL_AGE > 55 THEN '56 +'
			ELSE 'Other' END AS age_cat, 
	   CASE WHEN individual_age >= 18 AND INDIVIDUAL_AGE  <= 25 THEN 1 
			WHEN INDIVIDUAL_AGE >= 26 AND INDIVIDUAL_AGE  <= 35 THEN 2
			WHEN INDIVIDUAL_AGE >= 36 AND INDIVIDUAL_AGE  <= 45 THEN 3
			WHEN INDIVIDUAL_AGE >= 46 AND INDIVIDUAL_AGE  <= 55 THEN 4
			WHEN INDIVIDUAL_AGE > 55 THEN 5
			ELSE 6 END AS age_order,
			count(*) AS N
FROM DWH_PROD.REPORTING.AGG_PRETRIAL_KPI
GROUP BY age_cat, age_order
ORDER BY age_order)
ORDER BY age_order;
------------------------------------------------------


-- COLLAPSED STATEWIDE
SELECT charge_level_standard, count(booking_count) AS N, AVG(time_to_end) AS avg_days_to_end, round(N*avg_days_to_end) AS tot_person_days, year(BOOKING_DATE) AS book_year, month(BOOKING_DATE) AS book_month
FROM (
	SELECT COUNTY, CHARGE_LEVEL, CHARGE_LEVEL_SORT, BOOKING_TYPE, RELEASE_TYPE, BOOKING_DATE, RELEASE_DATE, ARRAIGNMENT_DATE, PRETRIAL_PERIOD_END_DATE,  
	 IS_BOOKING, BOOKING_COUNT, IS_RELEASE_ELIGIBLE, RELEASE_ELIGIBLE_COUNT,  IS_RELEASED, RELEASED_COUNT,
	 IS_RELEASED_PREARRAIGNMENT, RELEASED_PREARRAIGNMENT_COUNT, IS_RELEASED_PRETRIAL, RELEASED_PRETRIAL_COUNT, 
	 HAS_PRETRIAL_PERIOD_END, PRETRIAL_PERIOD_END_COUNT, IS_UNMATCHED_BOOKING, UNMATCHED_BOOKING_COUNT, DAYS_IN_PRETRIAL_PERIOD, 
	 CASE_FILED_DATE, CASE_DISPOSITION_DATE, (RELEASE_DATE - BOOKING_DATE) AS time_to_release, (PRETRIAL_PERIOD_END_DATE - BOOKING_DATE) AS time_to_end,
	 (release_date - pretrial_period_end_date) AS test,
	 CASE
          WHEN charge_level in ('Felony', 'F') THEN 'F'
          WHEN charge_level in ('Misdemeanor', 'M') THEN 'M'
          WHEN charge_level in ('Infraction', 'I') THEN 'I'
          ELSE 'Other/Unknown'
     END AS charge_level_standard
	FROM DWH_PROD.REPORTING.AGG_PRETRIAL_KPI
	)
WHERE is_release_eligible = 'Yes' 
	AND is_released_pretrial = 'No' 
--	AND release_date IS NOT NULL  -- don'T need this IF we ARE looking AT tiem TO dispo instead OF time TO release
	AND is_unmatched_booking = 'No' 
	AND charge_level_standard IN ('F', 'M') 
	AND HAS_PRETRIAL_PERIOD_END = 'Yes'
--	AND COUNTY != 'Los Angeles'
	AND BOOKING_DATE >= '07/01/2018'
GROUP BY charge_level_standard, book_year, book_month
ORDER BY charge_level_standard, book_year, book_month;

-----
-- include people not released before trial time til release

SELECT charge_level_standard, count(booking_count) AS N, AVG(time_to_end_or_release) AS avg_days_to_end_or_release, year(BOOKING_DATE) AS book_year
FROM (
	SELECT COUNTY, CHARGE_LEVEL, CHARGE_LEVEL_SORT, BOOKING_TYPE, RELEASE_TYPE, BOOKING_DATE, RELEASE_DATE, ARRAIGNMENT_DATE, PRETRIAL_PERIOD_END_DATE,  
	 IS_BOOKING, BOOKING_COUNT, IS_RELEASE_ELIGIBLE, RELEASE_ELIGIBLE_COUNT,  IS_RELEASED, RELEASED_COUNT,
	 IS_RELEASED_PREARRAIGNMENT, RELEASED_PREARRAIGNMENT_COUNT, IS_RELEASED_PRETRIAL, RELEASED_PRETRIAL_COUNT, 
	 HAS_PRETRIAL_PERIOD_END, PRETRIAL_PERIOD_END_COUNT, IS_UNMATCHED_BOOKING, UNMATCHED_BOOKING_COUNT, DAYS_IN_PRETRIAL_PERIOD, 
	 CASE_FILED_DATE, CASE_DISPOSITION_DATE, (RELEASE_DATE - BOOKING_DATE) AS time_to_release, (PRETRIAL_PERIOD_END_DATE - BOOKING_DATE) AS time_to_end,
	 (release_date - pretrial_period_end_date) AS test, CASE WHEN is_released_pretrial = 'Yes' THEN time_to_release ELSE time_to_end END AS time_to_end_or_release,
	 CASE
          WHEN charge_level in ('Felony', 'F') THEN 'F'
          WHEN charge_level in ('Misdemeanor', 'M') THEN 'M'
          WHEN charge_level in ('Infraction', 'I') THEN 'I'
          ELSE 'Other/Unknown'
     END AS charge_level_standard
	FROM DWH_PROD.REPORTING.AGG_PRETRIAL_KPI
	)
WHERE is_release_eligible = 'Yes' 
--	AND is_released_pretrial = 'No' 
--	AND release_date IS NOT NULL  -- don'T need this IF we ARE looking AT tiem TO dispo instead OF time TO release
	AND is_unmatched_booking = 'No' 
	AND charge_level_standard IN ('F', 'M') 
	AND HAS_PRETRIAL_PERIOD_END = 'Yes'
GROUP BY charge_level_standard, book_year
ORDER BY charge_level_standard, book_year;


---------------


SELECT county, 
	 CASE
          WHEN charge_level in ('Felony', 'F') THEN 'F'
          WHEN charge_level in ('Misdemeanor', 'M') THEN 'M'
          WHEN charge_level in ('Infraction', 'I') THEN 'I'
          ELSE 'Other/Unknown'
     END AS charge_level_standard, 
      avg((CASE WHEN PRETRIAL_PERIOD_END_DATE IS NOT NULL THEN PRETRIAL_PERIOD_END_DATE ELSE CAST(GETDATE() AS date) END) - booking_date),
      year(BOOKING_DATE) AS book_year
FROM DWH_PROD.REPORTING.AGG_PRETRIAL_KPI
WHERE is_release_eligible = 'Yes' 
	AND is_released_pretrial = 'No' 
--	AND release_date IS NOT NULL  -- don'T need this IF we ARE looking AT tiem TO dispo instead OF time TO release
	AND is_unmatched_booking = 'No' 
	AND charge_level_standard IN ('F', 'M') 
	AND HAS_PRETRIAL_PERIOD_END = 'No'
GROUP BY COUNTY, charge_level_standard, book_year
ORDER BY county, charge_level_standard, book_year;




----


SELECT county, is_released_prearraignment, count(*)
FROM DWH_PROD.REPORTING.AGG_PRETRIAL_KPI
GROUP BY county, IS_RELEASED_PREARRAIGNMENT
ORDER BY county, IS_RELEASED_PREARRAIGNMENT ; -- LA has ALL 'NO' FOR prearraignment RELEASE

SELECT RELEASE_TYPE , count(*) AS n
FROM DWH_PROD.REPORTING.AGG_PRETRIAL_KPI
--WHERE is_release_eligible = 'Yes' AND is_released_prearraignment = 'No' AND is_unmatched_booking = 'No'
GROUP BY RELEASE_TYPE
ORDER BY n desc; 


