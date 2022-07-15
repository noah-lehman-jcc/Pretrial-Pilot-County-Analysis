select year(booking_date) as book_year, month(booking_date) as book_month, 
    sum(booking_count) as book_count, 
    sum(booking_assessed_count) as assessed_bk_count
from "39_SANJOAQUIN-DWH_PROD".SECURE_SHARE."AGG_PRETRIAL_KPI"
group by book_year, book_month
order by book_year, book_month;


select cast(assessment_year as varchar) as assessment_year, assessment_month_no, assessment_month_name, 
    count(distinct pretrial_assessment_key) as unique_assessments,
    sum(CASE WHEN booking_key IS NULL THEN 0 ELSE 1 END) AS have_booking_key
FROM "39_SANJOAQUIN-DWH_PROD".SECURE_SHARE.FACT_PRETRIAL_ASSESSMENT AS fact_pretrial_assessment
join "39_SANJOAQUIN-DWH_PROD".SECURE_SHARE.DIM_ASSESSMENT_DATE AS dim_assessment_date
 on fact_pretrial_assessment.dim_assessment_date_key = dim_assessment_date.dim_assessment_date_key
group by assessment_year, assessment_month_no, assessment_month_name
order by assessment_year, assessment_month_no;


select cast(filed_year as varchar) as filed_year, filed_month_no, filed_month_name, 
    count(distinct case_key) as unique_case
from "39_SANJOAQUIN-DWH_PROD".SECURE_SHARE.fact_court_case AS fact_court_case
join "39_SANJOAQUIN-DWH_PROD".SECURE_SHARE.dim_filed_date AS dim_filed_date on fact_court_case.dim_filed_date_key = dim_filed_date.dim_filed_date_key
group by filed_year, filed_month_no, filed_month_name
order by filed_year, filed_month_no;

