create or replace view DWH_PROD.DATA_SCIENCE.OUTCOME_DF as

-- make sure to change the doj enhanced view location to PROD instead of UAT once it is deployed

select distinct
    agg.county,
    agg.booking_id,
    agg.booking_key,
    agg.booking_type,
    agg.booking_date,
    agg.release_date,
    agg.release_type,
    agg.charge_level, 
    agg.charge_level_sort,
    agg.public_case_number,
    agg.case_filed_date,
    agg.case_disposition_date,
    agg.pretrial_period_end_date,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name,
    agg.release_eligible_count,
    agg.released_pretrial_count, 
    agg.pretrial_period_end_count, 
    agg.released_to_pretrial_services_count,
    agg.judge_followed_release_recommendation_count,
    case when arrest_date_doj is null then 0 else 1 end as doj_matched,
    doj.pretrial_period_end_date_doj, 
    doj.new_criminal_activity_count, 
    doj.recid_arrested_flag_doj, 
    case when agg.new_criminal_activity_count = 1 then 1
         when doj.recid_arrested_flag_doj = 1 then 1
         else 0 end
         as recid_county_or_doj,
    case when agg.failure_to_appear_count = 1 then 1
         when doj.recid_arrest_fta_flag_doj = 1 then 1
         else 0 end
         as fta_county_or_doj,
    doj.recid_date_doj,
    doj.recid_filed_flag_doj, 
    doj.recid_convicted_flag_doj,
    doj.recid_arrest_violent_felony_flag_doj, 
    doj.recid_arrest_serious_felony_flag_doj,
    doj.recid_arrest_violent_psa_flag_doj, 
    doj.recid_arrest_fta_flag_doj,
    doj.recid_filed_violent_felony_flag_doj, 
    doj.recid_filed_serious_felony_flag_doj,
    doj.recid_filed_violent_psa_flag_doj, 
    doj.recid_filed_fta_flag_doj,
    doj.recid_conviction_violent_felony_flag_doj, 
    doj.recid_conviction_serious_felony_flag_doj,
    doj.recid_conviction_violent_psa_flag_doj, 
    doj.recid_conviction_fta_flag_doj,
    pta.release_decision_prearraignment,
    pta.release_decision_arraignment,
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    (agg.release_date - agg.booking_date) as days_to_release,
    FIRST_VALUE(court_detail.disposition_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when disposition_type = 'Convicted' then 1
                when disposition_type = 'Acquitted' then 2
                when disposition_type = 'Dismissed' then 3
                when disposition_type = 'No Disposition Information' then 4
                else null end
        ) AS disposition_type


from "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "DWH_PROD"."REPORTING"."AGG_PRETRIAL_KPI_DOJ_ENHANCED" as doj 
        on agg.booking_key = doj.booking_key AND agg.county = doj.county
    left join "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
        on agg.pretrial_assessment_key = fpa.pretrial_assessment_key
    left join "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta 
        on agg.booking_key = pta.booking_key
    left join "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail 
        on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case
            on agg.booking_key = court_case.booking_key
    left join "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
            on court_case.dim_court_detail_key = court_detail.dim_court_detail_key

        
        
UNION ALL

select distinct
    agg.county,
    agg.booking_id,
    agg.booking_key,
    agg.booking_type,
    agg.booking_date,
    agg.release_date,
    agg.release_type,
    agg.charge_level, 
    agg.charge_level_sort,
    agg.public_case_number,
    agg.case_filed_date,
    agg.case_disposition_date,
    agg.pretrial_period_end_date,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name,
    agg.release_eligible_count,
    agg.released_pretrial_count, 
    agg.pretrial_period_end_count,  
    agg.released_to_pretrial_services_count,
    agg.judge_followed_release_recommendation_count,
    case when arrest_date_doj is null then 0 else 1 end as doj_matched,
    doj.pretrial_period_end_date_doj, 
    doj.new_criminal_activity_count, 
    doj.recid_arrested_flag_doj, 
    case when agg.new_criminal_activity_count = 1 then 1
         when doj.recid_arrested_flag_doj = 1 then 1
         else 0 end
         as recid_county_or_doj,
    case when agg.failure_to_appear_count = 1 then 1
         when doj.recid_arrest_fta_flag_doj = 1 then 1
         else 0 end
         as fta_county_or_doj,
    doj.recid_date_doj,
    doj.recid_filed_flag_doj, 
    doj.recid_convicted_flag_doj,
    doj.recid_arrest_violent_felony_flag_doj, 
    doj.recid_arrest_serious_felony_flag_doj,
    doj.recid_arrest_violent_psa_flag_doj, 
    doj.recid_arrest_fta_flag_doj,
    doj.recid_filed_violent_felony_flag_doj, 
    doj.recid_filed_serious_felony_flag_doj,
    doj.recid_filed_violent_psa_flag_doj, 
    doj.recid_filed_fta_flag_doj,
    doj.recid_conviction_violent_felony_flag_doj, 
    doj.recid_conviction_serious_felony_flag_doj,
    doj.recid_conviction_violent_psa_flag_doj, 
    doj.recid_conviction_fta_flag_doj,
    pta.release_decision_prearraignment,
    pta.release_decision_arraignment,
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    (agg.release_date - agg.booking_date) as days_to_release,
    FIRST_VALUE(court_detail.disposition_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when disposition_type = 'Convicted' then 1
                when disposition_type = 'Acquitted' then 2
                when disposition_type = 'Dismissed' then 3
                when disposition_type = 'No Disposition Information' then 4
                else null end
        ) AS disposition_type


from "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "DWH_PROD"."REPORTING"."AGG_PRETRIAL_KPI_DOJ_ENHANCED" as doj 
        on agg.booking_key = doj.booking_key AND agg.county = doj.county
    left join "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
        on agg.pretrial_assessment_key = fpa.pretrial_assessment_key
    left join "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta 
        on agg.booking_key = pta.booking_key
    left join "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail 
        on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case
            on agg.booking_key = court_case.booking_key
    left join "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
            on court_case.dim_court_detail_key = court_detail.dim_court_detail_key

        
        
UNION ALL

select distinct
    agg.county,
    agg.booking_id,
    agg.booking_key,
    agg.booking_type,
    agg.booking_date,
    agg.release_date,
    agg.release_type,
    agg.charge_level, 
    agg.charge_level_sort,
    agg.public_case_number,
    agg.case_filed_date,
    agg.case_disposition_date,
    agg.pretrial_period_end_date,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name,
    agg.release_eligible_count,
    agg.released_pretrial_count, 
    agg.pretrial_period_end_count,  
    agg.released_to_pretrial_services_count,
    agg.judge_followed_release_recommendation_count,
    case when arrest_date_doj is null then 0 else 1 end as doj_matched,
    doj.pretrial_period_end_date_doj, 
    doj.new_criminal_activity_count, 
    doj.recid_arrested_flag_doj, 
    case when agg.new_criminal_activity_count = 1 then 1
         when doj.recid_arrested_flag_doj = 1 then 1
         else 0 end
         as recid_county_or_doj,
    case when agg.failure_to_appear_count = 1 then 1
         when doj.recid_arrest_fta_flag_doj = 1 then 1
         else 0 end
         as fta_county_or_doj,
    doj.recid_date_doj,
    doj.recid_filed_flag_doj, 
    doj.recid_convicted_flag_doj,
    doj.recid_arrest_violent_felony_flag_doj, 
    doj.recid_arrest_serious_felony_flag_doj,
    doj.recid_arrest_violent_psa_flag_doj, 
    doj.recid_arrest_fta_flag_doj,
    doj.recid_filed_violent_felony_flag_doj, 
    doj.recid_filed_serious_felony_flag_doj,
    doj.recid_filed_violent_psa_flag_doj, 
    doj.recid_filed_fta_flag_doj,
    doj.recid_conviction_violent_felony_flag_doj, 
    doj.recid_conviction_serious_felony_flag_doj,
    doj.recid_conviction_violent_psa_flag_doj, 
    doj.recid_conviction_fta_flag_doj,
    pta.release_decision_prearraignment,
    pta.release_decision_arraignment,
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    (agg.release_date - agg.booking_date) as days_to_release,
    FIRST_VALUE(court_detail.disposition_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when disposition_type = 'Convicted' then 1
                when disposition_type = 'Acquitted' then 2
                when disposition_type = 'Dismissed' then 3
                when disposition_type = 'No Disposition Information' then 4
                else null end
        ) AS disposition_type


from "16_KINGS-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "DWH_PROD"."REPORTING"."AGG_PRETRIAL_KPI_DOJ_ENHANCED" as doj 
        on agg.booking_key = doj.booking_key AND agg.county = doj.county
    left join "16_KINGS-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
        on agg.pretrial_assessment_key = fpa.pretrial_assessment_key
    left join "16_KINGS-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta 
        on agg.booking_key = pta.booking_key
    left join "16_KINGS-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail 
        on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "16_KINGS-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case
            on agg.booking_key = court_case.booking_key
    left join "16_KINGS-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
            on court_case.dim_court_detail_key = court_detail.dim_court_detail_key

        
        
UNION ALL


select distinct
    agg.county,
    agg.booking_id,
    agg.booking_key,
    agg.booking_type,
    agg.booking_date,
    agg.release_date,
    agg.release_type,
    agg.charge_level, 
    agg.charge_level_sort,
    agg.public_case_number,
    agg.case_filed_date,
    agg.case_disposition_date,
    agg.pretrial_period_end_date,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name,
    agg.release_eligible_count,
    agg.released_pretrial_count, 
    agg.pretrial_period_end_count,  
    agg.released_to_pretrial_services_count,
    agg.judge_followed_release_recommendation_count,
    case when arrest_date_doj is null then 0 else 1 end as doj_matched,
    doj.pretrial_period_end_date_doj, 
    doj.new_criminal_activity_count, 
    doj.recid_arrested_flag_doj, 
    case when agg.new_criminal_activity_count = 1 then 1
         when doj.recid_arrested_flag_doj = 1 then 1
         else 0 end
         as recid_county_or_doj,
    case when agg.failure_to_appear_count = 1 then 1
         when doj.recid_arrest_fta_flag_doj = 1 then 1
         else 0 end
         as fta_county_or_doj,
    doj.recid_date_doj,
    doj.recid_filed_flag_doj, 
    doj.recid_convicted_flag_doj,
    doj.recid_arrest_violent_felony_flag_doj, 
    doj.recid_arrest_serious_felony_flag_doj,
    doj.recid_arrest_violent_psa_flag_doj, 
    doj.recid_arrest_fta_flag_doj,
    doj.recid_filed_violent_felony_flag_doj, 
    doj.recid_filed_serious_felony_flag_doj,
    doj.recid_filed_violent_psa_flag_doj, 
    doj.recid_filed_fta_flag_doj,
    doj.recid_conviction_violent_felony_flag_doj, 
    doj.recid_conviction_serious_felony_flag_doj,
    doj.recid_conviction_violent_psa_flag_doj, 
    doj.recid_conviction_fta_flag_doj,
    pta.release_decision_prearraignment,
    pta.release_decision_arraignment,
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    (agg.release_date - agg.booking_date) as days_to_release,
    FIRST_VALUE(court_detail.disposition_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when disposition_type = 'Convicted' then 1
                when disposition_type = 'Acquitted' then 2
                when disposition_type = 'Dismissed' then 3
                when disposition_type = 'No Disposition Information' then 4
                else null end
        ) AS disposition_type


from "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "DWH_PROD"."REPORTING"."AGG_PRETRIAL_KPI_DOJ_ENHANCED" as doj 
        on agg.booking_key = doj.booking_key AND agg.county = doj.county
    left join "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
        on agg.pretrial_assessment_key = fpa.pretrial_assessment_key
    left join "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta 
        on agg.booking_key = pta.booking_key
    left join "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail 
        on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case
            on agg.booking_key = court_case.booking_key
    left join "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
            on court_case.dim_court_detail_key = court_detail.dim_court_detail_key

        
        
UNION ALL


select distinct
    agg.county,
    agg.booking_id,
    agg.booking_key,
    agg.booking_type,
    agg.booking_date,
    agg.release_date,
    agg.release_type,
    agg.charge_level, 
    agg.charge_level_sort,
    agg.public_case_number,
    agg.case_filed_date,
    agg.case_disposition_date,
    agg.pretrial_period_end_date,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name,
    agg.release_eligible_count,
    agg.released_pretrial_count, 
    agg.pretrial_period_end_count,  
    agg.released_to_pretrial_services_count,
    agg.judge_followed_release_recommendation_count,
    case when arrest_date_doj is null then 0 else 1 end as doj_matched,
    doj.pretrial_period_end_date_doj, 
    doj.new_criminal_activity_count, 
    doj.recid_arrested_flag_doj, 
    case when agg.new_criminal_activity_count = 1 then 1
         when doj.recid_arrested_flag_doj = 1 then 1
         else 0 end
         as recid_county_or_doj,
    case when agg.failure_to_appear_count = 1 then 1
         when doj.recid_arrest_fta_flag_doj = 1 then 1
         else 0 end
         as fta_county_or_doj,
    doj.recid_date_doj,
    doj.recid_filed_flag_doj, 
    doj.recid_convicted_flag_doj,
    doj.recid_arrest_violent_felony_flag_doj, 
    doj.recid_arrest_serious_felony_flag_doj,
    doj.recid_arrest_violent_psa_flag_doj, 
    doj.recid_arrest_fta_flag_doj,
    doj.recid_filed_violent_felony_flag_doj, 
    doj.recid_filed_serious_felony_flag_doj,
    doj.recid_filed_violent_psa_flag_doj, 
    doj.recid_filed_fta_flag_doj,
    doj.recid_conviction_violent_felony_flag_doj, 
    doj.recid_conviction_serious_felony_flag_doj,
    doj.recid_conviction_violent_psa_flag_doj, 
    doj.recid_conviction_fta_flag_doj,
    pta.release_decision_prearraignment,
    pta.release_decision_arraignment,
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    (agg.release_date - agg.booking_date) as days_to_release,
    FIRST_VALUE(court_detail.disposition_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when disposition_type = 'Convicted' then 1
                when disposition_type = 'Acquitted' then 2
                when disposition_type = 'Dismissed' then 3
                when disposition_type = 'No Disposition Information' then 4
                else null end
        ) AS disposition_type


from "25_MODOC-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "DWH_PROD"."REPORTING"."AGG_PRETRIAL_KPI_DOJ_ENHANCED" as doj 
        on agg.booking_key = doj.booking_key AND agg.county = doj.county
    left join "25_MODOC-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
        on agg.pretrial_assessment_key = fpa.pretrial_assessment_key
    left join "25_MODOC-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta 
        on agg.booking_key = pta.booking_key
    left join "25_MODOC-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail 
        on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "25_MODOC-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case
            on agg.booking_key = court_case.booking_key
    left join "25_MODOC-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
            on court_case.dim_court_detail_key = court_detail.dim_court_detail_key

        
        
UNION ALL



select distinct
    agg.county,
    agg.booking_id,
    agg.booking_key,
    agg.booking_type,
    agg.booking_date,
    agg.release_date,
    agg.release_type,
    agg.charge_level, 
    agg.charge_level_sort,
    agg.public_case_number,
    agg.case_filed_date,
    agg.case_disposition_date,
    agg.pretrial_period_end_date,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name,
    agg.release_eligible_count,
    agg.released_pretrial_count, 
    agg.pretrial_period_end_count,  
    agg.released_to_pretrial_services_count,
    agg.judge_followed_release_recommendation_count,
    case when arrest_date_doj is null then 0 else 1 end as doj_matched,
    doj.pretrial_period_end_date_doj, 
    doj.new_criminal_activity_count, 
    doj.recid_arrested_flag_doj, 
    case when agg.new_criminal_activity_count = 1 then 1
         when doj.recid_arrested_flag_doj = 1 then 1
         else 0 end
         as recid_county_or_doj,
    case when agg.failure_to_appear_count = 1 then 1
         when doj.recid_arrest_fta_flag_doj = 1 then 1
         else 0 end
         as fta_county_or_doj,
    doj.recid_date_doj,
    doj.recid_filed_flag_doj, 
    doj.recid_convicted_flag_doj,
    doj.recid_arrest_violent_felony_flag_doj, 
    doj.recid_arrest_serious_felony_flag_doj,
    doj.recid_arrest_violent_psa_flag_doj, 
    doj.recid_arrest_fta_flag_doj,
    doj.recid_filed_violent_felony_flag_doj, 
    doj.recid_filed_serious_felony_flag_doj,
    doj.recid_filed_violent_psa_flag_doj, 
    doj.recid_filed_fta_flag_doj,
    doj.recid_conviction_violent_felony_flag_doj, 
    doj.recid_conviction_serious_felony_flag_doj,
    doj.recid_conviction_violent_psa_flag_doj, 
    doj.recid_conviction_fta_flag_doj,
    pta.release_decision_prearraignment,
    pta.release_decision_arraignment,
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    (agg.release_date - agg.booking_date) as days_to_release,
    FIRST_VALUE(court_detail.disposition_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when disposition_type = 'Convicted' then 1
                when disposition_type = 'Acquitted' then 2
                when disposition_type = 'Dismissed' then 3
                when disposition_type = 'No Disposition Information' then 4
                else null end
        ) AS disposition_type


from "28_NAPA-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "DWH_PROD"."REPORTING"."AGG_PRETRIAL_KPI_DOJ_ENHANCED" as doj 
        on agg.booking_key = doj.booking_key AND agg.county = doj.county
    left join "28_NAPA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
        on agg.pretrial_assessment_key = fpa.pretrial_assessment_key
    left join "28_NAPA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta 
        on agg.booking_key = pta.booking_key
    left join "28_NAPA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail 
        on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "28_NAPA-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case
            on agg.booking_key = court_case.booking_key
    left join "28_NAPA-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
            on court_case.dim_court_detail_key = court_detail.dim_court_detail_key

        
        
UNION ALL



select distinct
    agg.county,
    agg.booking_id,
    agg.booking_key,
    agg.booking_type,
    agg.booking_date,
    agg.release_date,
    agg.release_type,
    agg.charge_level, 
    agg.charge_level_sort,
    agg.public_case_number,
    agg.case_filed_date,
    agg.case_disposition_date,
    agg.pretrial_period_end_date,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name,
    agg.release_eligible_count,
    agg.released_pretrial_count, 
    agg.pretrial_period_end_count,  
    agg.released_to_pretrial_services_count,
    agg.judge_followed_release_recommendation_count,
    case when arrest_date_doj is null then 0 else 1 end as doj_matched,
    doj.pretrial_period_end_date_doj, 
    doj.new_criminal_activity_count, 
    doj.recid_arrested_flag_doj, 
    case when agg.new_criminal_activity_count = 1 then 1
         when doj.recid_arrested_flag_doj = 1 then 1
         else 0 end
         as recid_county_or_doj,
    case when agg.failure_to_appear_count = 1 then 1
         when doj.recid_arrest_fta_flag_doj = 1 then 1
         else 0 end
         as fta_county_or_doj,
    doj.recid_date_doj,
    doj.recid_filed_flag_doj, 
    doj.recid_convicted_flag_doj,
    doj.recid_arrest_violent_felony_flag_doj, 
    doj.recid_arrest_serious_felony_flag_doj,
    doj.recid_arrest_violent_psa_flag_doj, 
    doj.recid_arrest_fta_flag_doj,
    doj.recid_filed_violent_felony_flag_doj, 
    doj.recid_filed_serious_felony_flag_doj,
    doj.recid_filed_violent_psa_flag_doj, 
    doj.recid_filed_fta_flag_doj,
    doj.recid_conviction_violent_felony_flag_doj, 
    doj.recid_conviction_serious_felony_flag_doj,
    doj.recid_conviction_violent_psa_flag_doj, 
    doj.recid_conviction_fta_flag_doj,
    pta.release_decision_prearraignment,
    pta.release_decision_arraignment,
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    (agg.release_date - agg.booking_date) as days_to_release,
    FIRST_VALUE(court_detail.disposition_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when disposition_type = 'Convicted' then 1
                when disposition_type = 'Acquitted' then 2
                when disposition_type = 'Dismissed' then 3
                when disposition_type = 'No Disposition Information' then 4
                else null end
        ) AS disposition_type


from "29_NEVADA-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "DWH_PROD"."REPORTING"."AGG_PRETRIAL_KPI_DOJ_ENHANCED" as doj 
        on agg.booking_key = doj.booking_key AND agg.county = doj.county
    left join "29_NEVADA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
        on agg.pretrial_assessment_key = fpa.pretrial_assessment_key
    left join "29_NEVADA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta 
        on agg.booking_key = pta.booking_key
    left join "29_NEVADA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail 
        on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "29_NEVADA-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case
            on agg.booking_key = court_case.booking_key
    left join "29_NEVADA-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
            on court_case.dim_court_detail_key = court_detail.dim_court_detail_key

        
        
UNION ALL


select distinct
    agg.county,
    agg.booking_id,
    agg.booking_key,
    agg.booking_type,
    agg.booking_date,
    agg.release_date,
    agg.release_type,
    agg.charge_level, 
    agg.charge_level_sort,
    agg.public_case_number,
    agg.case_filed_date,
    agg.case_disposition_date,
    agg.pretrial_period_end_date,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name,
    agg.release_eligible_count,
    agg.released_pretrial_count, 
    agg.pretrial_period_end_count,  
    agg.released_to_pretrial_services_count,
    agg.judge_followed_release_recommendation_count,
    case when arrest_date_doj is null then 0 else 1 end as doj_matched,
    doj.pretrial_period_end_date_doj, 
    doj.new_criminal_activity_count, 
    doj.recid_arrested_flag_doj, 
    case when agg.new_criminal_activity_count = 1 then 1
         when doj.recid_arrested_flag_doj = 1 then 1
         else 0 end
         as recid_county_or_doj,
    case when agg.failure_to_appear_count = 1 then 1
         when doj.recid_arrest_fta_flag_doj = 1 then 1
         else 0 end
         as fta_county_or_doj,
    doj.recid_date_doj,
    doj.recid_filed_flag_doj, 
    doj.recid_convicted_flag_doj,
    doj.recid_arrest_violent_felony_flag_doj, 
    doj.recid_arrest_serious_felony_flag_doj,
    doj.recid_arrest_violent_psa_flag_doj, 
    doj.recid_arrest_fta_flag_doj,
    doj.recid_filed_violent_felony_flag_doj, 
    doj.recid_filed_serious_felony_flag_doj,
    doj.recid_filed_violent_psa_flag_doj, 
    doj.recid_filed_fta_flag_doj,
    doj.recid_conviction_violent_felony_flag_doj, 
    doj.recid_conviction_serious_felony_flag_doj,
    doj.recid_conviction_violent_psa_flag_doj, 
    doj.recid_conviction_fta_flag_doj,
    pta.release_decision_prearraignment,
    pta.release_decision_arraignment,
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    (agg.release_date - agg.booking_date) as days_to_release,
    FIRST_VALUE(court_detail.disposition_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when disposition_type = 'Convicted' then 1
                when disposition_type = 'Acquitted' then 2
                when disposition_type = 'Dismissed' then 3
                when disposition_type = 'No Disposition Information' then 4
                else null end
        ) AS disposition_type


from "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "DWH_PROD"."REPORTING"."AGG_PRETRIAL_KPI_DOJ_ENHANCED" as doj 
        on agg.booking_key = doj.booking_key AND agg.county = doj.county
    left join "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
        on agg.pretrial_assessment_key = fpa.pretrial_assessment_key
    left join "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta 
        on agg.booking_key = pta.booking_key
    left join "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail 
        on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case
            on agg.booking_key = court_case.booking_key
    left join "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
            on court_case.dim_court_detail_key = court_detail.dim_court_detail_key

        
        
UNION ALL


select distinct
    agg.county,
    agg.booking_id,
    agg.booking_key,
    agg.booking_type,
    agg.booking_date,
    agg.release_date,
    agg.release_type,
    agg.charge_level, 
    agg.charge_level_sort,
    agg.public_case_number,
    agg.case_filed_date,
    agg.case_disposition_date,
    agg.pretrial_period_end_date,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name,
    agg.release_eligible_count,
    agg.released_pretrial_count, 
    agg.pretrial_period_end_count,  
    agg.released_to_pretrial_services_count,
    agg.judge_followed_release_recommendation_count,
    case when arrest_date_doj is null then 0 else 1 end as doj_matched,
    doj.pretrial_period_end_date_doj, 
    doj.new_criminal_activity_count, 
    doj.recid_arrested_flag_doj, 
    case when agg.new_criminal_activity_count = 1 then 1
         when doj.recid_arrested_flag_doj = 1 then 1
         else 0 end
         as recid_county_or_doj,
    case when agg.failure_to_appear_count = 1 then 1
         when doj.recid_arrest_fta_flag_doj = 1 then 1
         else 0 end
         as fta_county_or_doj,
    doj.recid_date_doj,
    doj.recid_filed_flag_doj, 
    doj.recid_convicted_flag_doj,
    doj.recid_arrest_violent_felony_flag_doj, 
    doj.recid_arrest_serious_felony_flag_doj,
    doj.recid_arrest_violent_psa_flag_doj, 
    doj.recid_arrest_fta_flag_doj,
    doj.recid_filed_violent_felony_flag_doj, 
    doj.recid_filed_serious_felony_flag_doj,
    doj.recid_filed_violent_psa_flag_doj, 
    doj.recid_filed_fta_flag_doj,
    doj.recid_conviction_violent_felony_flag_doj, 
    doj.recid_conviction_serious_felony_flag_doj,
    doj.recid_conviction_violent_psa_flag_doj, 
    doj.recid_conviction_fta_flag_doj,
    pta.release_decision_prearraignment,
    pta.release_decision_arraignment,
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    (agg.release_date - agg.booking_date) as days_to_release,
    FIRST_VALUE(court_detail.disposition_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when disposition_type = 'Convicted' then 1
                when disposition_type = 'Acquitted' then 2
                when disposition_type = 'Dismissed' then 3
                when disposition_type = 'No Disposition Information' then 4
                else null end
        ) AS disposition_type


from "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "DWH_PROD"."REPORTING"."AGG_PRETRIAL_KPI_DOJ_ENHANCED" as doj 
        on agg.booking_key = doj.booking_key AND agg.county = doj.county
    left join "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
        on agg.pretrial_assessment_key = fpa.pretrial_assessment_key
    left join "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta 
        on agg.booking_key = pta.booking_key
    left join "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail 
        on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case
            on agg.booking_key = court_case.booking_key
    left join "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
            on court_case.dim_court_detail_key = court_detail.dim_court_detail_key

        
        
UNION ALL


select distinct
    agg.county,
    agg.booking_id,
    agg.booking_key,
    agg.booking_type,
    agg.booking_date,
    agg.release_date,
    agg.release_type,
    agg.charge_level, 
    agg.charge_level_sort,
    agg.public_case_number,
    agg.case_filed_date,
    agg.case_disposition_date,
    agg.pretrial_period_end_date,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name,
    agg.release_eligible_count,
    agg.released_pretrial_count, 
    agg.pretrial_period_end_count,  
    agg.released_to_pretrial_services_count,
    agg.judge_followed_release_recommendation_count,
    case when arrest_date_doj is null then 0 else 1 end as doj_matched,
    doj.pretrial_period_end_date_doj, 
    doj.new_criminal_activity_count, 
    doj.recid_arrested_flag_doj, 
    case when agg.new_criminal_activity_count = 1 then 1
         when doj.recid_arrested_flag_doj = 1 then 1
         else 0 end
         as recid_county_or_doj,
    case when agg.failure_to_appear_count = 1 then 1
         when doj.recid_arrest_fta_flag_doj = 1 then 1
         else 0 end
         as fta_county_or_doj,
    doj.recid_date_doj,
    doj.recid_filed_flag_doj, 
    doj.recid_convicted_flag_doj,
    doj.recid_arrest_violent_felony_flag_doj, 
    doj.recid_arrest_serious_felony_flag_doj,
    doj.recid_arrest_violent_psa_flag_doj, 
    doj.recid_arrest_fta_flag_doj,
    doj.recid_filed_violent_felony_flag_doj, 
    doj.recid_filed_serious_felony_flag_doj,
    doj.recid_filed_violent_psa_flag_doj, 
    doj.recid_filed_fta_flag_doj,
    doj.recid_conviction_violent_felony_flag_doj, 
    doj.recid_conviction_serious_felony_flag_doj,
    doj.recid_conviction_violent_psa_flag_doj, 
    doj.recid_conviction_fta_flag_doj,
    pta.release_decision_prearraignment,
    pta.release_decision_arraignment,
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    (agg.release_date - agg.booking_date) as days_to_release,
    FIRST_VALUE(court_detail.disposition_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when disposition_type = 'Convicted' then 1
                when disposition_type = 'Acquitted' then 2
                when disposition_type = 'Dismissed' then 3
                when disposition_type = 'No Disposition Information' then 4
                else null end
        ) AS disposition_type


from "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "DWH_PROD"."REPORTING"."AGG_PRETRIAL_KPI_DOJ_ENHANCED" as doj 
        on agg.booking_key = doj.booking_key AND agg.county = doj.county
    left join "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
        on agg.pretrial_assessment_key = fpa.pretrial_assessment_key
    left join "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta 
        on agg.booking_key = pta.booking_key
    left join "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail 
        on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case
            on agg.booking_key = court_case.booking_key
    left join "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
            on court_case.dim_court_detail_key = court_detail.dim_court_detail_key

        
        
UNION ALL


select distinct
    agg.county,
    agg.booking_id,
    agg.booking_key,
    agg.booking_type,
    agg.booking_date,
    agg.release_date,
    agg.release_type,
    agg.charge_level, 
    agg.charge_level_sort,
    agg.public_case_number,
    agg.case_filed_date,
    agg.case_disposition_date,
    agg.pretrial_period_end_date,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name,
    agg.release_eligible_count,
    agg.released_pretrial_count, 
    agg.pretrial_period_end_count,  
    agg.released_to_pretrial_services_count,
    agg.judge_followed_release_recommendation_count,
    case when arrest_date_doj is null then 0 else 1 end as doj_matched,
    doj.pretrial_period_end_date_doj, 
    doj.new_criminal_activity_count, 
    doj.recid_arrested_flag_doj, 
    case when agg.new_criminal_activity_count = 1 then 1
         when doj.recid_arrested_flag_doj = 1 then 1
         else 0 end
         as recid_county_or_doj,
    case when agg.failure_to_appear_count = 1 then 1
         when doj.recid_arrest_fta_flag_doj = 1 then 1
         else 0 end
         as fta_county_or_doj,
    doj.recid_date_doj,
    doj.recid_filed_flag_doj, 
    doj.recid_convicted_flag_doj,
    doj.recid_arrest_violent_felony_flag_doj, 
    doj.recid_arrest_serious_felony_flag_doj,
    doj.recid_arrest_violent_psa_flag_doj, 
    doj.recid_arrest_fta_flag_doj,
    doj.recid_filed_violent_felony_flag_doj, 
    doj.recid_filed_serious_felony_flag_doj,
    doj.recid_filed_violent_psa_flag_doj, 
    doj.recid_filed_fta_flag_doj,
    doj.recid_conviction_violent_felony_flag_doj, 
    doj.recid_conviction_serious_felony_flag_doj,
    doj.recid_conviction_violent_psa_flag_doj, 
    doj.recid_conviction_fta_flag_doj,
    pta.release_decision_prearraignment,
    pta.release_decision_arraignment,
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    (agg.release_date - agg.booking_date) as days_to_release,
    FIRST_VALUE(court_detail.disposition_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when disposition_type = 'Convicted' then 1
                when disposition_type = 'Acquitted' then 2
                when disposition_type = 'Dismissed' then 3
                when disposition_type = 'No Disposition Information' then 4
                else null end
        ) AS disposition_type


from "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "DWH_PROD"."REPORTING"."AGG_PRETRIAL_KPI_DOJ_ENHANCED" as doj 
        on agg.booking_key = doj.booking_key AND agg.county = doj.county
    left join "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
        on agg.pretrial_assessment_key = fpa.pretrial_assessment_key
    left join "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta 
        on agg.booking_key = pta.booking_key
    left join "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail 
        on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case
            on agg.booking_key = court_case.booking_key
    left join "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
            on court_case.dim_court_detail_key = court_detail.dim_court_detail_key

        
        
UNION ALL


select distinct
    agg.county,
    agg.booking_id,
    agg.booking_key,
    agg.booking_type,
    agg.booking_date,
    agg.release_date,
    agg.release_type,
    agg.charge_level, 
    agg.charge_level_sort,
    agg.public_case_number,
    agg.case_filed_date,
    agg.case_disposition_date,
    agg.pretrial_period_end_date,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name,
    agg.release_eligible_count,
    agg.released_pretrial_count, 
    agg.pretrial_period_end_count,  
    agg.released_to_pretrial_services_count,
    agg.judge_followed_release_recommendation_count,
    case when arrest_date_doj is null then 0 else 1 end as doj_matched,
    doj.pretrial_period_end_date_doj, 
    doj.new_criminal_activity_count, 
    doj.recid_arrested_flag_doj, 
    case when agg.new_criminal_activity_count = 1 then 1
         when doj.recid_arrested_flag_doj = 1 then 1
         else 0 end
         as recid_county_or_doj,
    case when agg.failure_to_appear_count = 1 then 1
         when doj.recid_arrest_fta_flag_doj = 1 then 1
         else 0 end
         as fta_county_or_doj,
    doj.recid_date_doj,
    doj.recid_filed_flag_doj, 
    doj.recid_convicted_flag_doj,
    doj.recid_arrest_violent_felony_flag_doj, 
    doj.recid_arrest_serious_felony_flag_doj,
    doj.recid_arrest_violent_psa_flag_doj, 
    doj.recid_arrest_fta_flag_doj,
    doj.recid_filed_violent_felony_flag_doj, 
    doj.recid_filed_serious_felony_flag_doj,
    doj.recid_filed_violent_psa_flag_doj, 
    doj.recid_filed_fta_flag_doj,
    doj.recid_conviction_violent_felony_flag_doj, 
    doj.recid_conviction_serious_felony_flag_doj,
    doj.recid_conviction_violent_psa_flag_doj, 
    doj.recid_conviction_fta_flag_doj,
    pta.release_decision_prearraignment,
    pta.release_decision_arraignment,
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    (agg.release_date - agg.booking_date) as days_to_release,
    FIRST_VALUE(court_detail.disposition_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when disposition_type = 'Convicted' then 1
                when disposition_type = 'Acquitted' then 2
                when disposition_type = 'Dismissed' then 3
                when disposition_type = 'No Disposition Information' then 4
                else null end
        ) AS disposition_type


from "46_SIERRA-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "DWH_PROD"."REPORTING"."AGG_PRETRIAL_KPI_DOJ_ENHANCED" as doj 
        on agg.booking_key = doj.booking_key AND agg.county = doj.county
    left join "46_SIERRA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
        on agg.pretrial_assessment_key = fpa.pretrial_assessment_key
    left join "46_SIERRA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta 
        on agg.booking_key = pta.booking_key
    left join "46_SIERRA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail 
        on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "46_SIERRA-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case
            on agg.booking_key = court_case.booking_key
    left join "46_SIERRA-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
            on court_case.dim_court_detail_key = court_detail.dim_court_detail_key

        
        
UNION ALL


select distinct
    agg.county,
    agg.booking_id,
    agg.booking_key,
    agg.booking_type,
    agg.booking_date,
    agg.release_date,
    agg.release_type,
    agg.charge_level, 
    agg.charge_level_sort,
    agg.public_case_number,
    agg.case_filed_date,
    agg.case_disposition_date,
    agg.pretrial_period_end_date,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name,
    agg.release_eligible_count,
    agg.released_pretrial_count, 
    agg.pretrial_period_end_count,  
    agg.released_to_pretrial_services_count,
    agg.judge_followed_release_recommendation_count,
    case when arrest_date_doj is null then 0 else 1 end as doj_matched,
    doj.pretrial_period_end_date_doj, 
    doj.new_criminal_activity_count, 
    doj.recid_arrested_flag_doj, 
    case when agg.new_criminal_activity_count = 1 then 1
         when doj.recid_arrested_flag_doj = 1 then 1
         else 0 end
         as recid_county_or_doj,
    case when agg.failure_to_appear_count = 1 then 1
         when doj.recid_arrest_fta_flag_doj = 1 then 1
         else 0 end
         as fta_county_or_doj,
    doj.recid_date_doj,
    doj.recid_filed_flag_doj, 
    doj.recid_convicted_flag_doj,
    doj.recid_arrest_violent_felony_flag_doj, 
    doj.recid_arrest_serious_felony_flag_doj,
    doj.recid_arrest_violent_psa_flag_doj, 
    doj.recid_arrest_fta_flag_doj,
    doj.recid_filed_violent_felony_flag_doj, 
    doj.recid_filed_serious_felony_flag_doj,
    doj.recid_filed_violent_psa_flag_doj, 
    doj.recid_filed_fta_flag_doj,
    doj.recid_conviction_violent_felony_flag_doj, 
    doj.recid_conviction_serious_felony_flag_doj,
    doj.recid_conviction_violent_psa_flag_doj, 
    doj.recid_conviction_fta_flag_doj,
    pta.release_decision_prearraignment,
    pta.release_decision_arraignment,
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    (agg.release_date - agg.booking_date) as days_to_release,
    FIRST_VALUE(court_detail.disposition_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when disposition_type = 'Convicted' then 1
                when disposition_type = 'Acquitted' then 2
                when disposition_type = 'Dismissed' then 3
                when disposition_type = 'No Disposition Information' then 4
                else null end
        ) AS disposition_type


from "49_SONOMA-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "DWH_PROD"."REPORTING"."AGG_PRETRIAL_KPI_DOJ_ENHANCED" as doj 
        on agg.booking_key = doj.booking_key AND agg.county = doj.county
    left join "49_SONOMA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
        on agg.pretrial_assessment_key = fpa.pretrial_assessment_key
    left join "49_SONOMA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta 
        on agg.booking_key = pta.booking_key
    left join "49_SONOMA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail 
        on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "49_SONOMA-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case
            on agg.booking_key = court_case.booking_key
    left join "49_SONOMA-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
            on court_case.dim_court_detail_key = court_detail.dim_court_detail_key

        
        
UNION ALL


select distinct
    agg.county,
    agg.booking_id,
    agg.booking_key,
    agg.booking_type,
    agg.booking_date,
    agg.release_date,
    agg.release_type,
    agg.charge_level, 
    agg.charge_level_sort,
    agg.public_case_number,
    agg.case_filed_date,
    agg.case_disposition_date,
    agg.pretrial_period_end_date,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name,
    agg.release_eligible_count,
    agg.released_pretrial_count, 
    agg.pretrial_period_end_count,  
    agg.released_to_pretrial_services_count,
    agg.judge_followed_release_recommendation_count,
    case when arrest_date_doj is null then 0 else 1 end as doj_matched,
    doj.pretrial_period_end_date_doj, 
    doj.new_criminal_activity_count, 
    doj.recid_arrested_flag_doj, 
    case when agg.new_criminal_activity_count = 1 then 1
         when doj.recid_arrested_flag_doj = 1 then 1
         else 0 end
         as recid_county_or_doj,
    case when agg.failure_to_appear_count = 1 then 1
         when doj.recid_arrest_fta_flag_doj = 1 then 1
         else 0 end
         as fta_county_or_doj,
    doj.recid_date_doj,
    doj.recid_filed_flag_doj, 
    doj.recid_convicted_flag_doj,
    doj.recid_arrest_violent_felony_flag_doj, 
    doj.recid_arrest_serious_felony_flag_doj,
    doj.recid_arrest_violent_psa_flag_doj, 
    doj.recid_arrest_fta_flag_doj,
    doj.recid_filed_violent_felony_flag_doj, 
    doj.recid_filed_serious_felony_flag_doj,
    doj.recid_filed_violent_psa_flag_doj, 
    doj.recid_filed_fta_flag_doj,
    doj.recid_conviction_violent_felony_flag_doj, 
    doj.recid_conviction_serious_felony_flag_doj,
    doj.recid_conviction_violent_psa_flag_doj, 
    doj.recid_conviction_fta_flag_doj,
    pta.release_decision_prearraignment,
    pta.release_decision_arraignment,
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    (agg.release_date - agg.booking_date) as days_to_release,
    FIRST_VALUE(court_detail.disposition_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when disposition_type = 'Convicted' then 1
                when disposition_type = 'Acquitted' then 2
                when disposition_type = 'Dismissed' then 3
                when disposition_type = 'No Disposition Information' then 4
                else null end
        ) AS disposition_type


from "54_TULARE-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "DWH_PROD"."REPORTING"."AGG_PRETRIAL_KPI_DOJ_ENHANCED" as doj 
        on agg.booking_key = doj.booking_key AND agg.county = doj.county
    left join "54_TULARE-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
        on agg.pretrial_assessment_key = fpa.pretrial_assessment_key
    left join "54_TULARE-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta 
        on agg.booking_key = pta.booking_key
    left join "54_TULARE-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail 
        on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "54_TULARE-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case
            on agg.booking_key = court_case.booking_key
    left join "54_TULARE-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
            on court_case.dim_court_detail_key = court_detail.dim_court_detail_key

        
        
UNION ALL


select distinct
    agg.county,
    agg.booking_id,
    agg.booking_key,
    agg.booking_type,
    agg.booking_date,
    agg.release_date,
    agg.release_type,
    agg.charge_level, 
    agg.charge_level_sort,
    agg.public_case_number,
    agg.case_filed_date,
    agg.case_disposition_date,
    agg.pretrial_period_end_date,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name,
    agg.release_eligible_count,
    agg.released_pretrial_count, 
    agg.pretrial_period_end_count,  
    agg.released_to_pretrial_services_count,
    agg.judge_followed_release_recommendation_count,
    case when arrest_date_doj is null then 0 else 1 end as doj_matched,
    doj.pretrial_period_end_date_doj, 
    doj.new_criminal_activity_count, 
    doj.recid_arrested_flag_doj, 
    case when agg.new_criminal_activity_count = 1 then 1
         when doj.recid_arrested_flag_doj = 1 then 1
         else 0 end
         as recid_county_or_doj,
    case when agg.failure_to_appear_count = 1 then 1
         when doj.recid_arrest_fta_flag_doj = 1 then 1
         else 0 end
         as fta_county_or_doj,
    doj.recid_date_doj,
    doj.recid_filed_flag_doj, 
    doj.recid_convicted_flag_doj,
    doj.recid_arrest_violent_felony_flag_doj, 
    doj.recid_arrest_serious_felony_flag_doj,
    doj.recid_arrest_violent_psa_flag_doj, 
    doj.recid_arrest_fta_flag_doj,
    doj.recid_filed_violent_felony_flag_doj, 
    doj.recid_filed_serious_felony_flag_doj,
    doj.recid_filed_violent_psa_flag_doj, 
    doj.recid_filed_fta_flag_doj,
    doj.recid_conviction_violent_felony_flag_doj, 
    doj.recid_conviction_serious_felony_flag_doj,
    doj.recid_conviction_violent_psa_flag_doj, 
    doj.recid_conviction_fta_flag_doj,
    pta.release_decision_prearraignment,
    pta.release_decision_arraignment,
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    (agg.release_date - agg.booking_date) as days_to_release,
    FIRST_VALUE(court_detail.disposition_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when disposition_type = 'Convicted' then 1
                when disposition_type = 'Acquitted' then 2
                when disposition_type = 'Dismissed' then 3
                when disposition_type = 'No Disposition Information' then 4
                else null end
        ) AS disposition_type


from "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "DWH_PROD"."REPORTING"."AGG_PRETRIAL_KPI_DOJ_ENHANCED" as doj 
        on agg.booking_key = doj.booking_key AND agg.county = doj.county
    left join "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
        on agg.pretrial_assessment_key = fpa.pretrial_assessment_key
    left join "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta 
        on agg.booking_key = pta.booking_key
    left join "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail 
        on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case
            on agg.booking_key = court_case.booking_key
    left join "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
            on court_case.dim_court_detail_key = court_detail.dim_court_detail_key

        
        
UNION ALL


select distinct
    agg.county,
    agg.booking_id,
    agg.booking_key,
    agg.booking_type,
    agg.booking_date,
    agg.release_date,
    agg.release_type,
    agg.charge_level, 
    agg.charge_level_sort,
    agg.public_case_number,
    agg.case_filed_date,
    agg.case_disposition_date,
    agg.pretrial_period_end_date,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name,
    agg.release_eligible_count,
    agg.released_pretrial_count, 
    agg.pretrial_period_end_count,  
    agg.released_to_pretrial_services_count,
    agg.judge_followed_release_recommendation_count,
    case when arrest_date_doj is null then 0 else 1 end as doj_matched,
    doj.pretrial_period_end_date_doj, 
    doj.new_criminal_activity_count, 
    doj.recid_arrested_flag_doj, 
    case when agg.new_criminal_activity_count = 1 then 1
         when doj.recid_arrested_flag_doj = 1 then 1
         else 0 end
         as recid_county_or_doj,
    case when agg.failure_to_appear_count = 1 then 1
         when doj.recid_arrest_fta_flag_doj = 1 then 1
         else 0 end
         as fta_county_or_doj,
    doj.recid_date_doj,
    doj.recid_filed_flag_doj, 
    doj.recid_convicted_flag_doj,
    doj.recid_arrest_violent_felony_flag_doj, 
    doj.recid_arrest_serious_felony_flag_doj,
    doj.recid_arrest_violent_psa_flag_doj, 
    doj.recid_arrest_fta_flag_doj,
    doj.recid_filed_violent_felony_flag_doj, 
    doj.recid_filed_serious_felony_flag_doj,
    doj.recid_filed_violent_psa_flag_doj, 
    doj.recid_filed_fta_flag_doj,
    doj.recid_conviction_violent_felony_flag_doj, 
    doj.recid_conviction_serious_felony_flag_doj,
    doj.recid_conviction_violent_psa_flag_doj, 
    doj.recid_conviction_fta_flag_doj,
    pta.release_decision_prearraignment,
    pta.release_decision_arraignment,
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    (agg.release_date - agg.booking_date) as days_to_release,
    FIRST_VALUE(court_detail.disposition_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when disposition_type = 'Convicted' then 1
                when disposition_type = 'Acquitted' then 2
                when disposition_type = 'Dismissed' then 3
                when disposition_type = 'No Disposition Information' then 4
                else null end
        ) AS disposition_type


from "56_VENTURA-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "DWH_PROD"."REPORTING"."AGG_PRETRIAL_KPI_DOJ_ENHANCED" as doj 
        on agg.booking_key = doj.booking_key AND agg.county = doj.county
    left join "56_VENTURA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
        on agg.pretrial_assessment_key = fpa.pretrial_assessment_key
    left join "56_VENTURA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta 
        on agg.booking_key = pta.booking_key
    left join "56_VENTURA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail 
        on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "56_VENTURA-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case
            on agg.booking_key = court_case.booking_key
    left join "56_VENTURA-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
            on court_case.dim_court_detail_key = court_detail.dim_court_detail_key

        
        
UNION ALL


select distinct
    agg.county,
    agg.booking_id,
    agg.booking_key,
    agg.booking_type,
    agg.booking_date,
    agg.release_date,
    agg.release_type,
    agg.charge_level, 
    agg.charge_level_sort,
    agg.public_case_number,
    agg.case_filed_date,
    agg.case_disposition_date,
    agg.pretrial_period_end_date,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name,
    agg.release_eligible_count,
    agg.released_pretrial_count, 
    agg.pretrial_period_end_count,  
    agg.released_to_pretrial_services_count,
    agg.judge_followed_release_recommendation_count,
    case when arrest_date_doj is null then 0 else 1 end as doj_matched,
    doj.pretrial_period_end_date_doj, 
    doj.new_criminal_activity_count, 
    doj.recid_arrested_flag_doj, 
    case when agg.new_criminal_activity_count = 1 then 1
         when doj.recid_arrested_flag_doj = 1 then 1
         else 0 end
         as recid_county_or_doj,
    case when agg.failure_to_appear_count = 1 then 1
         when doj.recid_arrest_fta_flag_doj = 1 then 1
         else 0 end
         as fta_county_or_doj,
    doj.recid_date_doj,
    doj.recid_filed_flag_doj, 
    doj.recid_convicted_flag_doj,
    doj.recid_arrest_violent_felony_flag_doj, 
    doj.recid_arrest_serious_felony_flag_doj,
    doj.recid_arrest_violent_psa_flag_doj, 
    doj.recid_arrest_fta_flag_doj,
    doj.recid_filed_violent_felony_flag_doj, 
    doj.recid_filed_serious_felony_flag_doj,
    doj.recid_filed_violent_psa_flag_doj, 
    doj.recid_filed_fta_flag_doj,
    doj.recid_conviction_violent_felony_flag_doj, 
    doj.recid_conviction_serious_felony_flag_doj,
    doj.recid_conviction_violent_psa_flag_doj, 
    doj.recid_conviction_fta_flag_doj,
    pta.release_decision_prearraignment,
    pta.release_decision_arraignment,
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    (agg.release_date - agg.booking_date) as days_to_release,
    FIRST_VALUE(court_detail.disposition_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when disposition_type = 'Convicted' then 1
                when disposition_type = 'Acquitted' then 2
                when disposition_type = 'Dismissed' then 3
                when disposition_type = 'No Disposition Information' then 4
                else null end
        ) AS disposition_type


from "58_YUBA-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "DWH_PROD"."REPORTING"."AGG_PRETRIAL_KPI_DOJ_ENHANCED" as doj 
        on agg.booking_key = doj.booking_key AND agg.county = doj.county
    left join "58_YUBA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
        on agg.pretrial_assessment_key = fpa.pretrial_assessment_key
    left join "58_YUBA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta 
        on agg.booking_key = pta.booking_key
    left join "58_YUBA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail 
        on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "58_YUBA-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case
            on agg.booking_key = court_case.booking_key
    left join "58_YUBA-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
            on court_case.dim_court_detail_key = court_detail.dim_court_detail_key

;


