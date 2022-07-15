create or replace view DWH_PROD.DATA_SCIENCE.join_df as

select
    distinct
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
    agg.is_release_eligible,
    agg.release_eligible_count,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.is_released_prearraignment,
    agg.released_prearraignment_count,
    agg.is_released_pretrial,
    agg.released_pretrial_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.has_new_criminal_activity,
    agg.new_criminal_activity_count,
    agg.has_pretrial_period_end,
    agg.pretrial_period_end_count,
    agg.has_pretrial_violation,
    agg.pretrial_violation_count,
    agg.is_unmatched_booking,
    agg.unmatched_booking_count,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name, pre_assessment.risk_score, pre_assessment.risk_score_raw, pre_assessment.score_failure_to_appear, pre_assessment.score_new_criminal_activity, pre_assessment.score_new_criminal_violent_activity, pre_assessment.generic_tool_total_score, pre_assessment.generic_tool_total_score_raw,
    book_charge.principal_booking_offense_level as booking_charge_level,
    book_charge.principal_booking_offense_type as booking_charge_code,
    book_charge.principal_booking_charge as booking_charge,
    book_charge.principal_booking_charge_hierarchy_code as booking_charge_hierarchy,
    FIRST_VALUE(court_charge.principal_court_charge) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge,
    FIRST_VALUE(court_charge.principal_court_offense_level) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_level,
    FIRST_VALUE(court_charge.principal_court_offense_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_code,
    FIRST_VALUE(court_charge.principal_court_charge_hierarchy_code) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_hierarchy,
    MIN(court_case.dim_plea_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS plea_date_key, -- need plea date dimension to get actual date out
    MIN(court_case.dim_sentence_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS sentence_date_key, -- need sentence date dimension to get actual date out
    FIRST_VALUE(court_detail.plea_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
                case when plea_type = 'Guilty' then 1
                     when plea_type = 'Nolo Contendere' then 2
                     when plea_type = 'Not Guilty' then 3
                     else null end          
          ) AS plea_type,
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
        ) AS disposition_type,
    FIRST_VALUE(court_detail.sentence_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when sentence_type = 'Prison' then 1
                when sentence_type = 'Jail' then 2
                when sentence_type = 'Probation' then 3
                when sentence_type = 'Fine' then 4
                when sentence_type = 'No Sentence Information' then 5
         else null end
        ) AS sentence_type,

    -- pretrial monitoring flags
    pre_detail.monitoring_level,
    pre_detail.court_date_reminder_flag,
    pre_detail.transit_service_flag,
    pre_detail.other_pretrial_service,
    pre_detail.condition_phone_flag,
    pre_detail.condition_inperson_flag,
    pre_detail.condition_drug_test_flag,
    pre_detail.condition_alcohol_flag,
    pre_detail.condition_em_flag,
    pre_detail.condition_no_contact_flag,
    
    pre_detail.release_decision_prearraignment,
    pre_detail.release_decision_arraignment,
    pre_detail.pretrial_release_type, -- what is this compared to normal release type??
    pre_detail.pretrial_termination_outcome,
    pre_detail.pretrial_termination_reason,
    pre_termination_date.pretrial_termination_date,
    
    -- arrest charge flags
    book_flags.has_special_charge as arrest_special_flag,
    book_flags.has_serious_felony_charge as arrest_serious_flag,
    book_flags.has_violent_felony_charge as arrest_violent_flag,
    book_flags.has_violent_psa_charge as arrest_violent_psa_flag,
    book_flags.has_capital_charge as arrest_capital_flag,
    book_flags.has_dv_charge as arrest_dv_flag,
    book_flags.has_marijuana_charge as arrest_marijuana_flag,
    book_flags.has_sup_vio_charge as arrest_sup_vio_flag,
    book_flags.has_fta_charge as arrest_fta_flag,
    book_flags.has_sex_charge as arrest_sex_flag,
    book_flags.has_dui_charge as arrest_dui_flag,
    book_flags.has_restrain_charge as arrest_restrain_flag,
    book_flags.has_property_charge as arrest_property_flag,
    book_flags.has_drug_charge as arrest_drug_flag,
    book_flags.has_dv_possible_charge as arrest_dv_possible_flag,
    book_flags.booking_charge_group_level as arrest_charge_level, -- will need to convert into misdo and felony flags in R
    
    -- filing charge flags
    MAX(case when court_flags.has_special_charge = 'Yes' THEN 1 else 0 end) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_special_flag,
    MAX(case when court_flags.has_serious_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_serious_flag,
    MAX(case when court_flags.has_violent_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_flag,
    MAX(case when court_flags.has_violent_psa_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_psa_flag,
    MAX(case when court_flags.has_capital_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_capital_flag,
    MAX(case when court_flags.has_dv_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_flag,
    MAX(case when court_flags.has_marijuana_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_marijuana_flag,
    MAX(case when court_flags.has_sup_vio_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sup_vio_flag,
    MAX(case when court_flags.has_fta_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_fta_flag,
    MAX(case when court_flags.has_sex_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sex_flag,
    MAX(case when court_flags.has_dui_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dui_flag,
    MAX(case when court_flags.has_restrain_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_restrain_flag,
    MAX(case when court_flags.has_property_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_property_flag,
    MAX(case when court_flags.has_drug_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_drug_flag,
    MAX(case when court_flags.has_dv_possible_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_possible_flag,  
    -- conviction charge flags not currently possible
    arrest_date.arrest_date
    

from "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."FACT_BOOKING" as booking 
        on agg.booking_key = booking.booking_key
    left join "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_BOOKING_CHARGE" as book_charge 
        on booking.dim_principal_booking_charge_key = book_charge.dim_principal_booking_charge_key
    left join "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case 
        on agg.booking_key = court_case.booking_key
    left join "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_COURT_CHARGE" as court_charge 
        on court_case.dim_principal_court_charge_key = court_charge.dim_principal_court_charge_key
    left join "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
        on court_case.dim_court_detail_key = court_detail.dim_court_detail_key
    left join "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as pre_assessment
        on agg.pretrial_assessment_key = pre_assessment.pretrial_assessment_key
    left join "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."DIM_RELEASE_CONDITION" as release_cond
        on pre_assessment.dim_release_condition_key = release_cond.dim_release_condition_key
    left join "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as pre_detail
        on pre_assessment.dim_pretrial_detail_key = pre_detail.dim_pretrial_detail_key
    left join "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_TERMINATION_DATE" as pre_termination_date
        on pre_assessment.dim_pretrial_termination_date_key = pre_termination_date.dim_pretrial_termination_date_key
    left join "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."DIM_BOOKING_CHARGE_GROUP" as book_flags
        on booking.dim_booking_charge_group_key = book_flags.dim_booking_charge_group_key
    left join "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."DIM_COURT_CHARGE_GROUP" as court_flags
        on court_case.dim_court_charge_group_key = court_flags.dim_court_charge_group_key
    left join "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."DIM_ARREST_DATE" as arrest_date
        on booking.dim_arrest_date_key = arrest_date.dim_arrest_date_key
    
 

---------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------
UNION ALL


select
    distinct
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
    agg.is_release_eligible,
    agg.release_eligible_count,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.is_released_prearraignment,
    agg.released_prearraignment_count,
    agg.is_released_pretrial,
    agg.released_pretrial_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.has_new_criminal_activity,
    agg.new_criminal_activity_count,
    agg.has_pretrial_period_end,
    agg.pretrial_period_end_count,
    agg.has_pretrial_violation,
    agg.pretrial_violation_count,
    agg.is_unmatched_booking,
    agg.unmatched_booking_count,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name, pre_assessment.risk_score, pre_assessment.risk_score_raw, pre_assessment.score_failure_to_appear, pre_assessment.score_new_criminal_activity, pre_assessment.score_new_criminal_violent_activity, pre_assessment.generic_tool_total_score, pre_assessment.generic_tool_total_score_raw,
    book_charge.principal_booking_offense_level as booking_charge_level,
    book_charge.principal_booking_offense_type as booking_charge_code,
    book_charge.principal_booking_charge as booking_charge,
    book_charge.principal_booking_charge_hierarchy_code as booking_charge_hierarchy,
    FIRST_VALUE(court_charge.principal_court_charge) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge,
    FIRST_VALUE(court_charge.principal_court_offense_level) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_level,
    FIRST_VALUE(court_charge.principal_court_offense_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_code,
    FIRST_VALUE(court_charge.principal_court_charge_hierarchy_code) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_hierarchy,
    MIN(court_case.dim_plea_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS plea_date_key, -- need plea date dimension to get actual date out
    MIN(court_case.dim_sentence_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS sentence_date_key, -- need sentence date dimension to get actual date out
    FIRST_VALUE(court_detail.plea_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
                case when plea_type = 'Guilty' then 1
                     when plea_type = 'Nolo Contendere' then 2
                     when plea_type = 'Not Guilty' then 3
                     else null end          
          ) AS plea_type,
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
        ) AS disposition_type,
    FIRST_VALUE(court_detail.sentence_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when sentence_type = 'Prison' then 1
                when sentence_type = 'Jail' then 2
                when sentence_type = 'Probation' then 3
                when sentence_type = 'Fine' then 4
                when sentence_type = 'No Sentence Information' then 5
         else null end
        ) AS sentence_type,

    -- pretrial monitoring flags
    pre_detail.monitoring_level,
    pre_detail.court_date_reminder_flag,
    pre_detail.transit_service_flag,
    pre_detail.other_pretrial_service,
    pre_detail.condition_phone_flag,
    pre_detail.condition_inperson_flag,
    pre_detail.condition_drug_test_flag,
    pre_detail.condition_alcohol_flag,
    pre_detail.condition_em_flag,
    pre_detail.condition_no_contact_flag,
    
    pre_detail.release_decision_prearraignment,
    pre_detail.release_decision_arraignment,
    pre_detail.pretrial_release_type, -- what is this compared to normal release type??
    pre_detail.pretrial_termination_outcome,
    pre_detail.pretrial_termination_reason,
    pre_termination_date.pretrial_termination_date,
    
    -- arrest charge flags
    book_flags.has_special_charge as arrest_special_flag,
    book_flags.has_serious_felony_charge as arrest_serious_flag,
    book_flags.has_violent_felony_charge as arrest_violent_flag,
    book_flags.has_violent_psa_charge as arrest_violent_psa_flag,
    book_flags.has_capital_charge as arrest_capital_flag,
    book_flags.has_dv_charge as arrest_dv_flag,
    book_flags.has_marijuana_charge as arrest_marijuana_flag,
    book_flags.has_sup_vio_charge as arrest_sup_vio_flag,
    book_flags.has_fta_charge as arrest_fta_flag,
    book_flags.has_sex_charge as arrest_sex_flag,
    book_flags.has_dui_charge as arrest_dui_flag,
    book_flags.has_restrain_charge as arrest_restrain_flag,
    book_flags.has_property_charge as arrest_property_flag,
    book_flags.has_drug_charge as arrest_drug_flag,
    book_flags.has_dv_possible_charge as arrest_dv_possible_flag,
    book_flags.booking_charge_group_level as arrest_charge_level, -- will need to convert into misdo and felony flags in R
    
    -- filing charge flags
    MAX(case when court_flags.has_special_charge = 'Yes' THEN 1 else 0 end) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_special_flag,
    MAX(case when court_flags.has_serious_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_serious_flag,
    MAX(case when court_flags.has_violent_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_flag,
    MAX(case when court_flags.has_violent_psa_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_psa_flag,
    MAX(case when court_flags.has_capital_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_capital_flag,
    MAX(case when court_flags.has_dv_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_flag,
    MAX(case when court_flags.has_marijuana_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_marijuana_flag,
    MAX(case when court_flags.has_sup_vio_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sup_vio_flag,
    MAX(case when court_flags.has_fta_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_fta_flag,
    MAX(case when court_flags.has_sex_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sex_flag,
    MAX(case when court_flags.has_dui_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dui_flag,
    MAX(case when court_flags.has_restrain_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_restrain_flag,
    MAX(case when court_flags.has_property_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_property_flag,
    MAX(case when court_flags.has_drug_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_drug_flag,
    MAX(case when court_flags.has_dv_possible_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_possible_flag,  
    -- conviction charge flags not currently possible
    arrest_date.arrest_date
    

from "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."FACT_BOOKING" as booking 
        on agg.booking_key = booking.booking_key
    left join "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_BOOKING_CHARGE" as book_charge 
        on booking.dim_principal_booking_charge_key = book_charge.dim_principal_booking_charge_key
    left join "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case 
        on agg.booking_key = court_case.booking_key
    left join "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_COURT_CHARGE" as court_charge 
        on court_case.dim_principal_court_charge_key = court_charge.dim_principal_court_charge_key
    left join "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
        on court_case.dim_court_detail_key = court_detail.dim_court_detail_key
    left join "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as pre_assessment
        on agg.pretrial_assessment_key = pre_assessment.pretrial_assessment_key
    left join "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."DIM_RELEASE_CONDITION" as release_cond
        on pre_assessment.dim_release_condition_key = release_cond.dim_release_condition_key
    left join "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as pre_detail
        on pre_assessment.dim_pretrial_detail_key = pre_detail.dim_pretrial_detail_key
    left join "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_TERMINATION_DATE" as pre_termination_date
        on pre_assessment.dim_pretrial_termination_date_key = pre_termination_date.dim_pretrial_termination_date_key
    left join "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."DIM_BOOKING_CHARGE_GROUP" as book_flags
        on booking.dim_booking_charge_group_key = book_flags.dim_booking_charge_group_key
    left join "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."DIM_COURT_CHARGE_GROUP" as court_flags
        on court_case.dim_court_charge_group_key = court_flags.dim_court_charge_group_key
    left join "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."DIM_ARREST_DATE" as arrest_date
        on booking.dim_arrest_date_key = arrest_date.dim_arrest_date_key
    
 

---------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------
UNION ALL


select
    distinct
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
    agg.is_release_eligible,
    agg.release_eligible_count,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.is_released_prearraignment,
    agg.released_prearraignment_count,
    agg.is_released_pretrial,
    agg.released_pretrial_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.has_new_criminal_activity,
    agg.new_criminal_activity_count,
    agg.has_pretrial_period_end,
    agg.pretrial_period_end_count,
    agg.has_pretrial_violation,
    agg.pretrial_violation_count,
    agg.is_unmatched_booking,
    agg.unmatched_booking_count,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name, pre_assessment.risk_score, pre_assessment.risk_score_raw, pre_assessment.score_failure_to_appear, pre_assessment.score_new_criminal_activity, pre_assessment.score_new_criminal_violent_activity, pre_assessment.generic_tool_total_score, pre_assessment.generic_tool_total_score_raw,
    book_charge.principal_booking_offense_level as booking_charge_level,
    book_charge.principal_booking_offense_type as booking_charge_code,
    book_charge.principal_booking_charge as booking_charge,
    book_charge.principal_booking_charge_hierarchy_code as booking_charge_hierarchy,
    FIRST_VALUE(court_charge.principal_court_charge) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge,
    FIRST_VALUE(court_charge.principal_court_offense_level) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_level,
    FIRST_VALUE(court_charge.principal_court_offense_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_code,
    FIRST_VALUE(court_charge.principal_court_charge_hierarchy_code) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_hierarchy,
    MIN(court_case.dim_plea_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS plea_date_key, -- need plea date dimension to get actual date out
    MIN(court_case.dim_sentence_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS sentence_date_key, -- need sentence date dimension to get actual date out
    FIRST_VALUE(court_detail.plea_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
                case when plea_type = 'Guilty' then 1
                     when plea_type = 'Nolo Contendere' then 2
                     when plea_type = 'Not Guilty' then 3
                     else null end          
          ) AS plea_type,
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
        ) AS disposition_type,
    FIRST_VALUE(court_detail.sentence_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when sentence_type = 'Prison' then 1
                when sentence_type = 'Jail' then 2
                when sentence_type = 'Probation' then 3
                when sentence_type = 'Fine' then 4
                when sentence_type = 'No Sentence Information' then 5
         else null end
        ) AS sentence_type,

    -- pretrial monitoring flags
    pre_detail.monitoring_level,
    pre_detail.court_date_reminder_flag,
    pre_detail.transit_service_flag,
    pre_detail.other_pretrial_service,
    pre_detail.condition_phone_flag,
    pre_detail.condition_inperson_flag,
    pre_detail.condition_drug_test_flag,
    pre_detail.condition_alcohol_flag,
    pre_detail.condition_em_flag,
    pre_detail.condition_no_contact_flag,
    
    pre_detail.release_decision_prearraignment,
    pre_detail.release_decision_arraignment,
    pre_detail.pretrial_release_type, -- what is this compared to normal release type??
    pre_detail.pretrial_termination_outcome,
    pre_detail.pretrial_termination_reason,
    pre_termination_date.pretrial_termination_date,
    
    -- arrest charge flags
    book_flags.has_special_charge as arrest_special_flag,
    book_flags.has_serious_felony_charge as arrest_serious_flag,
    book_flags.has_violent_felony_charge as arrest_violent_flag,
    book_flags.has_violent_psa_charge as arrest_violent_psa_flag,
    book_flags.has_capital_charge as arrest_capital_flag,
    book_flags.has_dv_charge as arrest_dv_flag,
    book_flags.has_marijuana_charge as arrest_marijuana_flag,
    book_flags.has_sup_vio_charge as arrest_sup_vio_flag,
    book_flags.has_fta_charge as arrest_fta_flag,
    book_flags.has_sex_charge as arrest_sex_flag,
    book_flags.has_dui_charge as arrest_dui_flag,
    book_flags.has_restrain_charge as arrest_restrain_flag,
    book_flags.has_property_charge as arrest_property_flag,
    book_flags.has_drug_charge as arrest_drug_flag,
    book_flags.has_dv_possible_charge as arrest_dv_possible_flag,
    book_flags.booking_charge_group_level as arrest_charge_level, -- will need to convert into misdo and felony flags in R
    
    -- filing charge flags
    MAX(case when court_flags.has_special_charge = 'Yes' THEN 1 else 0 end) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_special_flag,
    MAX(case when court_flags.has_serious_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_serious_flag,
    MAX(case when court_flags.has_violent_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_flag,
    MAX(case when court_flags.has_violent_psa_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_psa_flag,
    MAX(case when court_flags.has_capital_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_capital_flag,
    MAX(case when court_flags.has_dv_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_flag,
    MAX(case when court_flags.has_marijuana_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_marijuana_flag,
    MAX(case when court_flags.has_sup_vio_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sup_vio_flag,
    MAX(case when court_flags.has_fta_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_fta_flag,
    MAX(case when court_flags.has_sex_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sex_flag,
    MAX(case when court_flags.has_dui_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dui_flag,
    MAX(case when court_flags.has_restrain_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_restrain_flag,
    MAX(case when court_flags.has_property_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_property_flag,
    MAX(case when court_flags.has_drug_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_drug_flag,
    MAX(case when court_flags.has_dv_possible_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_possible_flag,  
    -- conviction charge flags not currently possible
    arrest_date.arrest_date
    

from "16_KINGS-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "16_KINGS-DWH_PROD"."SECURE_SHARE"."FACT_BOOKING" as booking 
        on agg.booking_key = booking.booking_key
    left join "16_KINGS-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_BOOKING_CHARGE" as book_charge 
        on booking.dim_principal_booking_charge_key = book_charge.dim_principal_booking_charge_key
    left join "16_KINGS-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case 
        on agg.booking_key = court_case.booking_key
    left join "16_KINGS-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_COURT_CHARGE" as court_charge 
        on court_case.dim_principal_court_charge_key = court_charge.dim_principal_court_charge_key
    left join "16_KINGS-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
        on court_case.dim_court_detail_key = court_detail.dim_court_detail_key
    left join "16_KINGS-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as pre_assessment
        on agg.pretrial_assessment_key = pre_assessment.pretrial_assessment_key
    left join "16_KINGS-DWH_PROD"."SECURE_SHARE"."DIM_RELEASE_CONDITION" as release_cond
        on pre_assessment.dim_release_condition_key = release_cond.dim_release_condition_key
    left join "16_KINGS-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as pre_detail
        on pre_assessment.dim_pretrial_detail_key = pre_detail.dim_pretrial_detail_key
    left join "16_KINGS-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_TERMINATION_DATE" as pre_termination_date
        on pre_assessment.dim_pretrial_termination_date_key = pre_termination_date.dim_pretrial_termination_date_key
    left join "16_KINGS-DWH_PROD"."SECURE_SHARE"."DIM_BOOKING_CHARGE_GROUP" as book_flags
        on booking.dim_booking_charge_group_key = book_flags.dim_booking_charge_group_key
    left join "16_KINGS-DWH_PROD"."SECURE_SHARE"."DIM_COURT_CHARGE_GROUP" as court_flags
        on court_case.dim_court_charge_group_key = court_flags.dim_court_charge_group_key
    left join "16_KINGS-DWH_PROD"."SECURE_SHARE"."DIM_ARREST_DATE" as arrest_date
        on booking.dim_arrest_date_key = arrest_date.dim_arrest_date_key
    
 

---------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------
UNION ALL


select
    distinct
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
    agg.is_release_eligible,
    agg.release_eligible_count,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.is_released_prearraignment,
    agg.released_prearraignment_count,
    agg.is_released_pretrial,
    agg.released_pretrial_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.has_new_criminal_activity,
    agg.new_criminal_activity_count,
    agg.has_pretrial_period_end,
    agg.pretrial_period_end_count,
    agg.has_pretrial_violation,
    agg.pretrial_violation_count,
    agg.is_unmatched_booking,
    agg.unmatched_booking_count,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name, pre_assessment.risk_score, pre_assessment.risk_score_raw, pre_assessment.score_failure_to_appear, pre_assessment.score_new_criminal_activity, pre_assessment.score_new_criminal_violent_activity, pre_assessment.generic_tool_total_score, pre_assessment.generic_tool_total_score_raw,
    book_charge.principal_booking_offense_level as booking_charge_level,
    book_charge.principal_booking_offense_type as booking_charge_code,
    book_charge.principal_booking_charge as booking_charge,
    book_charge.principal_booking_charge_hierarchy_code as booking_charge_hierarchy,
    FIRST_VALUE(court_charge.principal_court_charge) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge,
    FIRST_VALUE(court_charge.principal_court_offense_level) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_level,
    FIRST_VALUE(court_charge.principal_court_offense_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_code,
    FIRST_VALUE(court_charge.principal_court_charge_hierarchy_code) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_hierarchy,
    MIN(court_case.dim_plea_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS plea_date_key, -- need plea date dimension to get actual date out
    MIN(court_case.dim_sentence_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS sentence_date_key, -- need sentence date dimension to get actual date out
    FIRST_VALUE(court_detail.plea_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
                case when plea_type = 'Guilty' then 1
                     when plea_type = 'Nolo Contendere' then 2
                     when plea_type = 'Not Guilty' then 3
                     else null end          
          ) AS plea_type,
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
        ) AS disposition_type,
    FIRST_VALUE(court_detail.sentence_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when sentence_type = 'Prison' then 1
                when sentence_type = 'Jail' then 2
                when sentence_type = 'Probation' then 3
                when sentence_type = 'Fine' then 4
                when sentence_type = 'No Sentence Information' then 5
         else null end
        ) AS sentence_type,

    -- pretrial monitoring flags
    pre_detail.monitoring_level,
    pre_detail.court_date_reminder_flag,
    pre_detail.transit_service_flag,
    pre_detail.other_pretrial_service,
    pre_detail.condition_phone_flag,
    pre_detail.condition_inperson_flag,
    pre_detail.condition_drug_test_flag,
    pre_detail.condition_alcohol_flag,
    pre_detail.condition_em_flag,
    pre_detail.condition_no_contact_flag,
    
    pre_detail.release_decision_prearraignment,
    pre_detail.release_decision_arraignment,
    pre_detail.pretrial_release_type, -- what is this compared to normal release type??
    pre_detail.pretrial_termination_outcome,
    pre_detail.pretrial_termination_reason,
    pre_termination_date.pretrial_termination_date,
    
    -- arrest charge flags
    book_flags.has_special_charge as arrest_special_flag,
    book_flags.has_serious_felony_charge as arrest_serious_flag,
    book_flags.has_violent_felony_charge as arrest_violent_flag,
    book_flags.has_violent_psa_charge as arrest_violent_psa_flag,
    book_flags.has_capital_charge as arrest_capital_flag,
    book_flags.has_dv_charge as arrest_dv_flag,
    book_flags.has_marijuana_charge as arrest_marijuana_flag,
    book_flags.has_sup_vio_charge as arrest_sup_vio_flag,
    book_flags.has_fta_charge as arrest_fta_flag,
    book_flags.has_sex_charge as arrest_sex_flag,
    book_flags.has_dui_charge as arrest_dui_flag,
    book_flags.has_restrain_charge as arrest_restrain_flag,
    book_flags.has_property_charge as arrest_property_flag,
    book_flags.has_drug_charge as arrest_drug_flag,
    book_flags.has_dv_possible_charge as arrest_dv_possible_flag,
    book_flags.booking_charge_group_level as arrest_charge_level, -- will need to convert into misdo and felony flags in R
    
    -- filing charge flags
    MAX(case when court_flags.has_special_charge = 'Yes' THEN 1 else 0 end) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_special_flag,
    MAX(case when court_flags.has_serious_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_serious_flag,
    MAX(case when court_flags.has_violent_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_flag,
    MAX(case when court_flags.has_violent_psa_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_psa_flag,
    MAX(case when court_flags.has_capital_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_capital_flag,
    MAX(case when court_flags.has_dv_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_flag,
    MAX(case when court_flags.has_marijuana_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_marijuana_flag,
    MAX(case when court_flags.has_sup_vio_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sup_vio_flag,
    MAX(case when court_flags.has_fta_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_fta_flag,
    MAX(case when court_flags.has_sex_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sex_flag,
    MAX(case when court_flags.has_dui_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dui_flag,
    MAX(case when court_flags.has_restrain_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_restrain_flag,
    MAX(case when court_flags.has_property_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_property_flag,
    MAX(case when court_flags.has_drug_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_drug_flag,
    MAX(case when court_flags.has_dv_possible_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_possible_flag,  
    -- conviction charge flags not currently possible
    arrest_date.arrest_date
    

from "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."FACT_BOOKING" as booking 
        on agg.booking_key = booking.booking_key
    left join "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_BOOKING_CHARGE" as book_charge 
        on booking.dim_principal_booking_charge_key = book_charge.dim_principal_booking_charge_key
    left join "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case 
        on agg.booking_key = court_case.booking_key
    left join "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_COURT_CHARGE" as court_charge 
        on court_case.dim_principal_court_charge_key = court_charge.dim_principal_court_charge_key
    left join "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
        on court_case.dim_court_detail_key = court_detail.dim_court_detail_key
    left join "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as pre_assessment
        on agg.pretrial_assessment_key = pre_assessment.pretrial_assessment_key
    left join "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."DIM_RELEASE_CONDITION" as release_cond
        on pre_assessment.dim_release_condition_key = release_cond.dim_release_condition_key
    left join "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as pre_detail
        on pre_assessment.dim_pretrial_detail_key = pre_detail.dim_pretrial_detail_key
    left join "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_TERMINATION_DATE" as pre_termination_date
        on pre_assessment.dim_pretrial_termination_date_key = pre_termination_date.dim_pretrial_termination_date_key
    left join "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."DIM_BOOKING_CHARGE_GROUP" as book_flags
        on booking.dim_booking_charge_group_key = book_flags.dim_booking_charge_group_key
    left join "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."DIM_COURT_CHARGE_GROUP" as court_flags
        on court_case.dim_court_charge_group_key = court_flags.dim_court_charge_group_key
    left join "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."DIM_ARREST_DATE" as arrest_date
        on booking.dim_arrest_date_key = arrest_date.dim_arrest_date_key
    
 

---------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------
UNION ALL


select
    distinct
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
    agg.is_release_eligible,
    agg.release_eligible_count,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.is_released_prearraignment,
    agg.released_prearraignment_count,
    agg.is_released_pretrial,
    agg.released_pretrial_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.has_new_criminal_activity,
    agg.new_criminal_activity_count,
    agg.has_pretrial_period_end,
    agg.pretrial_period_end_count,
    agg.has_pretrial_violation,
    agg.pretrial_violation_count,
    agg.is_unmatched_booking,
    agg.unmatched_booking_count,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name, pre_assessment.risk_score, pre_assessment.risk_score_raw, pre_assessment.score_failure_to_appear, pre_assessment.score_new_criminal_activity, pre_assessment.score_new_criminal_violent_activity, pre_assessment.generic_tool_total_score, pre_assessment.generic_tool_total_score_raw,
    book_charge.principal_booking_offense_level as booking_charge_level,
    book_charge.principal_booking_offense_type as booking_charge_code,
    book_charge.principal_booking_charge as booking_charge,
    book_charge.principal_booking_charge_hierarchy_code as booking_charge_hierarchy,
    FIRST_VALUE(court_charge.principal_court_charge) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge,
    FIRST_VALUE(court_charge.principal_court_offense_level) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_level,
    FIRST_VALUE(court_charge.principal_court_offense_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_code,
    FIRST_VALUE(court_charge.principal_court_charge_hierarchy_code) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_hierarchy,
    MIN(court_case.dim_plea_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS plea_date_key, -- need plea date dimension to get actual date out
    MIN(court_case.dim_sentence_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS sentence_date_key, -- need sentence date dimension to get actual date out
    FIRST_VALUE(court_detail.plea_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
                case when plea_type = 'Guilty' then 1
                     when plea_type = 'Nolo Contendere' then 2
                     when plea_type = 'Not Guilty' then 3
                     else null end          
          ) AS plea_type,
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
        ) AS disposition_type,
    FIRST_VALUE(court_detail.sentence_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when sentence_type = 'Prison' then 1
                when sentence_type = 'Jail' then 2
                when sentence_type = 'Probation' then 3
                when sentence_type = 'Fine' then 4
                when sentence_type = 'No Sentence Information' then 5
         else null end
        ) AS sentence_type,

    -- pretrial monitoring flags
    pre_detail.monitoring_level,
    pre_detail.court_date_reminder_flag,
    pre_detail.transit_service_flag,
    pre_detail.other_pretrial_service,
    pre_detail.condition_phone_flag,
    pre_detail.condition_inperson_flag,
    pre_detail.condition_drug_test_flag,
    pre_detail.condition_alcohol_flag,
    pre_detail.condition_em_flag,
    pre_detail.condition_no_contact_flag,
    
    pre_detail.release_decision_prearraignment,
    pre_detail.release_decision_arraignment,
    pre_detail.pretrial_release_type, -- what is this compared to normal release type??
    pre_detail.pretrial_termination_outcome,
    pre_detail.pretrial_termination_reason,
    pre_termination_date.pretrial_termination_date,
    
    -- arrest charge flags
    book_flags.has_special_charge as arrest_special_flag,
    book_flags.has_serious_felony_charge as arrest_serious_flag,
    book_flags.has_violent_felony_charge as arrest_violent_flag,
    book_flags.has_violent_psa_charge as arrest_violent_psa_flag,
    book_flags.has_capital_charge as arrest_capital_flag,
    book_flags.has_dv_charge as arrest_dv_flag,
    book_flags.has_marijuana_charge as arrest_marijuana_flag,
    book_flags.has_sup_vio_charge as arrest_sup_vio_flag,
    book_flags.has_fta_charge as arrest_fta_flag,
    book_flags.has_sex_charge as arrest_sex_flag,
    book_flags.has_dui_charge as arrest_dui_flag,
    book_flags.has_restrain_charge as arrest_restrain_flag,
    book_flags.has_property_charge as arrest_property_flag,
    book_flags.has_drug_charge as arrest_drug_flag,
    book_flags.has_dv_possible_charge as arrest_dv_possible_flag,
    book_flags.booking_charge_group_level as arrest_charge_level, -- will need to convert into misdo and felony flags in R
    
    -- filing charge flags
    MAX(case when court_flags.has_special_charge = 'Yes' THEN 1 else 0 end) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_special_flag,
    MAX(case when court_flags.has_serious_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_serious_flag,
    MAX(case when court_flags.has_violent_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_flag,
    MAX(case when court_flags.has_violent_psa_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_psa_flag,
    MAX(case when court_flags.has_capital_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_capital_flag,
    MAX(case when court_flags.has_dv_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_flag,
    MAX(case when court_flags.has_marijuana_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_marijuana_flag,
    MAX(case when court_flags.has_sup_vio_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sup_vio_flag,
    MAX(case when court_flags.has_fta_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_fta_flag,
    MAX(case when court_flags.has_sex_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sex_flag,
    MAX(case when court_flags.has_dui_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dui_flag,
    MAX(case when court_flags.has_restrain_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_restrain_flag,
    MAX(case when court_flags.has_property_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_property_flag,
    MAX(case when court_flags.has_drug_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_drug_flag,
    MAX(case when court_flags.has_dv_possible_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_possible_flag,  
    -- conviction charge flags not currently possible
    arrest_date.arrest_date
    

from "25_MODOC-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "25_MODOC-DWH_PROD"."SECURE_SHARE"."FACT_BOOKING" as booking 
        on agg.booking_key = booking.booking_key
    left join "25_MODOC-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_BOOKING_CHARGE" as book_charge 
        on booking.dim_principal_booking_charge_key = book_charge.dim_principal_booking_charge_key
    left join "25_MODOC-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case 
        on agg.booking_key = court_case.booking_key
    left join "25_MODOC-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_COURT_CHARGE" as court_charge 
        on court_case.dim_principal_court_charge_key = court_charge.dim_principal_court_charge_key
    left join "25_MODOC-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
        on court_case.dim_court_detail_key = court_detail.dim_court_detail_key
    left join "25_MODOC-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as pre_assessment
        on agg.pretrial_assessment_key = pre_assessment.pretrial_assessment_key
    left join "25_MODOC-DWH_PROD"."SECURE_SHARE"."DIM_RELEASE_CONDITION" as release_cond
        on pre_assessment.dim_release_condition_key = release_cond.dim_release_condition_key
    left join "25_MODOC-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as pre_detail
        on pre_assessment.dim_pretrial_detail_key = pre_detail.dim_pretrial_detail_key
    left join "25_MODOC-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_TERMINATION_DATE" as pre_termination_date
        on pre_assessment.dim_pretrial_termination_date_key = pre_termination_date.dim_pretrial_termination_date_key
    left join "25_MODOC-DWH_PROD"."SECURE_SHARE"."DIM_BOOKING_CHARGE_GROUP" as book_flags
        on booking.dim_booking_charge_group_key = book_flags.dim_booking_charge_group_key
    left join "25_MODOC-DWH_PROD"."SECURE_SHARE"."DIM_COURT_CHARGE_GROUP" as court_flags
        on court_case.dim_court_charge_group_key = court_flags.dim_court_charge_group_key
    left join "25_MODOC-DWH_PROD"."SECURE_SHARE"."DIM_ARREST_DATE" as arrest_date
        on booking.dim_arrest_date_key = arrest_date.dim_arrest_date_key
    
 

---------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------
UNION ALL


select
    distinct
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
    agg.is_release_eligible,
    agg.release_eligible_count,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.is_released_prearraignment,
    agg.released_prearraignment_count,
    agg.is_released_pretrial,
    agg.released_pretrial_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.has_new_criminal_activity,
    agg.new_criminal_activity_count,
    agg.has_pretrial_period_end,
    agg.pretrial_period_end_count,
    agg.has_pretrial_violation,
    agg.pretrial_violation_count,
    agg.is_unmatched_booking,
    agg.unmatched_booking_count,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name, pre_assessment.risk_score, pre_assessment.risk_score_raw, pre_assessment.score_failure_to_appear, pre_assessment.score_new_criminal_activity, pre_assessment.score_new_criminal_violent_activity, pre_assessment.generic_tool_total_score, pre_assessment.generic_tool_total_score_raw,
    book_charge.principal_booking_offense_level as booking_charge_level,
    book_charge.principal_booking_offense_type as booking_charge_code,
    book_charge.principal_booking_charge as booking_charge,
    book_charge.principal_booking_charge_hierarchy_code as booking_charge_hierarchy,
    FIRST_VALUE(court_charge.principal_court_charge) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge,
    FIRST_VALUE(court_charge.principal_court_offense_level) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_level,
    FIRST_VALUE(court_charge.principal_court_offense_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_code,
    FIRST_VALUE(court_charge.principal_court_charge_hierarchy_code) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_hierarchy,
    MIN(court_case.dim_plea_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS plea_date_key, -- need plea date dimension to get actual date out
    MIN(court_case.dim_sentence_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS sentence_date_key, -- need sentence date dimension to get actual date out
    FIRST_VALUE(court_detail.plea_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
                case when plea_type = 'Guilty' then 1
                     when plea_type = 'Nolo Contendere' then 2
                     when plea_type = 'Not Guilty' then 3
                     else null end          
          ) AS plea_type,
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
        ) AS disposition_type,
    FIRST_VALUE(court_detail.sentence_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when sentence_type = 'Prison' then 1
                when sentence_type = 'Jail' then 2
                when sentence_type = 'Probation' then 3
                when sentence_type = 'Fine' then 4
                when sentence_type = 'No Sentence Information' then 5
         else null end
        ) AS sentence_type,

    -- pretrial monitoring flags
    pre_detail.monitoring_level,
    pre_detail.court_date_reminder_flag,
    pre_detail.transit_service_flag,
    pre_detail.other_pretrial_service,
    pre_detail.condition_phone_flag,
    pre_detail.condition_inperson_flag,
    pre_detail.condition_drug_test_flag,
    pre_detail.condition_alcohol_flag,
    pre_detail.condition_em_flag,
    pre_detail.condition_no_contact_flag,
    
    pre_detail.release_decision_prearraignment,
    pre_detail.release_decision_arraignment,
    pre_detail.pretrial_release_type, -- what is this compared to normal release type??
    pre_detail.pretrial_termination_outcome,
    pre_detail.pretrial_termination_reason,
    pre_termination_date.pretrial_termination_date,
    
    -- arrest charge flags
    book_flags.has_special_charge as arrest_special_flag,
    book_flags.has_serious_felony_charge as arrest_serious_flag,
    book_flags.has_violent_felony_charge as arrest_violent_flag,
    book_flags.has_violent_psa_charge as arrest_violent_psa_flag,
    book_flags.has_capital_charge as arrest_capital_flag,
    book_flags.has_dv_charge as arrest_dv_flag,
    book_flags.has_marijuana_charge as arrest_marijuana_flag,
    book_flags.has_sup_vio_charge as arrest_sup_vio_flag,
    book_flags.has_fta_charge as arrest_fta_flag,
    book_flags.has_sex_charge as arrest_sex_flag,
    book_flags.has_dui_charge as arrest_dui_flag,
    book_flags.has_restrain_charge as arrest_restrain_flag,
    book_flags.has_property_charge as arrest_property_flag,
    book_flags.has_drug_charge as arrest_drug_flag,
    book_flags.has_dv_possible_charge as arrest_dv_possible_flag,
    book_flags.booking_charge_group_level as arrest_charge_level, -- will need to convert into misdo and felony flags in R
    
    -- filing charge flags
    MAX(case when court_flags.has_special_charge = 'Yes' THEN 1 else 0 end) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_special_flag,
    MAX(case when court_flags.has_serious_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_serious_flag,
    MAX(case when court_flags.has_violent_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_flag,
    MAX(case when court_flags.has_violent_psa_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_psa_flag,
    MAX(case when court_flags.has_capital_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_capital_flag,
    MAX(case when court_flags.has_dv_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_flag,
    MAX(case when court_flags.has_marijuana_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_marijuana_flag,
    MAX(case when court_flags.has_sup_vio_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sup_vio_flag,
    MAX(case when court_flags.has_fta_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_fta_flag,
    MAX(case when court_flags.has_sex_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sex_flag,
    MAX(case when court_flags.has_dui_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dui_flag,
    MAX(case when court_flags.has_restrain_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_restrain_flag,
    MAX(case when court_flags.has_property_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_property_flag,
    MAX(case when court_flags.has_drug_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_drug_flag,
    MAX(case when court_flags.has_dv_possible_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_possible_flag,  
    -- conviction charge flags not currently possible
    arrest_date.arrest_date
    

from "28_NAPA-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "28_NAPA-DWH_PROD"."SECURE_SHARE"."FACT_BOOKING" as booking 
        on agg.booking_key = booking.booking_key
    left join "28_NAPA-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_BOOKING_CHARGE" as book_charge 
        on booking.dim_principal_booking_charge_key = book_charge.dim_principal_booking_charge_key
    left join "28_NAPA-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case 
        on agg.booking_key = court_case.booking_key
    left join "28_NAPA-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_COURT_CHARGE" as court_charge 
        on court_case.dim_principal_court_charge_key = court_charge.dim_principal_court_charge_key
    left join "28_NAPA-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
        on court_case.dim_court_detail_key = court_detail.dim_court_detail_key
    left join "28_NAPA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as pre_assessment
        on agg.pretrial_assessment_key = pre_assessment.pretrial_assessment_key
    left join "28_NAPA-DWH_PROD"."SECURE_SHARE"."DIM_RELEASE_CONDITION" as release_cond
        on pre_assessment.dim_release_condition_key = release_cond.dim_release_condition_key
    left join "28_NAPA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as pre_detail
        on pre_assessment.dim_pretrial_detail_key = pre_detail.dim_pretrial_detail_key
    left join "28_NAPA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_TERMINATION_DATE" as pre_termination_date
        on pre_assessment.dim_pretrial_termination_date_key = pre_termination_date.dim_pretrial_termination_date_key
    left join "28_NAPA-DWH_PROD"."SECURE_SHARE"."DIM_BOOKING_CHARGE_GROUP" as book_flags
        on booking.dim_booking_charge_group_key = book_flags.dim_booking_charge_group_key
    left join "28_NAPA-DWH_PROD"."SECURE_SHARE"."DIM_COURT_CHARGE_GROUP" as court_flags
        on court_case.dim_court_charge_group_key = court_flags.dim_court_charge_group_key
    left join "28_NAPA-DWH_PROD"."SECURE_SHARE"."DIM_ARREST_DATE" as arrest_date
        on booking.dim_arrest_date_key = arrest_date.dim_arrest_date_key
    
 

---------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------
UNION ALL


select
    distinct
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
    agg.is_release_eligible,
    agg.release_eligible_count,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.is_released_prearraignment,
    agg.released_prearraignment_count,
    agg.is_released_pretrial,
    agg.released_pretrial_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.has_new_criminal_activity,
    agg.new_criminal_activity_count,
    agg.has_pretrial_period_end,
    agg.pretrial_period_end_count,
    agg.has_pretrial_violation,
    agg.pretrial_violation_count,
    agg.is_unmatched_booking,
    agg.unmatched_booking_count,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name, pre_assessment.risk_score, pre_assessment.risk_score_raw, pre_assessment.score_failure_to_appear, pre_assessment.score_new_criminal_activity, pre_assessment.score_new_criminal_violent_activity, pre_assessment.generic_tool_total_score, pre_assessment.generic_tool_total_score_raw,
    book_charge.principal_booking_offense_level as booking_charge_level,
    book_charge.principal_booking_offense_type as booking_charge_code,
    book_charge.principal_booking_charge as booking_charge,
    book_charge.principal_booking_charge_hierarchy_code as booking_charge_hierarchy,
    FIRST_VALUE(court_charge.principal_court_charge) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge,
    FIRST_VALUE(court_charge.principal_court_offense_level) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_level,
    FIRST_VALUE(court_charge.principal_court_offense_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_code,
    FIRST_VALUE(court_charge.principal_court_charge_hierarchy_code) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_hierarchy,
    MIN(court_case.dim_plea_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS plea_date_key, -- need plea date dimension to get actual date out
    MIN(court_case.dim_sentence_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS sentence_date_key, -- need sentence date dimension to get actual date out
    FIRST_VALUE(court_detail.plea_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
                case when plea_type = 'Guilty' then 1
                     when plea_type = 'Nolo Contendere' then 2
                     when plea_type = 'Not Guilty' then 3
                     else null end          
          ) AS plea_type,
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
        ) AS disposition_type,
    FIRST_VALUE(court_detail.sentence_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when sentence_type = 'Prison' then 1
                when sentence_type = 'Jail' then 2
                when sentence_type = 'Probation' then 3
                when sentence_type = 'Fine' then 4
                when sentence_type = 'No Sentence Information' then 5
         else null end
        ) AS sentence_type,

    -- pretrial monitoring flags
    pre_detail.monitoring_level,
    pre_detail.court_date_reminder_flag,
    pre_detail.transit_service_flag,
    pre_detail.other_pretrial_service,
    pre_detail.condition_phone_flag,
    pre_detail.condition_inperson_flag,
    pre_detail.condition_drug_test_flag,
    pre_detail.condition_alcohol_flag,
    pre_detail.condition_em_flag,
    pre_detail.condition_no_contact_flag,
    
    pre_detail.release_decision_prearraignment,
    pre_detail.release_decision_arraignment,
    pre_detail.pretrial_release_type, -- what is this compared to normal release type??
    pre_detail.pretrial_termination_outcome,
    pre_detail.pretrial_termination_reason,
    pre_termination_date.pretrial_termination_date,
    
    -- arrest charge flags
    book_flags.has_special_charge as arrest_special_flag,
    book_flags.has_serious_felony_charge as arrest_serious_flag,
    book_flags.has_violent_felony_charge as arrest_violent_flag,
    book_flags.has_violent_psa_charge as arrest_violent_psa_flag,
    book_flags.has_capital_charge as arrest_capital_flag,
    book_flags.has_dv_charge as arrest_dv_flag,
    book_flags.has_marijuana_charge as arrest_marijuana_flag,
    book_flags.has_sup_vio_charge as arrest_sup_vio_flag,
    book_flags.has_fta_charge as arrest_fta_flag,
    book_flags.has_sex_charge as arrest_sex_flag,
    book_flags.has_dui_charge as arrest_dui_flag,
    book_flags.has_restrain_charge as arrest_restrain_flag,
    book_flags.has_property_charge as arrest_property_flag,
    book_flags.has_drug_charge as arrest_drug_flag,
    book_flags.has_dv_possible_charge as arrest_dv_possible_flag,
    book_flags.booking_charge_group_level as arrest_charge_level, -- will need to convert into misdo and felony flags in R
    
    -- filing charge flags
    MAX(case when court_flags.has_special_charge = 'Yes' THEN 1 else 0 end) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_special_flag,
    MAX(case when court_flags.has_serious_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_serious_flag,
    MAX(case when court_flags.has_violent_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_flag,
    MAX(case when court_flags.has_violent_psa_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_psa_flag,
    MAX(case when court_flags.has_capital_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_capital_flag,
    MAX(case when court_flags.has_dv_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_flag,
    MAX(case when court_flags.has_marijuana_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_marijuana_flag,
    MAX(case when court_flags.has_sup_vio_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sup_vio_flag,
    MAX(case when court_flags.has_fta_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_fta_flag,
    MAX(case when court_flags.has_sex_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sex_flag,
    MAX(case when court_flags.has_dui_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dui_flag,
    MAX(case when court_flags.has_restrain_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_restrain_flag,
    MAX(case when court_flags.has_property_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_property_flag,
    MAX(case when court_flags.has_drug_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_drug_flag,
    MAX(case when court_flags.has_dv_possible_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_possible_flag,  
    -- conviction charge flags not currently possible
    arrest_date.arrest_date
    

from "29_NEVADA-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "29_NEVADA-DWH_PROD"."SECURE_SHARE"."FACT_BOOKING" as booking 
        on agg.booking_key = booking.booking_key
    left join "29_NEVADA-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_BOOKING_CHARGE" as book_charge 
        on booking.dim_principal_booking_charge_key = book_charge.dim_principal_booking_charge_key
    left join "29_NEVADA-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case 
        on agg.booking_key = court_case.booking_key
    left join "29_NEVADA-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_COURT_CHARGE" as court_charge 
        on court_case.dim_principal_court_charge_key = court_charge.dim_principal_court_charge_key
    left join "29_NEVADA-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
        on court_case.dim_court_detail_key = court_detail.dim_court_detail_key
    left join "29_NEVADA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as pre_assessment
        on agg.pretrial_assessment_key = pre_assessment.pretrial_assessment_key
    left join "29_NEVADA-DWH_PROD"."SECURE_SHARE"."DIM_RELEASE_CONDITION" as release_cond
        on pre_assessment.dim_release_condition_key = release_cond.dim_release_condition_key
    left join "29_NEVADA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as pre_detail
        on pre_assessment.dim_pretrial_detail_key = pre_detail.dim_pretrial_detail_key
    left join "29_NEVADA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_TERMINATION_DATE" as pre_termination_date
        on pre_assessment.dim_pretrial_termination_date_key = pre_termination_date.dim_pretrial_termination_date_key
    left join "29_NEVADA-DWH_PROD"."SECURE_SHARE"."DIM_BOOKING_CHARGE_GROUP" as book_flags
        on booking.dim_booking_charge_group_key = book_flags.dim_booking_charge_group_key
    left join "29_NEVADA-DWH_PROD"."SECURE_SHARE"."DIM_COURT_CHARGE_GROUP" as court_flags
        on court_case.dim_court_charge_group_key = court_flags.dim_court_charge_group_key
    left join "29_NEVADA-DWH_PROD"."SECURE_SHARE"."DIM_ARREST_DATE" as arrest_date
        on booking.dim_arrest_date_key = arrest_date.dim_arrest_date_key
    
 

---------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------
UNION ALL


select
    distinct
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
    agg.is_release_eligible,
    agg.release_eligible_count,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.is_released_prearraignment,
    agg.released_prearraignment_count,
    agg.is_released_pretrial,
    agg.released_pretrial_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.has_new_criminal_activity,
    agg.new_criminal_activity_count,
    agg.has_pretrial_period_end,
    agg.pretrial_period_end_count,
    agg.has_pretrial_violation,
    agg.pretrial_violation_count,
    agg.is_unmatched_booking,
    agg.unmatched_booking_count,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name, pre_assessment.risk_score, pre_assessment.risk_score_raw, pre_assessment.score_failure_to_appear, pre_assessment.score_new_criminal_activity, pre_assessment.score_new_criminal_violent_activity, pre_assessment.generic_tool_total_score, pre_assessment.generic_tool_total_score_raw,
    book_charge.principal_booking_offense_level as booking_charge_level,
    book_charge.principal_booking_offense_type as booking_charge_code,
    book_charge.principal_booking_charge as booking_charge,
    book_charge.principal_booking_charge_hierarchy_code as booking_charge_hierarchy,
    FIRST_VALUE(court_charge.principal_court_charge) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge,
    FIRST_VALUE(court_charge.principal_court_offense_level) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_level,
    FIRST_VALUE(court_charge.principal_court_offense_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_code,
    FIRST_VALUE(court_charge.principal_court_charge_hierarchy_code) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_hierarchy,
    MIN(court_case.dim_plea_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS plea_date_key, -- need plea date dimension to get actual date out
    MIN(court_case.dim_sentence_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS sentence_date_key, -- need sentence date dimension to get actual date out
    FIRST_VALUE(court_detail.plea_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
                case when plea_type = 'Guilty' then 1
                     when plea_type = 'Nolo Contendere' then 2
                     when plea_type = 'Not Guilty' then 3
                     else null end          
          ) AS plea_type,
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
        ) AS disposition_type,
    FIRST_VALUE(court_detail.sentence_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when sentence_type = 'Prison' then 1
                when sentence_type = 'Jail' then 2
                when sentence_type = 'Probation' then 3
                when sentence_type = 'Fine' then 4
                when sentence_type = 'No Sentence Information' then 5
         else null end
        ) AS sentence_type,

    -- pretrial monitoring flags
    pre_detail.monitoring_level,
    pre_detail.court_date_reminder_flag,
    pre_detail.transit_service_flag,
    pre_detail.other_pretrial_service,
    pre_detail.condition_phone_flag,
    pre_detail.condition_inperson_flag,
    pre_detail.condition_drug_test_flag,
    pre_detail.condition_alcohol_flag,
    pre_detail.condition_em_flag,
    pre_detail.condition_no_contact_flag,
    
    pre_detail.release_decision_prearraignment,
    pre_detail.release_decision_arraignment,
    pre_detail.pretrial_release_type, -- what is this compared to normal release type??
    pre_detail.pretrial_termination_outcome,
    pre_detail.pretrial_termination_reason,
    pre_termination_date.pretrial_termination_date,
    
    -- arrest charge flags
    book_flags.has_special_charge as arrest_special_flag,
    book_flags.has_serious_felony_charge as arrest_serious_flag,
    book_flags.has_violent_felony_charge as arrest_violent_flag,
    book_flags.has_violent_psa_charge as arrest_violent_psa_flag,
    book_flags.has_capital_charge as arrest_capital_flag,
    book_flags.has_dv_charge as arrest_dv_flag,
    book_flags.has_marijuana_charge as arrest_marijuana_flag,
    book_flags.has_sup_vio_charge as arrest_sup_vio_flag,
    book_flags.has_fta_charge as arrest_fta_flag,
    book_flags.has_sex_charge as arrest_sex_flag,
    book_flags.has_dui_charge as arrest_dui_flag,
    book_flags.has_restrain_charge as arrest_restrain_flag,
    book_flags.has_property_charge as arrest_property_flag,
    book_flags.has_drug_charge as arrest_drug_flag,
    book_flags.has_dv_possible_charge as arrest_dv_possible_flag,
    book_flags.booking_charge_group_level as arrest_charge_level, -- will need to convert into misdo and felony flags in R
    
    -- filing charge flags
    MAX(case when court_flags.has_special_charge = 'Yes' THEN 1 else 0 end) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_special_flag,
    MAX(case when court_flags.has_serious_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_serious_flag,
    MAX(case when court_flags.has_violent_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_flag,
    MAX(case when court_flags.has_violent_psa_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_psa_flag,
    MAX(case when court_flags.has_capital_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_capital_flag,
    MAX(case when court_flags.has_dv_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_flag,
    MAX(case when court_flags.has_marijuana_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_marijuana_flag,
    MAX(case when court_flags.has_sup_vio_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sup_vio_flag,
    MAX(case when court_flags.has_fta_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_fta_flag,
    MAX(case when court_flags.has_sex_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sex_flag,
    MAX(case when court_flags.has_dui_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dui_flag,
    MAX(case when court_flags.has_restrain_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_restrain_flag,
    MAX(case when court_flags.has_property_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_property_flag,
    MAX(case when court_flags.has_drug_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_drug_flag,
    MAX(case when court_flags.has_dv_possible_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_possible_flag,  
    -- conviction charge flags not currently possible
    arrest_date.arrest_date
    

from "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."FACT_BOOKING" as booking 
        on agg.booking_key = booking.booking_key
    left join "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_BOOKING_CHARGE" as book_charge 
        on booking.dim_principal_booking_charge_key = book_charge.dim_principal_booking_charge_key
    left join "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case 
        on agg.booking_key = court_case.booking_key
    left join "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_COURT_CHARGE" as court_charge 
        on court_case.dim_principal_court_charge_key = court_charge.dim_principal_court_charge_key
    left join "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
        on court_case.dim_court_detail_key = court_detail.dim_court_detail_key
    left join "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as pre_assessment
        on agg.pretrial_assessment_key = pre_assessment.pretrial_assessment_key
    left join "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."DIM_RELEASE_CONDITION" as release_cond
        on pre_assessment.dim_release_condition_key = release_cond.dim_release_condition_key
    left join "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as pre_detail
        on pre_assessment.dim_pretrial_detail_key = pre_detail.dim_pretrial_detail_key
    left join "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_TERMINATION_DATE" as pre_termination_date
        on pre_assessment.dim_pretrial_termination_date_key = pre_termination_date.dim_pretrial_termination_date_key
    left join "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."DIM_BOOKING_CHARGE_GROUP" as book_flags
        on booking.dim_booking_charge_group_key = book_flags.dim_booking_charge_group_key
    left join "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."DIM_COURT_CHARGE_GROUP" as court_flags
        on court_case.dim_court_charge_group_key = court_flags.dim_court_charge_group_key
    left join "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."DIM_ARREST_DATE" as arrest_date
        on booking.dim_arrest_date_key = arrest_date.dim_arrest_date_key
    
 

---------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------
UNION ALL


select
    distinct
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
    agg.is_release_eligible,
    agg.release_eligible_count,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.is_released_prearraignment,
    agg.released_prearraignment_count,
    agg.is_released_pretrial,
    agg.released_pretrial_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.has_new_criminal_activity,
    agg.new_criminal_activity_count,
    agg.has_pretrial_period_end,
    agg.pretrial_period_end_count,
    agg.has_pretrial_violation,
    agg.pretrial_violation_count,
    agg.is_unmatched_booking,
    agg.unmatched_booking_count,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name, pre_assessment.risk_score, pre_assessment.risk_score_raw, pre_assessment.score_failure_to_appear, pre_assessment.score_new_criminal_activity, pre_assessment.score_new_criminal_violent_activity, pre_assessment.generic_tool_total_score, pre_assessment.generic_tool_total_score_raw,
    book_charge.principal_booking_offense_level as booking_charge_level,
    book_charge.principal_booking_offense_type as booking_charge_code,
    book_charge.principal_booking_charge as booking_charge,
    book_charge.principal_booking_charge_hierarchy_code as booking_charge_hierarchy,
    FIRST_VALUE(court_charge.principal_court_charge) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge,
    FIRST_VALUE(court_charge.principal_court_offense_level) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_level,
    FIRST_VALUE(court_charge.principal_court_offense_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_code,
    FIRST_VALUE(court_charge.principal_court_charge_hierarchy_code) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_hierarchy,
    MIN(court_case.dim_plea_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS plea_date_key, -- need plea date dimension to get actual date out
    MIN(court_case.dim_sentence_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS sentence_date_key, -- need sentence date dimension to get actual date out
    FIRST_VALUE(court_detail.plea_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
                case when plea_type = 'Guilty' then 1
                     when plea_type = 'Nolo Contendere' then 2
                     when plea_type = 'Not Guilty' then 3
                     else null end          
          ) AS plea_type,
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
        ) AS disposition_type,
    FIRST_VALUE(court_detail.sentence_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when sentence_type = 'Prison' then 1
                when sentence_type = 'Jail' then 2
                when sentence_type = 'Probation' then 3
                when sentence_type = 'Fine' then 4
                when sentence_type = 'No Sentence Information' then 5
         else null end
        ) AS sentence_type,

    -- pretrial monitoring flags
    pre_detail.monitoring_level,
    pre_detail.court_date_reminder_flag,
    pre_detail.transit_service_flag,
    pre_detail.other_pretrial_service,
    pre_detail.condition_phone_flag,
    pre_detail.condition_inperson_flag,
    pre_detail.condition_drug_test_flag,
    pre_detail.condition_alcohol_flag,
    pre_detail.condition_em_flag,
    pre_detail.condition_no_contact_flag,
    
    pre_detail.release_decision_prearraignment,
    pre_detail.release_decision_arraignment,
    pre_detail.pretrial_release_type, -- what is this compared to normal release type??
    pre_detail.pretrial_termination_outcome,
    pre_detail.pretrial_termination_reason,
    pre_termination_date.pretrial_termination_date,
    
    -- arrest charge flags
    book_flags.has_special_charge as arrest_special_flag,
    book_flags.has_serious_felony_charge as arrest_serious_flag,
    book_flags.has_violent_felony_charge as arrest_violent_flag,
    book_flags.has_violent_psa_charge as arrest_violent_psa_flag,
    book_flags.has_capital_charge as arrest_capital_flag,
    book_flags.has_dv_charge as arrest_dv_flag,
    book_flags.has_marijuana_charge as arrest_marijuana_flag,
    book_flags.has_sup_vio_charge as arrest_sup_vio_flag,
    book_flags.has_fta_charge as arrest_fta_flag,
    book_flags.has_sex_charge as arrest_sex_flag,
    book_flags.has_dui_charge as arrest_dui_flag,
    book_flags.has_restrain_charge as arrest_restrain_flag,
    book_flags.has_property_charge as arrest_property_flag,
    book_flags.has_drug_charge as arrest_drug_flag,
    book_flags.has_dv_possible_charge as arrest_dv_possible_flag,
    book_flags.booking_charge_group_level as arrest_charge_level, -- will need to convert into misdo and felony flags in R
    
    -- filing charge flags
    MAX(case when court_flags.has_special_charge = 'Yes' THEN 1 else 0 end) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_special_flag,
    MAX(case when court_flags.has_serious_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_serious_flag,
    MAX(case when court_flags.has_violent_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_flag,
    MAX(case when court_flags.has_violent_psa_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_psa_flag,
    MAX(case when court_flags.has_capital_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_capital_flag,
    MAX(case when court_flags.has_dv_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_flag,
    MAX(case when court_flags.has_marijuana_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_marijuana_flag,
    MAX(case when court_flags.has_sup_vio_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sup_vio_flag,
    MAX(case when court_flags.has_fta_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_fta_flag,
    MAX(case when court_flags.has_sex_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sex_flag,
    MAX(case when court_flags.has_dui_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dui_flag,
    MAX(case when court_flags.has_restrain_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_restrain_flag,
    MAX(case when court_flags.has_property_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_property_flag,
    MAX(case when court_flags.has_drug_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_drug_flag,
    MAX(case when court_flags.has_dv_possible_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_possible_flag,  
    -- conviction charge flags not currently possible
    arrest_date.arrest_date
    

from "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."FACT_BOOKING" as booking 
        on agg.booking_key = booking.booking_key
    left join "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_BOOKING_CHARGE" as book_charge 
        on booking.dim_principal_booking_charge_key = book_charge.dim_principal_booking_charge_key
    left join "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case 
        on agg.booking_key = court_case.booking_key
    left join "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_COURT_CHARGE" as court_charge 
        on court_case.dim_principal_court_charge_key = court_charge.dim_principal_court_charge_key
    left join "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
        on court_case.dim_court_detail_key = court_detail.dim_court_detail_key
    left join "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as pre_assessment
        on agg.pretrial_assessment_key = pre_assessment.pretrial_assessment_key
    left join "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."DIM_RELEASE_CONDITION" as release_cond
        on pre_assessment.dim_release_condition_key = release_cond.dim_release_condition_key
    left join "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as pre_detail
        on pre_assessment.dim_pretrial_detail_key = pre_detail.dim_pretrial_detail_key
    left join "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_TERMINATION_DATE" as pre_termination_date
        on pre_assessment.dim_pretrial_termination_date_key = pre_termination_date.dim_pretrial_termination_date_key
    left join "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."DIM_BOOKING_CHARGE_GROUP" as book_flags
        on booking.dim_booking_charge_group_key = book_flags.dim_booking_charge_group_key
    left join "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."DIM_COURT_CHARGE_GROUP" as court_flags
        on court_case.dim_court_charge_group_key = court_flags.dim_court_charge_group_key
    left join "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."DIM_ARREST_DATE" as arrest_date
        on booking.dim_arrest_date_key = arrest_date.dim_arrest_date_key
    
 

---------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------
UNION ALL


select
    distinct
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
    agg.is_release_eligible,
    agg.release_eligible_count,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.is_released_prearraignment,
    agg.released_prearraignment_count,
    agg.is_released_pretrial,
    agg.released_pretrial_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.has_new_criminal_activity,
    agg.new_criminal_activity_count,
    agg.has_pretrial_period_end,
    agg.pretrial_period_end_count,
    agg.has_pretrial_violation,
    agg.pretrial_violation_count,
    agg.is_unmatched_booking,
    agg.unmatched_booking_count,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name, pre_assessment.risk_score, pre_assessment.risk_score_raw, pre_assessment.score_failure_to_appear, pre_assessment.score_new_criminal_activity, pre_assessment.score_new_criminal_violent_activity, pre_assessment.generic_tool_total_score, pre_assessment.generic_tool_total_score_raw,
    book_charge.principal_booking_offense_level as booking_charge_level,
    book_charge.principal_booking_offense_type as booking_charge_code,
    book_charge.principal_booking_charge as booking_charge,
    book_charge.principal_booking_charge_hierarchy_code as booking_charge_hierarchy,
    FIRST_VALUE(court_charge.principal_court_charge) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge,
    FIRST_VALUE(court_charge.principal_court_offense_level) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_level,
    FIRST_VALUE(court_charge.principal_court_offense_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_code,
    FIRST_VALUE(court_charge.principal_court_charge_hierarchy_code) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_hierarchy,
    MIN(court_case.dim_plea_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS plea_date_key, -- need plea date dimension to get actual date out
    MIN(court_case.dim_sentence_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS sentence_date_key, -- need sentence date dimension to get actual date out
    FIRST_VALUE(court_detail.plea_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
                case when plea_type = 'Guilty' then 1
                     when plea_type = 'Nolo Contendere' then 2
                     when plea_type = 'Not Guilty' then 3
                     else null end          
          ) AS plea_type,
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
        ) AS disposition_type,
    FIRST_VALUE(court_detail.sentence_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when sentence_type = 'Prison' then 1
                when sentence_type = 'Jail' then 2
                when sentence_type = 'Probation' then 3
                when sentence_type = 'Fine' then 4
                when sentence_type = 'No Sentence Information' then 5
         else null end
        ) AS sentence_type,

    -- pretrial monitoring flags
    pre_detail.monitoring_level,
    pre_detail.court_date_reminder_flag,
    pre_detail.transit_service_flag,
    pre_detail.other_pretrial_service,
    pre_detail.condition_phone_flag,
    pre_detail.condition_inperson_flag,
    pre_detail.condition_drug_test_flag,
    pre_detail.condition_alcohol_flag,
    pre_detail.condition_em_flag,
    pre_detail.condition_no_contact_flag,
    
    pre_detail.release_decision_prearraignment,
    pre_detail.release_decision_arraignment,
    pre_detail.pretrial_release_type, -- what is this compared to normal release type??
    pre_detail.pretrial_termination_outcome,
    pre_detail.pretrial_termination_reason,
    pre_termination_date.pretrial_termination_date,
    
    -- arrest charge flags
    book_flags.has_special_charge as arrest_special_flag,
    book_flags.has_serious_felony_charge as arrest_serious_flag,
    book_flags.has_violent_felony_charge as arrest_violent_flag,
    book_flags.has_violent_psa_charge as arrest_violent_psa_flag,
    book_flags.has_capital_charge as arrest_capital_flag,
    book_flags.has_dv_charge as arrest_dv_flag,
    book_flags.has_marijuana_charge as arrest_marijuana_flag,
    book_flags.has_sup_vio_charge as arrest_sup_vio_flag,
    book_flags.has_fta_charge as arrest_fta_flag,
    book_flags.has_sex_charge as arrest_sex_flag,
    book_flags.has_dui_charge as arrest_dui_flag,
    book_flags.has_restrain_charge as arrest_restrain_flag,
    book_flags.has_property_charge as arrest_property_flag,
    book_flags.has_drug_charge as arrest_drug_flag,
    book_flags.has_dv_possible_charge as arrest_dv_possible_flag,
    book_flags.booking_charge_group_level as arrest_charge_level, -- will need to convert into misdo and felony flags in R
    
    -- filing charge flags
    MAX(case when court_flags.has_special_charge = 'Yes' THEN 1 else 0 end) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_special_flag,
    MAX(case when court_flags.has_serious_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_serious_flag,
    MAX(case when court_flags.has_violent_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_flag,
    MAX(case when court_flags.has_violent_psa_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_psa_flag,
    MAX(case when court_flags.has_capital_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_capital_flag,
    MAX(case when court_flags.has_dv_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_flag,
    MAX(case when court_flags.has_marijuana_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_marijuana_flag,
    MAX(case when court_flags.has_sup_vio_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sup_vio_flag,
    MAX(case when court_flags.has_fta_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_fta_flag,
    MAX(case when court_flags.has_sex_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sex_flag,
    MAX(case when court_flags.has_dui_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dui_flag,
    MAX(case when court_flags.has_restrain_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_restrain_flag,
    MAX(case when court_flags.has_property_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_property_flag,
    MAX(case when court_flags.has_drug_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_drug_flag,
    MAX(case when court_flags.has_dv_possible_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_possible_flag,  
    -- conviction charge flags not currently possible
    arrest_date.arrest_date
    

from "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."FACT_BOOKING" as booking 
        on agg.booking_key = booking.booking_key
    left join "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_BOOKING_CHARGE" as book_charge 
        on booking.dim_principal_booking_charge_key = book_charge.dim_principal_booking_charge_key
    left join "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case 
        on agg.booking_key = court_case.booking_key
    left join "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_COURT_CHARGE" as court_charge 
        on court_case.dim_principal_court_charge_key = court_charge.dim_principal_court_charge_key
    left join "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
        on court_case.dim_court_detail_key = court_detail.dim_court_detail_key
    left join "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as pre_assessment
        on agg.pretrial_assessment_key = pre_assessment.pretrial_assessment_key
    left join "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."DIM_RELEASE_CONDITION" as release_cond
        on pre_assessment.dim_release_condition_key = release_cond.dim_release_condition_key
    left join "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as pre_detail
        on pre_assessment.dim_pretrial_detail_key = pre_detail.dim_pretrial_detail_key
    left join "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_TERMINATION_DATE" as pre_termination_date
        on pre_assessment.dim_pretrial_termination_date_key = pre_termination_date.dim_pretrial_termination_date_key
    left join "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."DIM_BOOKING_CHARGE_GROUP" as book_flags
        on booking.dim_booking_charge_group_key = book_flags.dim_booking_charge_group_key
    left join "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."DIM_COURT_CHARGE_GROUP" as court_flags
        on court_case.dim_court_charge_group_key = court_flags.dim_court_charge_group_key
    left join "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."DIM_ARREST_DATE" as arrest_date
        on booking.dim_arrest_date_key = arrest_date.dim_arrest_date_key
    
 

---------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------
UNION ALL


select
    distinct
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
    agg.is_release_eligible,
    agg.release_eligible_count,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.is_released_prearraignment,
    agg.released_prearraignment_count,
    agg.is_released_pretrial,
    agg.released_pretrial_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.has_new_criminal_activity,
    agg.new_criminal_activity_count,
    agg.has_pretrial_period_end,
    agg.pretrial_period_end_count,
    agg.has_pretrial_violation,
    agg.pretrial_violation_count,
    agg.is_unmatched_booking,
    agg.unmatched_booking_count,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name, pre_assessment.risk_score, pre_assessment.risk_score_raw, pre_assessment.score_failure_to_appear, pre_assessment.score_new_criminal_activity, pre_assessment.score_new_criminal_violent_activity, pre_assessment.generic_tool_total_score, pre_assessment.generic_tool_total_score_raw,
    book_charge.principal_booking_offense_level as booking_charge_level,
    book_charge.principal_booking_offense_type as booking_charge_code,
    book_charge.principal_booking_charge as booking_charge,
    book_charge.principal_booking_charge_hierarchy_code as booking_charge_hierarchy,
    FIRST_VALUE(court_charge.principal_court_charge) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge,
    FIRST_VALUE(court_charge.principal_court_offense_level) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_level,
    FIRST_VALUE(court_charge.principal_court_offense_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_code,
    FIRST_VALUE(court_charge.principal_court_charge_hierarchy_code) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_hierarchy,
    MIN(court_case.dim_plea_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS plea_date_key, -- need plea date dimension to get actual date out
    MIN(court_case.dim_sentence_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS sentence_date_key, -- need sentence date dimension to get actual date out
    FIRST_VALUE(court_detail.plea_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
                case when plea_type = 'Guilty' then 1
                     when plea_type = 'Nolo Contendere' then 2
                     when plea_type = 'Not Guilty' then 3
                     else null end          
          ) AS plea_type,
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
        ) AS disposition_type,
    FIRST_VALUE(court_detail.sentence_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when sentence_type = 'Prison' then 1
                when sentence_type = 'Jail' then 2
                when sentence_type = 'Probation' then 3
                when sentence_type = 'Fine' then 4
                when sentence_type = 'No Sentence Information' then 5
         else null end
        ) AS sentence_type,

    -- pretrial monitoring flags
    pre_detail.monitoring_level,
    pre_detail.court_date_reminder_flag,
    pre_detail.transit_service_flag,
    pre_detail.other_pretrial_service,
    pre_detail.condition_phone_flag,
    pre_detail.condition_inperson_flag,
    pre_detail.condition_drug_test_flag,
    pre_detail.condition_alcohol_flag,
    pre_detail.condition_em_flag,
    pre_detail.condition_no_contact_flag,
    
    pre_detail.release_decision_prearraignment,
    pre_detail.release_decision_arraignment,
    pre_detail.pretrial_release_type, -- what is this compared to normal release type??
    pre_detail.pretrial_termination_outcome,
    pre_detail.pretrial_termination_reason,
    pre_termination_date.pretrial_termination_date,
    
    -- arrest charge flags
    book_flags.has_special_charge as arrest_special_flag,
    book_flags.has_serious_felony_charge as arrest_serious_flag,
    book_flags.has_violent_felony_charge as arrest_violent_flag,
    book_flags.has_violent_psa_charge as arrest_violent_psa_flag,
    book_flags.has_capital_charge as arrest_capital_flag,
    book_flags.has_dv_charge as arrest_dv_flag,
    book_flags.has_marijuana_charge as arrest_marijuana_flag,
    book_flags.has_sup_vio_charge as arrest_sup_vio_flag,
    book_flags.has_fta_charge as arrest_fta_flag,
    book_flags.has_sex_charge as arrest_sex_flag,
    book_flags.has_dui_charge as arrest_dui_flag,
    book_flags.has_restrain_charge as arrest_restrain_flag,
    book_flags.has_property_charge as arrest_property_flag,
    book_flags.has_drug_charge as arrest_drug_flag,
    book_flags.has_dv_possible_charge as arrest_dv_possible_flag,
    book_flags.booking_charge_group_level as arrest_charge_level, -- will need to convert into misdo and felony flags in R
    
    -- filing charge flags
    MAX(case when court_flags.has_special_charge = 'Yes' THEN 1 else 0 end) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_special_flag,
    MAX(case when court_flags.has_serious_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_serious_flag,
    MAX(case when court_flags.has_violent_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_flag,
    MAX(case when court_flags.has_violent_psa_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_psa_flag,
    MAX(case when court_flags.has_capital_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_capital_flag,
    MAX(case when court_flags.has_dv_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_flag,
    MAX(case when court_flags.has_marijuana_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_marijuana_flag,
    MAX(case when court_flags.has_sup_vio_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sup_vio_flag,
    MAX(case when court_flags.has_fta_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_fta_flag,
    MAX(case when court_flags.has_sex_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sex_flag,
    MAX(case when court_flags.has_dui_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dui_flag,
    MAX(case when court_flags.has_restrain_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_restrain_flag,
    MAX(case when court_flags.has_property_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_property_flag,
    MAX(case when court_flags.has_drug_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_drug_flag,
    MAX(case when court_flags.has_dv_possible_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_possible_flag,  
    -- conviction charge flags not currently possible
    arrest_date.arrest_date
    

from "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."FACT_BOOKING" as booking 
        on agg.booking_key = booking.booking_key
    left join "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_BOOKING_CHARGE" as book_charge 
        on booking.dim_principal_booking_charge_key = book_charge.dim_principal_booking_charge_key
    left join "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case 
        on agg.booking_key = court_case.booking_key
    left join "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_COURT_CHARGE" as court_charge 
        on court_case.dim_principal_court_charge_key = court_charge.dim_principal_court_charge_key
    left join "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
        on court_case.dim_court_detail_key = court_detail.dim_court_detail_key
    left join "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as pre_assessment
        on agg.pretrial_assessment_key = pre_assessment.pretrial_assessment_key
    left join "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."DIM_RELEASE_CONDITION" as release_cond
        on pre_assessment.dim_release_condition_key = release_cond.dim_release_condition_key
    left join "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as pre_detail
        on pre_assessment.dim_pretrial_detail_key = pre_detail.dim_pretrial_detail_key
    left join "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_TERMINATION_DATE" as pre_termination_date
        on pre_assessment.dim_pretrial_termination_date_key = pre_termination_date.dim_pretrial_termination_date_key
    left join "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."DIM_BOOKING_CHARGE_GROUP" as book_flags
        on booking.dim_booking_charge_group_key = book_flags.dim_booking_charge_group_key
    left join "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."DIM_COURT_CHARGE_GROUP" as court_flags
        on court_case.dim_court_charge_group_key = court_flags.dim_court_charge_group_key
    left join "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."DIM_ARREST_DATE" as arrest_date
        on booking.dim_arrest_date_key = arrest_date.dim_arrest_date_key
    
 

---------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------
UNION ALL


select
    distinct
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
    agg.is_release_eligible,
    agg.release_eligible_count,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.is_released_prearraignment,
    agg.released_prearraignment_count,
    agg.is_released_pretrial,
    agg.released_pretrial_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.has_new_criminal_activity,
    agg.new_criminal_activity_count,
    agg.has_pretrial_period_end,
    agg.pretrial_period_end_count,
    agg.has_pretrial_violation,
    agg.pretrial_violation_count,
    agg.is_unmatched_booking,
    agg.unmatched_booking_count,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name, pre_assessment.risk_score, pre_assessment.risk_score_raw, pre_assessment.score_failure_to_appear, pre_assessment.score_new_criminal_activity, pre_assessment.score_new_criminal_violent_activity, pre_assessment.generic_tool_total_score, pre_assessment.generic_tool_total_score_raw,
    book_charge.principal_booking_offense_level as booking_charge_level,
    book_charge.principal_booking_offense_type as booking_charge_code,
    book_charge.principal_booking_charge as booking_charge,
    book_charge.principal_booking_charge_hierarchy_code as booking_charge_hierarchy,
    FIRST_VALUE(court_charge.principal_court_charge) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge,
    FIRST_VALUE(court_charge.principal_court_offense_level) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_level,
    FIRST_VALUE(court_charge.principal_court_offense_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_code,
    FIRST_VALUE(court_charge.principal_court_charge_hierarchy_code) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_hierarchy,
    MIN(court_case.dim_plea_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS plea_date_key, -- need plea date dimension to get actual date out
    MIN(court_case.dim_sentence_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS sentence_date_key, -- need sentence date dimension to get actual date out
    FIRST_VALUE(court_detail.plea_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
                case when plea_type = 'Guilty' then 1
                     when plea_type = 'Nolo Contendere' then 2
                     when plea_type = 'Not Guilty' then 3
                     else null end          
          ) AS plea_type,
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
        ) AS disposition_type,
    FIRST_VALUE(court_detail.sentence_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when sentence_type = 'Prison' then 1
                when sentence_type = 'Jail' then 2
                when sentence_type = 'Probation' then 3
                when sentence_type = 'Fine' then 4
                when sentence_type = 'No Sentence Information' then 5
         else null end
        ) AS sentence_type,

    -- pretrial monitoring flags
    pre_detail.monitoring_level,
    pre_detail.court_date_reminder_flag,
    pre_detail.transit_service_flag,
    pre_detail.other_pretrial_service,
    pre_detail.condition_phone_flag,
    pre_detail.condition_inperson_flag,
    pre_detail.condition_drug_test_flag,
    pre_detail.condition_alcohol_flag,
    pre_detail.condition_em_flag,
    pre_detail.condition_no_contact_flag,
    
    pre_detail.release_decision_prearraignment,
    pre_detail.release_decision_arraignment,
    pre_detail.pretrial_release_type, -- what is this compared to normal release type??
    pre_detail.pretrial_termination_outcome,
    pre_detail.pretrial_termination_reason,
    pre_termination_date.pretrial_termination_date,
    
    -- arrest charge flags
    book_flags.has_special_charge as arrest_special_flag,
    book_flags.has_serious_felony_charge as arrest_serious_flag,
    book_flags.has_violent_felony_charge as arrest_violent_flag,
    book_flags.has_violent_psa_charge as arrest_violent_psa_flag,
    book_flags.has_capital_charge as arrest_capital_flag,
    book_flags.has_dv_charge as arrest_dv_flag,
    book_flags.has_marijuana_charge as arrest_marijuana_flag,
    book_flags.has_sup_vio_charge as arrest_sup_vio_flag,
    book_flags.has_fta_charge as arrest_fta_flag,
    book_flags.has_sex_charge as arrest_sex_flag,
    book_flags.has_dui_charge as arrest_dui_flag,
    book_flags.has_restrain_charge as arrest_restrain_flag,
    book_flags.has_property_charge as arrest_property_flag,
    book_flags.has_drug_charge as arrest_drug_flag,
    book_flags.has_dv_possible_charge as arrest_dv_possible_flag,
    book_flags.booking_charge_group_level as arrest_charge_level, -- will need to convert into misdo and felony flags in R
    
    -- filing charge flags
    MAX(case when court_flags.has_special_charge = 'Yes' THEN 1 else 0 end) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_special_flag,
    MAX(case when court_flags.has_serious_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_serious_flag,
    MAX(case when court_flags.has_violent_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_flag,
    MAX(case when court_flags.has_violent_psa_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_psa_flag,
    MAX(case when court_flags.has_capital_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_capital_flag,
    MAX(case when court_flags.has_dv_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_flag,
    MAX(case when court_flags.has_marijuana_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_marijuana_flag,
    MAX(case when court_flags.has_sup_vio_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sup_vio_flag,
    MAX(case when court_flags.has_fta_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_fta_flag,
    MAX(case when court_flags.has_sex_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sex_flag,
    MAX(case when court_flags.has_dui_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dui_flag,
    MAX(case when court_flags.has_restrain_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_restrain_flag,
    MAX(case when court_flags.has_property_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_property_flag,
    MAX(case when court_flags.has_drug_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_drug_flag,
    MAX(case when court_flags.has_dv_possible_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_possible_flag,  
    -- conviction charge flags not currently possible
    arrest_date.arrest_date
    

from "46_SIERRA-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "46_SIERRA-DWH_PROD"."SECURE_SHARE"."FACT_BOOKING" as booking 
        on agg.booking_key = booking.booking_key
    left join "46_SIERRA-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_BOOKING_CHARGE" as book_charge 
        on booking.dim_principal_booking_charge_key = book_charge.dim_principal_booking_charge_key
    left join "46_SIERRA-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case 
        on agg.booking_key = court_case.booking_key
    left join "46_SIERRA-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_COURT_CHARGE" as court_charge 
        on court_case.dim_principal_court_charge_key = court_charge.dim_principal_court_charge_key
    left join "46_SIERRA-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
        on court_case.dim_court_detail_key = court_detail.dim_court_detail_key
    left join "46_SIERRA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as pre_assessment
        on agg.pretrial_assessment_key = pre_assessment.pretrial_assessment_key
    left join "46_SIERRA-DWH_PROD"."SECURE_SHARE"."DIM_RELEASE_CONDITION" as release_cond
        on pre_assessment.dim_release_condition_key = release_cond.dim_release_condition_key
    left join "46_SIERRA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as pre_detail
        on pre_assessment.dim_pretrial_detail_key = pre_detail.dim_pretrial_detail_key
    left join "46_SIERRA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_TERMINATION_DATE" as pre_termination_date
        on pre_assessment.dim_pretrial_termination_date_key = pre_termination_date.dim_pretrial_termination_date_key
    left join "46_SIERRA-DWH_PROD"."SECURE_SHARE"."DIM_BOOKING_CHARGE_GROUP" as book_flags
        on booking.dim_booking_charge_group_key = book_flags.dim_booking_charge_group_key
    left join "46_SIERRA-DWH_PROD"."SECURE_SHARE"."DIM_COURT_CHARGE_GROUP" as court_flags
        on court_case.dim_court_charge_group_key = court_flags.dim_court_charge_group_key
    left join "46_SIERRA-DWH_PROD"."SECURE_SHARE"."DIM_ARREST_DATE" as arrest_date
        on booking.dim_arrest_date_key = arrest_date.dim_arrest_date_key
    
 

---------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------
UNION ALL


select
    distinct
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
    agg.is_release_eligible,
    agg.release_eligible_count,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.is_released_prearraignment,
    agg.released_prearraignment_count,
    agg.is_released_pretrial,
    agg.released_pretrial_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.has_new_criminal_activity,
    agg.new_criminal_activity_count,
    agg.has_pretrial_period_end,
    agg.pretrial_period_end_count,
    agg.has_pretrial_violation,
    agg.pretrial_violation_count,
    agg.is_unmatched_booking,
    agg.unmatched_booking_count,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name, pre_assessment.risk_score, pre_assessment.risk_score_raw, pre_assessment.score_failure_to_appear, pre_assessment.score_new_criminal_activity, pre_assessment.score_new_criminal_violent_activity, pre_assessment.generic_tool_total_score, pre_assessment.generic_tool_total_score_raw,
    book_charge.principal_booking_offense_level as booking_charge_level,
    book_charge.principal_booking_offense_type as booking_charge_code,
    book_charge.principal_booking_charge as booking_charge,
    book_charge.principal_booking_charge_hierarchy_code as booking_charge_hierarchy,
    FIRST_VALUE(court_charge.principal_court_charge) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge,
    FIRST_VALUE(court_charge.principal_court_offense_level) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_level,
    FIRST_VALUE(court_charge.principal_court_offense_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_code,
    FIRST_VALUE(court_charge.principal_court_charge_hierarchy_code) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_hierarchy,
    MIN(court_case.dim_plea_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS plea_date_key, -- need plea date dimension to get actual date out
    MIN(court_case.dim_sentence_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS sentence_date_key, -- need sentence date dimension to get actual date out
    FIRST_VALUE(court_detail.plea_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
                case when plea_type = 'Guilty' then 1
                     when plea_type = 'Nolo Contendere' then 2
                     when plea_type = 'Not Guilty' then 3
                     else null end          
          ) AS plea_type,
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
        ) AS disposition_type,
    FIRST_VALUE(court_detail.sentence_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when sentence_type = 'Prison' then 1
                when sentence_type = 'Jail' then 2
                when sentence_type = 'Probation' then 3
                when sentence_type = 'Fine' then 4
                when sentence_type = 'No Sentence Information' then 5
         else null end
        ) AS sentence_type,

    -- pretrial monitoring flags
    pre_detail.monitoring_level,
    pre_detail.court_date_reminder_flag,
    pre_detail.transit_service_flag,
    pre_detail.other_pretrial_service,
    pre_detail.condition_phone_flag,
    pre_detail.condition_inperson_flag,
    pre_detail.condition_drug_test_flag,
    pre_detail.condition_alcohol_flag,
    pre_detail.condition_em_flag,
    pre_detail.condition_no_contact_flag,
    
    pre_detail.release_decision_prearraignment,
    pre_detail.release_decision_arraignment,
    pre_detail.pretrial_release_type, -- what is this compared to normal release type??
    pre_detail.pretrial_termination_outcome,
    pre_detail.pretrial_termination_reason,
    pre_termination_date.pretrial_termination_date,
    
    -- arrest charge flags
    book_flags.has_special_charge as arrest_special_flag,
    book_flags.has_serious_felony_charge as arrest_serious_flag,
    book_flags.has_violent_felony_charge as arrest_violent_flag,
    book_flags.has_violent_psa_charge as arrest_violent_psa_flag,
    book_flags.has_capital_charge as arrest_capital_flag,
    book_flags.has_dv_charge as arrest_dv_flag,
    book_flags.has_marijuana_charge as arrest_marijuana_flag,
    book_flags.has_sup_vio_charge as arrest_sup_vio_flag,
    book_flags.has_fta_charge as arrest_fta_flag,
    book_flags.has_sex_charge as arrest_sex_flag,
    book_flags.has_dui_charge as arrest_dui_flag,
    book_flags.has_restrain_charge as arrest_restrain_flag,
    book_flags.has_property_charge as arrest_property_flag,
    book_flags.has_drug_charge as arrest_drug_flag,
    book_flags.has_dv_possible_charge as arrest_dv_possible_flag,
    book_flags.booking_charge_group_level as arrest_charge_level, -- will need to convert into misdo and felony flags in R
    
    -- filing charge flags
    MAX(case when court_flags.has_special_charge = 'Yes' THEN 1 else 0 end) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_special_flag,
    MAX(case when court_flags.has_serious_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_serious_flag,
    MAX(case when court_flags.has_violent_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_flag,
    MAX(case when court_flags.has_violent_psa_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_psa_flag,
    MAX(case when court_flags.has_capital_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_capital_flag,
    MAX(case when court_flags.has_dv_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_flag,
    MAX(case when court_flags.has_marijuana_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_marijuana_flag,
    MAX(case when court_flags.has_sup_vio_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sup_vio_flag,
    MAX(case when court_flags.has_fta_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_fta_flag,
    MAX(case when court_flags.has_sex_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sex_flag,
    MAX(case when court_flags.has_dui_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dui_flag,
    MAX(case when court_flags.has_restrain_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_restrain_flag,
    MAX(case when court_flags.has_property_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_property_flag,
    MAX(case when court_flags.has_drug_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_drug_flag,
    MAX(case when court_flags.has_dv_possible_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_possible_flag,  
    -- conviction charge flags not currently possible
    arrest_date.arrest_date
    

from "49_SONOMA-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "49_SONOMA-DWH_PROD"."SECURE_SHARE"."FACT_BOOKING" as booking 
        on agg.booking_key = booking.booking_key
    left join "49_SONOMA-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_BOOKING_CHARGE" as book_charge 
        on booking.dim_principal_booking_charge_key = book_charge.dim_principal_booking_charge_key
    left join "49_SONOMA-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case 
        on agg.booking_key = court_case.booking_key
    left join "49_SONOMA-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_COURT_CHARGE" as court_charge 
        on court_case.dim_principal_court_charge_key = court_charge.dim_principal_court_charge_key
    left join "49_SONOMA-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
        on court_case.dim_court_detail_key = court_detail.dim_court_detail_key
    left join "49_SONOMA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as pre_assessment
        on agg.pretrial_assessment_key = pre_assessment.pretrial_assessment_key
    left join "49_SONOMA-DWH_PROD"."SECURE_SHARE"."DIM_RELEASE_CONDITION" as release_cond
        on pre_assessment.dim_release_condition_key = release_cond.dim_release_condition_key
    left join "49_SONOMA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as pre_detail
        on pre_assessment.dim_pretrial_detail_key = pre_detail.dim_pretrial_detail_key
    left join "49_SONOMA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_TERMINATION_DATE" as pre_termination_date
        on pre_assessment.dim_pretrial_termination_date_key = pre_termination_date.dim_pretrial_termination_date_key
    left join "49_SONOMA-DWH_PROD"."SECURE_SHARE"."DIM_BOOKING_CHARGE_GROUP" as book_flags
        on booking.dim_booking_charge_group_key = book_flags.dim_booking_charge_group_key
    left join "49_SONOMA-DWH_PROD"."SECURE_SHARE"."DIM_COURT_CHARGE_GROUP" as court_flags
        on court_case.dim_court_charge_group_key = court_flags.dim_court_charge_group_key
    left join "49_SONOMA-DWH_PROD"."SECURE_SHARE"."DIM_ARREST_DATE" as arrest_date
        on booking.dim_arrest_date_key = arrest_date.dim_arrest_date_key
    
 

---------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------
UNION ALL


select
    distinct
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
    agg.is_release_eligible,
    agg.release_eligible_count,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.is_released_prearraignment,
    agg.released_prearraignment_count,
    agg.is_released_pretrial,
    agg.released_pretrial_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.has_new_criminal_activity,
    agg.new_criminal_activity_count,
    agg.has_pretrial_period_end,
    agg.pretrial_period_end_count,
    agg.has_pretrial_violation,
    agg.pretrial_violation_count,
    agg.is_unmatched_booking,
    agg.unmatched_booking_count,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name, pre_assessment.risk_score, pre_assessment.risk_score_raw, pre_assessment.score_failure_to_appear, pre_assessment.score_new_criminal_activity, pre_assessment.score_new_criminal_violent_activity, pre_assessment.generic_tool_total_score, pre_assessment.generic_tool_total_score_raw,
    book_charge.principal_booking_offense_level as booking_charge_level,
    book_charge.principal_booking_offense_type as booking_charge_code,
    book_charge.principal_booking_charge as booking_charge,
    book_charge.principal_booking_charge_hierarchy_code as booking_charge_hierarchy,
    FIRST_VALUE(court_charge.principal_court_charge) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge,
    FIRST_VALUE(court_charge.principal_court_offense_level) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_level,
    FIRST_VALUE(court_charge.principal_court_offense_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_code,
    FIRST_VALUE(court_charge.principal_court_charge_hierarchy_code) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_hierarchy,
    MIN(court_case.dim_plea_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS plea_date_key, -- need plea date dimension to get actual date out
    MIN(court_case.dim_sentence_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS sentence_date_key, -- need sentence date dimension to get actual date out
    FIRST_VALUE(court_detail.plea_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
                case when plea_type = 'Guilty' then 1
                     when plea_type = 'Nolo Contendere' then 2
                     when plea_type = 'Not Guilty' then 3
                     else null end          
          ) AS plea_type,
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
        ) AS disposition_type,
    FIRST_VALUE(court_detail.sentence_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when sentence_type = 'Prison' then 1
                when sentence_type = 'Jail' then 2
                when sentence_type = 'Probation' then 3
                when sentence_type = 'Fine' then 4
                when sentence_type = 'No Sentence Information' then 5
         else null end
        ) AS sentence_type,

    -- pretrial monitoring flags
    pre_detail.monitoring_level,
    pre_detail.court_date_reminder_flag,
    pre_detail.transit_service_flag,
    pre_detail.other_pretrial_service,
    pre_detail.condition_phone_flag,
    pre_detail.condition_inperson_flag,
    pre_detail.condition_drug_test_flag,
    pre_detail.condition_alcohol_flag,
    pre_detail.condition_em_flag,
    pre_detail.condition_no_contact_flag,
    
    pre_detail.release_decision_prearraignment,
    pre_detail.release_decision_arraignment,
    pre_detail.pretrial_release_type, -- what is this compared to normal release type??
    pre_detail.pretrial_termination_outcome,
    pre_detail.pretrial_termination_reason,
    pre_termination_date.pretrial_termination_date,
    
    -- arrest charge flags
    book_flags.has_special_charge as arrest_special_flag,
    book_flags.has_serious_felony_charge as arrest_serious_flag,
    book_flags.has_violent_felony_charge as arrest_violent_flag,
    book_flags.has_violent_psa_charge as arrest_violent_psa_flag,
    book_flags.has_capital_charge as arrest_capital_flag,
    book_flags.has_dv_charge as arrest_dv_flag,
    book_flags.has_marijuana_charge as arrest_marijuana_flag,
    book_flags.has_sup_vio_charge as arrest_sup_vio_flag,
    book_flags.has_fta_charge as arrest_fta_flag,
    book_flags.has_sex_charge as arrest_sex_flag,
    book_flags.has_dui_charge as arrest_dui_flag,
    book_flags.has_restrain_charge as arrest_restrain_flag,
    book_flags.has_property_charge as arrest_property_flag,
    book_flags.has_drug_charge as arrest_drug_flag,
    book_flags.has_dv_possible_charge as arrest_dv_possible_flag,
    book_flags.booking_charge_group_level as arrest_charge_level, -- will need to convert into misdo and felony flags in R
    
    -- filing charge flags
    MAX(case when court_flags.has_special_charge = 'Yes' THEN 1 else 0 end) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_special_flag,
    MAX(case when court_flags.has_serious_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_serious_flag,
    MAX(case when court_flags.has_violent_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_flag,
    MAX(case when court_flags.has_violent_psa_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_psa_flag,
    MAX(case when court_flags.has_capital_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_capital_flag,
    MAX(case when court_flags.has_dv_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_flag,
    MAX(case when court_flags.has_marijuana_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_marijuana_flag,
    MAX(case when court_flags.has_sup_vio_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sup_vio_flag,
    MAX(case when court_flags.has_fta_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_fta_flag,
    MAX(case when court_flags.has_sex_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sex_flag,
    MAX(case when court_flags.has_dui_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dui_flag,
    MAX(case when court_flags.has_restrain_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_restrain_flag,
    MAX(case when court_flags.has_property_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_property_flag,
    MAX(case when court_flags.has_drug_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_drug_flag,
    MAX(case when court_flags.has_dv_possible_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_possible_flag,  
    -- conviction charge flags not currently possible
    arrest_date.arrest_date
    

from "54_TULARE-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "54_TULARE-DWH_PROD"."SECURE_SHARE"."FACT_BOOKING" as booking 
        on agg.booking_key = booking.booking_key
    left join "54_TULARE-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_BOOKING_CHARGE" as book_charge 
        on booking.dim_principal_booking_charge_key = book_charge.dim_principal_booking_charge_key
    left join "54_TULARE-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case 
        on agg.booking_key = court_case.booking_key
    left join "54_TULARE-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_COURT_CHARGE" as court_charge 
        on court_case.dim_principal_court_charge_key = court_charge.dim_principal_court_charge_key
    left join "54_TULARE-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
        on court_case.dim_court_detail_key = court_detail.dim_court_detail_key
    left join "54_TULARE-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as pre_assessment
        on agg.pretrial_assessment_key = pre_assessment.pretrial_assessment_key
    left join "54_TULARE-DWH_PROD"."SECURE_SHARE"."DIM_RELEASE_CONDITION" as release_cond
        on pre_assessment.dim_release_condition_key = release_cond.dim_release_condition_key
    left join "54_TULARE-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as pre_detail
        on pre_assessment.dim_pretrial_detail_key = pre_detail.dim_pretrial_detail_key
    left join "54_TULARE-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_TERMINATION_DATE" as pre_termination_date
        on pre_assessment.dim_pretrial_termination_date_key = pre_termination_date.dim_pretrial_termination_date_key
    left join "54_TULARE-DWH_PROD"."SECURE_SHARE"."DIM_BOOKING_CHARGE_GROUP" as book_flags
        on booking.dim_booking_charge_group_key = book_flags.dim_booking_charge_group_key
    left join "54_TULARE-DWH_PROD"."SECURE_SHARE"."DIM_COURT_CHARGE_GROUP" as court_flags
        on court_case.dim_court_charge_group_key = court_flags.dim_court_charge_group_key
    left join "54_TULARE-DWH_PROD"."SECURE_SHARE"."DIM_ARREST_DATE" as arrest_date
        on booking.dim_arrest_date_key = arrest_date.dim_arrest_date_key
    
 

---------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------
UNION ALL

select
    distinct
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
    agg.is_release_eligible,
    agg.release_eligible_count,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.is_released_prearraignment,
    agg.released_prearraignment_count,
    agg.is_released_pretrial,
    agg.released_pretrial_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.has_new_criminal_activity,
    agg.new_criminal_activity_count,
    agg.has_pretrial_period_end,
    agg.pretrial_period_end_count,
    agg.has_pretrial_violation,
    agg.pretrial_violation_count,
    agg.is_unmatched_booking,
    agg.unmatched_booking_count,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name, pre_assessment.risk_score, pre_assessment.risk_score_raw, pre_assessment.score_failure_to_appear, pre_assessment.score_new_criminal_activity, pre_assessment.score_new_criminal_violent_activity, pre_assessment.generic_tool_total_score, pre_assessment.generic_tool_total_score_raw,
    book_charge.principal_booking_offense_level as booking_charge_level,
    book_charge.principal_booking_offense_type as booking_charge_code,
    book_charge.principal_booking_charge as booking_charge,
    book_charge.principal_booking_charge_hierarchy_code as booking_charge_hierarchy,
    FIRST_VALUE(court_charge.principal_court_charge) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge,
    FIRST_VALUE(court_charge.principal_court_offense_level) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_level,
    FIRST_VALUE(court_charge.principal_court_offense_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_code,
    FIRST_VALUE(court_charge.principal_court_charge_hierarchy_code) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_hierarchy,
    MIN(court_case.dim_plea_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS plea_date_key, -- need plea date dimension to get actual date out
    MIN(court_case.dim_sentence_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS sentence_date_key, -- need sentence date dimension to get actual date out
    FIRST_VALUE(court_detail.plea_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
                case when plea_type = 'Guilty' then 1
                     when plea_type = 'Nolo Contendere' then 2
                     when plea_type = 'Not Guilty' then 3
                     else null end          
          ) AS plea_type,
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
        ) AS disposition_type,
    FIRST_VALUE(court_detail.sentence_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when sentence_type = 'Prison' then 1
                when sentence_type = 'Jail' then 2
                when sentence_type = 'Probation' then 3
                when sentence_type = 'Fine' then 4
                when sentence_type = 'No Sentence Information' then 5
         else null end
        ) AS sentence_type,

    -- pretrial monitoring flags
    pre_detail.monitoring_level,
    pre_detail.court_date_reminder_flag,
    pre_detail.transit_service_flag,
    pre_detail.other_pretrial_service,
    pre_detail.condition_phone_flag,
    pre_detail.condition_inperson_flag,
    pre_detail.condition_drug_test_flag,
    pre_detail.condition_alcohol_flag,
    pre_detail.condition_em_flag,
    pre_detail.condition_no_contact_flag,
    
    pre_detail.release_decision_prearraignment,
    pre_detail.release_decision_arraignment,
    pre_detail.pretrial_release_type, -- what is this compared to normal release type??
    pre_detail.pretrial_termination_outcome,
    pre_detail.pretrial_termination_reason,
    pre_termination_date.pretrial_termination_date,
    
    -- arrest charge flags
    book_flags.has_special_charge as arrest_special_flag,
    book_flags.has_serious_felony_charge as arrest_serious_flag,
    book_flags.has_violent_felony_charge as arrest_violent_flag,
    book_flags.has_violent_psa_charge as arrest_violent_psa_flag,
    book_flags.has_capital_charge as arrest_capital_flag,
    book_flags.has_dv_charge as arrest_dv_flag,
    book_flags.has_marijuana_charge as arrest_marijuana_flag,
    book_flags.has_sup_vio_charge as arrest_sup_vio_flag,
    book_flags.has_fta_charge as arrest_fta_flag,
    book_flags.has_sex_charge as arrest_sex_flag,
    book_flags.has_dui_charge as arrest_dui_flag,
    book_flags.has_restrain_charge as arrest_restrain_flag,
    book_flags.has_property_charge as arrest_property_flag,
    book_flags.has_drug_charge as arrest_drug_flag,
    book_flags.has_dv_possible_charge as arrest_dv_possible_flag,
    book_flags.booking_charge_group_level as arrest_charge_level, -- will need to convert into misdo and felony flags in R
    
    -- filing charge flags
    MAX(case when court_flags.has_special_charge = 'Yes' THEN 1 else 0 end) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_special_flag,
    MAX(case when court_flags.has_serious_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_serious_flag,
    MAX(case when court_flags.has_violent_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_flag,
    MAX(case when court_flags.has_violent_psa_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_psa_flag,
    MAX(case when court_flags.has_capital_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_capital_flag,
    MAX(case when court_flags.has_dv_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_flag,
    MAX(case when court_flags.has_marijuana_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_marijuana_flag,
    MAX(case when court_flags.has_sup_vio_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sup_vio_flag,
    MAX(case when court_flags.has_fta_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_fta_flag,
    MAX(case when court_flags.has_sex_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sex_flag,
    MAX(case when court_flags.has_dui_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dui_flag,
    MAX(case when court_flags.has_restrain_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_restrain_flag,
    MAX(case when court_flags.has_property_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_property_flag,
    MAX(case when court_flags.has_drug_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_drug_flag,
    MAX(case when court_flags.has_dv_possible_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_possible_flag,  
    -- conviction charge flags not currently possible
    arrest_date.arrest_date
    

from "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."FACT_BOOKING" as booking 
        on agg.booking_key = booking.booking_key
    left join "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_BOOKING_CHARGE" as book_charge 
        on booking.dim_principal_booking_charge_key = book_charge.dim_principal_booking_charge_key
    left join "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case 
        on agg.booking_key = court_case.booking_key
    left join "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_COURT_CHARGE" as court_charge 
        on court_case.dim_principal_court_charge_key = court_charge.dim_principal_court_charge_key
    left join "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
        on court_case.dim_court_detail_key = court_detail.dim_court_detail_key
    left join "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as pre_assessment
        on agg.pretrial_assessment_key = pre_assessment.pretrial_assessment_key
    left join "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."DIM_RELEASE_CONDITION" as release_cond
        on pre_assessment.dim_release_condition_key = release_cond.dim_release_condition_key
    left join "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as pre_detail
        on pre_assessment.dim_pretrial_detail_key = pre_detail.dim_pretrial_detail_key
    left join "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_TERMINATION_DATE" as pre_termination_date
        on pre_assessment.dim_pretrial_termination_date_key = pre_termination_date.dim_pretrial_termination_date_key
    left join "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."DIM_BOOKING_CHARGE_GROUP" as book_flags
        on booking.dim_booking_charge_group_key = book_flags.dim_booking_charge_group_key
    left join "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."DIM_COURT_CHARGE_GROUP" as court_flags
        on court_case.dim_court_charge_group_key = court_flags.dim_court_charge_group_key
    left join "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."DIM_ARREST_DATE" as arrest_date
        on booking.dim_arrest_date_key = arrest_date.dim_arrest_date_key
    
 

---------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------
UNION ALL


select
    distinct
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
    agg.is_release_eligible,
    agg.release_eligible_count,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.is_released_prearraignment,
    agg.released_prearraignment_count,
    agg.is_released_pretrial,
    agg.released_pretrial_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.has_new_criminal_activity,
    agg.new_criminal_activity_count,
    agg.has_pretrial_period_end,
    agg.pretrial_period_end_count,
    agg.has_pretrial_violation,
    agg.pretrial_violation_count,
    agg.is_unmatched_booking,
    agg.unmatched_booking_count,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name, 
    pre_assessment.risk_score, 
    pre_assessment.risk_score_raw, 
    pre_assessment.score_failure_to_appear, 
    pre_assessment.score_new_criminal_activity, 
    pre_assessment.score_new_criminal_violent_activity, 
    pre_assessment.generic_tool_total_score, 
    pre_assessment.generic_tool_total_score_raw,
    book_charge.principal_booking_offense_level as booking_charge_level,
    book_charge.principal_booking_offense_type as booking_charge_code,
    book_charge.principal_booking_charge as booking_charge,
    book_charge.principal_booking_charge_hierarchy_code as booking_charge_hierarchy,
    FIRST_VALUE(court_charge.principal_court_charge) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge,
    FIRST_VALUE(court_charge.principal_court_offense_level) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_level,
    FIRST_VALUE(court_charge.principal_court_offense_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_code,
    FIRST_VALUE(court_charge.principal_court_charge_hierarchy_code) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_hierarchy,
    MIN(court_case.dim_plea_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS plea_date_key, -- need plea date dimension to get actual date out
    MIN(court_case.dim_sentence_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS sentence_date_key, -- need sentence date dimension to get actual date out
    FIRST_VALUE(court_detail.plea_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
                case when plea_type = 'Guilty' then 1
                     when plea_type = 'Nolo Contendere' then 2
                     when plea_type = 'Not Guilty' then 3
                     else null end          
          ) AS plea_type,
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
        ) AS disposition_type,
    FIRST_VALUE(court_detail.sentence_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when sentence_type = 'Prison' then 1
                when sentence_type = 'Jail' then 2
                when sentence_type = 'Probation' then 3
                when sentence_type = 'Fine' then 4
                when sentence_type = 'No Sentence Information' then 5
         else null end
        ) AS sentence_type,

    -- pretrial monitoring flags
    pre_detail.monitoring_level,
    pre_detail.court_date_reminder_flag,
    pre_detail.transit_service_flag,
    pre_detail.other_pretrial_service,
    pre_detail.condition_phone_flag,
    pre_detail.condition_inperson_flag,
    pre_detail.condition_drug_test_flag,
    pre_detail.condition_alcohol_flag,
    pre_detail.condition_em_flag,
    pre_detail.condition_no_contact_flag,
    
    pre_detail.release_decision_prearraignment,
    pre_detail.release_decision_arraignment,
    pre_detail.pretrial_release_type, -- what is this compared to normal release type??
    pre_detail.pretrial_termination_outcome,
    pre_detail.pretrial_termination_reason,
    pre_termination_date.pretrial_termination_date,
    
    -- arrest charge flags
    book_flags.has_special_charge as arrest_special_flag,
    book_flags.has_serious_felony_charge as arrest_serious_flag,
    book_flags.has_violent_felony_charge as arrest_violent_flag,
    book_flags.has_violent_psa_charge as arrest_violent_psa_flag,
    book_flags.has_capital_charge as arrest_capital_flag,
    book_flags.has_dv_charge as arrest_dv_flag,
    book_flags.has_marijuana_charge as arrest_marijuana_flag,
    book_flags.has_sup_vio_charge as arrest_sup_vio_flag,
    book_flags.has_fta_charge as arrest_fta_flag,
    book_flags.has_sex_charge as arrest_sex_flag,
    book_flags.has_dui_charge as arrest_dui_flag,
    book_flags.has_restrain_charge as arrest_restrain_flag,
    book_flags.has_property_charge as arrest_property_flag,
    book_flags.has_drug_charge as arrest_drug_flag,
    book_flags.has_dv_possible_charge as arrest_dv_possible_flag,
    book_flags.booking_charge_group_level as arrest_charge_level, -- will need to convert into misdo and felony flags in R
    
    -- filing charge flags
    MAX(case when court_flags.has_special_charge = 'Yes' THEN 1 else 0 end) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_special_flag,
    MAX(case when court_flags.has_serious_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_serious_flag,
    MAX(case when court_flags.has_violent_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_flag,
    MAX(case when court_flags.has_violent_psa_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_psa_flag,
    MAX(case when court_flags.has_capital_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_capital_flag,
    MAX(case when court_flags.has_dv_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_flag,
    MAX(case when court_flags.has_marijuana_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_marijuana_flag,
    MAX(case when court_flags.has_sup_vio_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sup_vio_flag,
    MAX(case when court_flags.has_fta_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_fta_flag,
    MAX(case when court_flags.has_sex_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sex_flag,
    MAX(case when court_flags.has_dui_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dui_flag,
    MAX(case when court_flags.has_restrain_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_restrain_flag,
    MAX(case when court_flags.has_property_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_property_flag,
    MAX(case when court_flags.has_drug_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_drug_flag,
    MAX(case when court_flags.has_dv_possible_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_possible_flag,  
    -- conviction charge flags not currently possible
    arrest_date.arrest_date
    

from "56_VENTURA-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "56_VENTURA-DWH_PROD"."SECURE_SHARE"."FACT_BOOKING" as booking 
        on agg.booking_key = booking.booking_key
    left join "56_VENTURA-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_BOOKING_CHARGE" as book_charge 
        on booking.dim_principal_booking_charge_key = book_charge.dim_principal_booking_charge_key
    left join "56_VENTURA-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case 
        on agg.booking_key = court_case.booking_key
    left join "56_VENTURA-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_COURT_CHARGE" as court_charge 
        on court_case.dim_principal_court_charge_key = court_charge.dim_principal_court_charge_key
    left join "56_VENTURA-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
        on court_case.dim_court_detail_key = court_detail.dim_court_detail_key
    left join "56_VENTURA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as pre_assessment
        on agg.pretrial_assessment_key = pre_assessment.pretrial_assessment_key
    left join "56_VENTURA-DWH_PROD"."SECURE_SHARE"."DIM_RELEASE_CONDITION" as release_cond
        on pre_assessment.dim_release_condition_key = release_cond.dim_release_condition_key
    left join "56_VENTURA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as pre_detail
        on pre_assessment.dim_pretrial_detail_key = pre_detail.dim_pretrial_detail_key
    left join "56_VENTURA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_TERMINATION_DATE" as pre_termination_date
        on pre_assessment.dim_pretrial_termination_date_key = pre_termination_date.dim_pretrial_termination_date_key
    left join "56_VENTURA-DWH_PROD"."SECURE_SHARE"."DIM_BOOKING_CHARGE_GROUP" as book_flags
        on booking.dim_booking_charge_group_key = book_flags.dim_booking_charge_group_key
    left join "56_VENTURA-DWH_PROD"."SECURE_SHARE"."DIM_COURT_CHARGE_GROUP" as court_flags
        on court_case.dim_court_charge_group_key = court_flags.dim_court_charge_group_key
    left join "56_VENTURA-DWH_PROD"."SECURE_SHARE"."DIM_ARREST_DATE" as arrest_date
        on booking.dim_arrest_date_key = arrest_date.dim_arrest_date_key
    
 

---------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------
UNION ALL


select
    distinct
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
    agg.is_release_eligible,
    agg.release_eligible_count,
    agg.assessment_date,
    agg.is_booking_assessed,
    agg.booking_assessed_count,
    agg.is_released_prearraignment,
    agg.released_prearraignment_count,
    agg.is_released_pretrial,
    agg.released_pretrial_count,
    agg.arraignment_date,
    agg.has_failure_to_appear,
    agg.failure_to_appear_count,
    agg.failure_to_appear_date,
    agg.has_new_criminal_activity,
    agg.new_criminal_activity_count,
    agg.has_pretrial_period_end,
    agg.pretrial_period_end_count,
    agg.has_pretrial_violation,
    agg.pretrial_violation_count,
    agg.is_unmatched_booking,
    agg.unmatched_booking_count,
    agg.recommended_release_type,
    agg.date_of_birth,
    agg.individual_age,
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name, 
    pre_assessment.risk_score, 
    pre_assessment.risk_score_raw, 
    pre_assessment.score_failure_to_appear, 
    pre_assessment.score_new_criminal_activity, 
    pre_assessment.score_new_criminal_violent_activity, 
    pre_assessment.generic_tool_total_score, 
    pre_assessment.generic_tool_total_score_raw,
    book_charge.principal_booking_offense_level as booking_charge_level,
    book_charge.principal_booking_offense_type as booking_charge_code,
    book_charge.principal_booking_charge as booking_charge,
    book_charge.principal_booking_charge_hierarchy_code as booking_charge_hierarchy,
    FIRST_VALUE(court_charge.principal_court_charge) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge,
    FIRST_VALUE(court_charge.principal_court_offense_level) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_level,
    FIRST_VALUE(court_charge.principal_court_offense_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_code,
    FIRST_VALUE(court_charge.principal_court_charge_hierarchy_code) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
               court_charge.principal_court_charge_hierarchy_code ASC
          ) AS court_charge_hierarchy,
    MIN(court_case.dim_plea_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS plea_date_key, -- need plea date dimension to get actual date out
    MIN(court_case.dim_sentence_date_key) OVER (PARTITION BY agg.BOOKING_KEY) AS sentence_date_key, -- need sentence date dimension to get actual date out
    FIRST_VALUE(court_detail.plea_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
                case when plea_type = 'Guilty' then 1
                     when plea_type = 'Nolo Contendere' then 2
                     when plea_type = 'Not Guilty' then 3
                     else null end          
          ) AS plea_type,
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
        ) AS disposition_type,
    FIRST_VALUE(court_detail.sentence_type) IGNORE NULLS OVER
        (
        PARTITION BY
            agg.BOOKING_KEY
        ORDER BY
            case when sentence_type = 'Prison' then 1
                when sentence_type = 'Jail' then 2
                when sentence_type = 'Probation' then 3
                when sentence_type = 'Fine' then 4
                when sentence_type = 'No Sentence Information' then 5
         else null end
        ) AS sentence_type,

    -- pretrial monitoring flags
    pre_detail.monitoring_level,
    pre_detail.court_date_reminder_flag,
    pre_detail.transit_service_flag,
    pre_detail.other_pretrial_service,
    pre_detail.condition_phone_flag,
    pre_detail.condition_inperson_flag,
    pre_detail.condition_drug_test_flag,
    pre_detail.condition_alcohol_flag,
    pre_detail.condition_em_flag,
    pre_detail.condition_no_contact_flag,
    
    pre_detail.release_decision_prearraignment,
    pre_detail.release_decision_arraignment,
    pre_detail.pretrial_release_type, -- what is this compared to normal release type??
    pre_detail.pretrial_termination_outcome,
    pre_detail.pretrial_termination_reason,
    pre_termination_date.pretrial_termination_date,
    
    -- arrest charge flags
    book_flags.has_special_charge as arrest_special_flag,
    book_flags.has_serious_felony_charge as arrest_serious_flag,
    book_flags.has_violent_felony_charge as arrest_violent_flag,
    book_flags.has_violent_psa_charge as arrest_violent_psa_flag,
    book_flags.has_capital_charge as arrest_capital_flag,
    book_flags.has_dv_charge as arrest_dv_flag,
    book_flags.has_marijuana_charge as arrest_marijuana_flag,
    book_flags.has_sup_vio_charge as arrest_sup_vio_flag,
    book_flags.has_fta_charge as arrest_fta_flag,
    book_flags.has_sex_charge as arrest_sex_flag,
    book_flags.has_dui_charge as arrest_dui_flag,
    book_flags.has_restrain_charge as arrest_restrain_flag,
    book_flags.has_property_charge as arrest_property_flag,
    book_flags.has_drug_charge as arrest_drug_flag,
    book_flags.has_dv_possible_charge as arrest_dv_possible_flag,
    book_flags.booking_charge_group_level as arrest_charge_level, -- will need to convert into misdo and felony flags in R
    
    -- filing charge flags
    MAX(case when court_flags.has_special_charge = 'Yes' THEN 1 else 0 end) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_special_flag,
    MAX(case when court_flags.has_serious_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_serious_flag,
    MAX(case when court_flags.has_violent_felony_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_flag,
    MAX(case when court_flags.has_violent_psa_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_violent_psa_flag,
    MAX(case when court_flags.has_capital_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_capital_flag,
    MAX(case when court_flags.has_dv_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_flag,
    MAX(case when court_flags.has_marijuana_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_marijuana_flag,
    MAX(case when court_flags.has_sup_vio_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sup_vio_flag,
    MAX(case when court_flags.has_fta_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_fta_flag,
    MAX(case when court_flags.has_sex_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_sex_flag,
    MAX(case when court_flags.has_dui_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dui_flag,
    MAX(case when court_flags.has_restrain_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_restrain_flag,
    MAX(case when court_flags.has_property_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_property_flag,
    MAX(case when court_flags.has_drug_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_drug_flag,
    MAX(case when court_flags.has_dv_possible_charge = 'Yes' THEN 1 ELSE 0 END) OVER
        (PARTITION BY agg.BOOKING_KEY) as court_dv_possible_flag,  
    -- conviction charge flags not currently possible
    arrest_date.arrest_date
    

from "58_YUBA-DWH_PROD"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "58_YUBA-DWH_PROD"."SECURE_SHARE"."FACT_BOOKING" as booking 
        on agg.booking_key = booking.booking_key
    left join "58_YUBA-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_BOOKING_CHARGE" as book_charge 
        on booking.dim_principal_booking_charge_key = book_charge.dim_principal_booking_charge_key
    left join "58_YUBA-DWH_PROD"."SECURE_SHARE"."FACT_COURT_CASE" as court_case 
        on agg.booking_key = court_case.booking_key
    left join "58_YUBA-DWH_PROD"."SECURE_SHARE"."DIM_PRINCIPAL_COURT_CHARGE" as court_charge 
        on court_case.dim_principal_court_charge_key = court_charge.dim_principal_court_charge_key
    left join "58_YUBA-DWH_PROD"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
        on court_case.dim_court_detail_key = court_detail.dim_court_detail_key
    left join "58_YUBA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as pre_assessment
        on agg.pretrial_assessment_key = pre_assessment.pretrial_assessment_key
    left join "58_YUBA-DWH_PROD"."SECURE_SHARE"."DIM_RELEASE_CONDITION" as release_cond
        on pre_assessment.dim_release_condition_key = release_cond.dim_release_condition_key
    left join "58_YUBA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as pre_detail
        on pre_assessment.dim_pretrial_detail_key = pre_detail.dim_pretrial_detail_key
    left join "58_YUBA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_TERMINATION_DATE" as pre_termination_date
        on pre_assessment.dim_pretrial_termination_date_key = pre_termination_date.dim_pretrial_termination_date_key
    left join "58_YUBA-DWH_PROD"."SECURE_SHARE"."DIM_BOOKING_CHARGE_GROUP" as book_flags
        on booking.dim_booking_charge_group_key = book_flags.dim_booking_charge_group_key
    left join "58_YUBA-DWH_PROD"."SECURE_SHARE"."DIM_COURT_CHARGE_GROUP" as court_flags
        on court_case.dim_court_charge_group_key = court_flags.dim_court_charge_group_key
    left join "58_YUBA-DWH_PROD"."SECURE_SHARE"."DIM_ARREST_DATE" as arrest_date
        on booking.dim_arrest_date_key = arrest_date.dim_arrest_date_key
    
 

---------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------

;

    
