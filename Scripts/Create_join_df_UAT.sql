select
    distinct
    agg.county,
    agg.booking_id,
    agg.booking_key,
    agg.booking_type,
    agg.booking_date,
    agg.release_date,
    agg.release_type,
    agg.charge_level, -- confirm that this is the booking charge level
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
    agg.sex,
    agg.race,
    agg.ethnicity,
    agg.zip_code_of_residency,
    agg.tool_name,
    agg.is_release_eligible,
    book_charge.principal_booking_offense_level as booking_charge_level,
    book_charge.principal_booking_offense_type as booking_charge_code,
    book_charge.principal_booking_charge as booking_charge,
    book_charge.principal_booking_charge_hierarchy_code AS booking_charge_hierarchy,
    book_charge.principal_booking_charge_description AS booking_charge_description,
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
    court_case.dim_plea_date_key, -- need plea date dimension to get actual date OUT, need TO figure OUT which date TO KEEP IF there ARE multiple cases
    FIRST_VALUE(court_detail.plea_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
                case when court_detail.plea_type = 'Guilty' then 1
                     when court_detail.plea_type = 'Nolo Contendere' then 2
                     when court_detail.plea_type = 'Not Guilty' then 3
                     else null end          
          ) AS plea_type,
     FIRST_VALUE(court_detail.disposition_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
                case when court_detail.disposition_type = 'Convicted' then 1
                     when court_detail.disposition_type = 'Acquitted' then 2
                     when court_detail.disposition_type = 'Dismissed' then 3
                     else null end          
          ) AS disposition_type,
      FIRST_VALUE(dispo_date.disposition_date) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY dispo_date.disposition_date DESC       
          ) AS disposition_date, -- will need TO JOIN KEY TO date dim
      dispo_date.disposition_date,
      FIRST_VALUE(court_detail.sentence_type) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY
                case when court_detail.sentence_type = 'Prison' then 1
                     when court_detail.sentence_type = 'Jail' then 2
                     when court_detail.sentence_type = 'Probation' then 3
                     when court_detail.sentence_type = 'Fine' then 4
                     else null end          
          ) AS sentence_type,
    FIRST_VALUE(court_case.sentence_date_key) IGNORE NULLS OVER
          (
          PARTITION BY
               agg.BOOKING_KEY -- might need to adjust partition to person/bookdate
          ORDER BY court_case.sentence_date_key        
          ) AS sentence_date_key,
    agg.release_decision_prearraignment,
    agg.release_decision_arraignment,
    pre_assessment.risk_score_raw,
    pre_assessment.risk_score,
    pretrial_detail.pretrial_termination_reason,
    termination_date.pretrial_termination_date,
    pretrial_detail.pretrial_termination_outcome,
    pretrial_detail.pretrial_termination_reason,
    pretrial_detail.pretrial_release_type,
    pretrial_detail.court_date_reminder_flag,
    pretrial_detail.transit_service_flag,
    pretrial_detail.condition_phone_flag,
    pretrial_detail.condition_inperson_flag,
    pretrial_detail.CONDITION_drug_test_flag,
    pretrial_detail.condition_alcohol_flag,
    pretrial_detail.condition_em_flag,
    pretrial_detail.condition_no_contact_flag,
    pretrial_detail.other_pretrial_service,
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
    court_flags.has_special_charge as court_special_flag,
    court_flags.has_serious_felony_charge as court_serious_flag,
    court_flags.has_violent_felony_charge as court_violent_flag,
    court_flags.has_violent_psa_charge as court_violent_psa_flag,
    court_flags.has_capital_charge as arrest_capital_flag,
    court_flags.has_dv_charge as court_dv_flag,
    court_flags.has_marijuana_charge as court_marijuana_flag,
    court_flags.has_sup_vio_charge as court_sup_vio_flag,
    court_flags.has_fta_charge as court_fta_flag,
    court_flags.has_sex_charge as court_sex_flag,
    court_flags.has_dui_charge as court_dui_flag,
    court_flags.has_restrain_charge as court_restrain_flag,
    court_flags.has_property_charge as court_property_flag,
    court_flags.has_drug_charge as court_drug_flag,
    court_flags.has_dv_possible_charge as court_dv_possible_flag,
    court_flags.court_charge_group_level as court_charge_level, -- will need to convert into misdo and felony flags in R
    
    arrest_date.arrest_date,
    
   
          
-- booking ids are duplicated in the fact_booking table, with different booking keys (for example with just different booking detail, ie booking type). 
-- need to ask Preston if that is how it is supposed to be
-- i recall they used person/bookdate to collapse, but where was that? how would that show up? how can we replicate that?

from "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."AGG_PRETRIAL_KPI" as agg
    left join "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."FACT_BOOKING" as booking 
        on agg.booking_key = booking.booking_key
    left join "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."DIM_PRINCIPAL_BOOKING_CHARGE" as book_charge 
        on booking.dim_principal_booking_charge_key = book_charge.dim_principal_booking_charge_key
    left join "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."FACT_COURT_CASE" as court_case 
        on agg.booking_key = court_case.booking_key
    left join "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."DIM_PRINCIPAL_COURT_CHARGE" as court_charge 
        on court_case.dim_principal_court_charge_key = court_charge.dim_principal_court_charge_key
    left join "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."DIM_COURT_DETAIL" as court_detail
        on court_case.dim_court_detail_key = court_detail.dim_court_detail_key
    left join "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as pre_assessment
        on booking.booking_key = pre_assessment.booking_key
    left join "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."DIM_RELEASE_CONDITION" as release_cond
        on pre_assessment.dim_release_condition_key = release_cond.dim_release_condition_key
    left join "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."DIM_BOOKING_CHARGE_GROUP" as book_flags
        on booking.dim_principal_booking_charge_key = book_flags.dim_principal_booking_charge_key
    left join "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."DIM_COURT_CHARGE_GROUP" as court_flags
        on court_case.dim_principal_court_charge_key = court_flags.dim_principal_court_charge_key
    left join "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."DIM_ARREST_DATE" as arrest_date
        on booking.dim_arrest_date_key = arrest_date.dim_arrest_date_key
    LEFT JOIN "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."DIM_DISPOSITION_DATE" AS DISPO_DATE 
    	ON court_case.DIM_DISPOSITION_DATE_KEY = dispo_date.dim_disposition_date_key
    LEFT JOIN "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" AS pretrial_detail
    	ON pre_assessment.DIM_PRETRIAL_DETAIL_KEY _KEY = pretrial_detail.DIM_PRETRIAL_DETAIL_KEY
    LEFT JOIN "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."DIM_PRETRIAL_TERMINATION_DATE" AS termination_date
    	ON pre_assessment.DIM_PRETRIAL_TERMINATION_DATE_KEY _KEY = termination_date.DIM_PRETRIAL_TERMINATION_DATE_KEY
    	

    
order by booking_id;
    -- i think we need to make a sub-query that collapses court cases to the booking key
    -- how do we group all court cases associated with the booking? check code from creation of agg table
    
    
    -- X book charge code
    -- X book charge
    -- X court charge code
    -- X court charge
    -- X court charge level
    -- X dispo type -- access added, in dim_court_detail
    -- ~ plea date -- need dim_plea_date_key
    -- X plea type
    -- ~ sentence date -- need dim_sentence_date
    -- X sentence type -- access added, in dim_court_detail
    -- X release decision (prearraignment and arraignment)
    -- !! monitoring conditions -- release_condition_key in fact_pretrial_assessment are all NULL
    -- !! monitoring level -- release_condition_key in fact_pretrial_assessment are all NULL
    -- X risk score raw
 	-- X risk score standardized
    -- X arrest charge flags
    -- X court charge flags
    -- O conviction charge flags -- not created in snowflake, would need to make a new dimension for conviction charge group
    -- X arrest date
    -- X tool name
    -- X pretrial termination reason
    -- X pretrial termination date
    -- 0 pretrial violation ---- this field (dim_pretrial_violation) is not standardized and therefore not useful
    -- !! pretrial violation date --- I see a dimension dim_pretrial_violation_date, but where is the key??
    -- X pretrial termination outcome 
    -- X pretrial ineligible reason
    -- X pretrial release type 
    -- X assessed flag
    -- recommended release flag -- create this out of dim_pretrial_detail.release_recommendation
    -- decision release flag -- create this out of dim_pretrial_detail release_decision_prearraignment and release_decision_arraignment
    -- release override flag -- create this from the two prior vars
    -- X court date reminder flag
    -- X transit service flag
    -- pretrial violation flag -- create this from has_fta and whatever new var we make for has new booking or new doj arrest 
    -- X booking charge description
    -- X arrest charge flags -- 20
    -- X court charge flags -- 20
    -- 0 conviction charge flags -- 20  we don't have conviction charge flags
    -- guilty plea flag -- create this from plea_type
    -- conviction flag -- create this from disposition_type
    -- prearraignment flag -- what is this?? prearraignment release? clarify
    -- new arrest flag -- create out of booking_type
    -- arrest sup vio post flag -- create from dim_booking_charge_group.has_sup_vio_charge
    -- arrest sup vio pre flag -- do we have anything to make this? unclear.
    -- qualifier flags -- what are these and do we need them?
    -- no bail flag -- create from dim_booking_charge_group.has_capital_charge, probably the best we can do?
    -- X pretrial eligible flag -- agg.is_release_eligible (could also use agg.release_eligible_count)
    -- . hold flag -- i don't think we need this because the booking type will prioritize holds over new arrests
    -- X arrest charge hierarchy
    -- X court charge hierarchy
    -- 0 conviction charge hierarchy - we don't have conviction offense tables
    
    
   
--- diagnostic stuff below here -----------------------------------------------------------------------------------------------------------------   
   
select booking_key, case_key
from "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."FACT_COURT_CASE"
order by booking_key;

select *
from "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."FACT_BOOKING"
where booking_id like '286399';

select *
from "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."DIM_BOOKING_DETAIL"
where dim_booking_detail_key = 25 or dim_booking_detail_key = 33;

select booking_key, num
from (select booking_key, count(*) as num
from "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."AGG_PRETRIAL_KPI"
group by booking_key)
where num=1;
-- booking keys are unique in the agg

select count(distinct booking_key), 'agg' as tab_name
from "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."AGG_PRETRIAL_KPI"
union all
select count(distinct booking_key), 'fact_booking' as tab_name
from "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."FACT_BOOKING";


select charge_keys
from (select count(distinct dim_principal_booking_charge_key) as charge_keys
from "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."FACT_BOOKING"
group by booking_id)
where charge_keys > 1;
-- one booking_id can have multiple principal booking charge keys, is this correct? seems wrong
-- why weren't the principal booking charge keys assigned by person/bookdate instead of booking_key since the booking keys are messed up?

select distinct(plea_type)
from "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."DIM_COURT_DETAIL"
order by 
    case when plea_type = 'Guilty' then 1
            when plea_type = 'Nolo Contendere' then 2
            when plea_type = 'Not Guilty' then 3
          else null end ;
select distinct(release_condition_key)
from "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT";

select distinct(disposition_type)
from "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."DIM_COURT_DETAIL";

select distinct(sentence_type)
from "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."DIM_COURT_DETAIL";

select * 
from "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."DIM_BOOKING_CHARGE_GROUP";

select risk_level, tool_name, county, count(*)
from "DWH_UAT"."REPORTING"."AGG_PRETRIAL_KPI"
group by risk_level, tool_name, county
order by tool_name, county;


-- code from Nate's collapse of Pretrial Episode
     FIRST_VALUE(STG_FACT_PRETRIAL_EPISODE_10_BOOKING.BOOKING_KEY) IGNORE NULLS OVER
          (
          PARTITION BY
          STG_FACT_PRETRIAL_EPISODE_10_BOOKING.COUNTY,
          STG_FACT_PRETRIAL_EPISODE_10_BOOKING.INDIVIDUAL_IDENTIFIER,
          STG_FACT_PRETRIAL_EPISODE_10_BOOKING.BOOKING_DATE
          ORDER BY
          TRY_CAST(STG_FACT_PRETRIAL_EPISODE_10_BOOKING.BOOKING_KEY AS NUMBER) ASC,
          STG_FACT_PRETRIAL_EPISODE_10_BOOKING.BOOKING_KEY ASC
          ) AS MINIMUM_BOOKING_KEY;
          
select *
from "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."AGG_PRETRIAL_KPI";

select *
from "01_ALAMEDA-DWH_UAT"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE";