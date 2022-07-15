
create or replace view DWH_PROD.DATA_SCIENCE.CJS_DF as

select 
    fpa.booking_key,
    county_name as county_name_orig,
    'Alameda' as county_name,
    indiv.sex, 
    indiv.race, 
    indiv.ethnicity, 
    DATEDIFF(YEAR,indiv.DOB, CURRENT_DATE()) AS INDIVIDUAL_AGE,
    pta.charge_level,
    adate.assessment_date, 
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw, 
    dpdetail.monitoring_level,
    dpdetail.release_decision_prearraignment,
    dpdetail.release_decision_arraignment,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    dtool.tool_name,
    dpdetail.risk_level -- this seems maybe broken? often null even when we have a tool score
from "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
    left join "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta on fpa.booking_key = pta.booking_key
    left join "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."DIM_COUNTY" as dcounty on fpa.dim_county_key = dcounty.dim_county_key
    left join "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_TOOL" as dtool on fpa.dim_assessment_tool_key = dtool.dim_assessment_tool_key
    left join "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."DIM_INDIVIDUAL" as indiv on fpa.dim_individual_key = indiv.dim_individual_key
    left join "01_ALAMEDA-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_DATE" as adate on fpa.dim_assessment_date_key = adate.dim_assessment_date_key
    
UNION ALL


select 
    fpa.booking_key,
    county_name as county_name_orig,
    'Calaveras' as county_name, 
    indiv.sex, 
    indiv.race, 
    indiv.ethnicity, 
    DATEDIFF(YEAR,indiv.DOB, CURRENT_DATE()) AS INDIVIDUAL_AGE,
    pta.charge_level,
    adate.assessment_date, 
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_decision_prearraignment,
    dpdetail.release_decision_arraignment,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    dtool.tool_name,
    dpdetail.risk_level -- need access to individual tool scores
from "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
    left join "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta on fpa.booking_key = pta.booking_key
    left join "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."DIM_COUNTY" as dcounty on fpa.dim_county_key = dcounty.dim_county_key
    left join "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_TOOL" as dtool on fpa.dim_assessment_tool_key = dtool.dim_assessment_tool_key
    left join "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."DIM_INDIVIDUAL" as indiv on fpa.dim_individual_key = indiv.dim_individual_key
    left join "05_CALAVERAS-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_DATE" as adate on fpa.dim_assessment_date_key = adate.dim_assessment_date_key
   
UNION ALL

select 
    fpa.booking_key,
    county_name as county_name_orig,
    'Kings' as county_name, 
    indiv.sex, 
    indiv.race, 
    indiv.ethnicity, 
    DATEDIFF(YEAR,indiv.DOB, CURRENT_DATE()) AS INDIVIDUAL_AGE,
    pta.charge_level,
    adate.assessment_date, 
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_decision_prearraignment,
    dpdetail.release_decision_arraignment,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    dtool.tool_name,
    dpdetail.risk_level -- need access to individual tool scores
from "16_KINGS-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
    left join "16_KINGS-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta on fpa.booking_key = pta.booking_key
    left join "16_KINGS-DWH_PROD"."SECURE_SHARE"."DIM_COUNTY" as dcounty on fpa.dim_county_key = dcounty.dim_county_key
    left join "16_KINGS-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_TOOL" as dtool on fpa.dim_assessment_tool_key = dtool.dim_assessment_tool_key
    left join "16_KINGS-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "16_KINGS-DWH_PROD"."SECURE_SHARE"."DIM_INDIVIDUAL" as indiv on fpa.dim_individual_key = indiv.dim_individual_key
    left join "16_KINGS-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_DATE" as adate on fpa.dim_assessment_date_key = adate.dim_assessment_date_key
   
UNION ALL


select 
    fpa.booking_key,
    county_name as county_name_orig,
    'Los Angeles' as county_name,
    indiv.sex, 
    indiv.race, 
    indiv.ethnicity, 
    DATEDIFF(YEAR,indiv.DOB, CURRENT_DATE()) AS INDIVIDUAL_AGE,
    pta.charge_level,
    adate.assessment_date, 
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_decision_prearraignment,
    dpdetail.release_decision_arraignment,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    dtool.tool_name,
    dpdetail.risk_level -- need access to individual tool scores
from "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
    left join "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta on fpa.booking_key = pta.booking_key
    left join "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."DIM_COUNTY" as dcounty on fpa.dim_county_key = dcounty.dim_county_key
    left join "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_TOOL" as dtool on fpa.dim_assessment_tool_key = dtool.dim_assessment_tool_key
    left join "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."DIM_INDIVIDUAL" as indiv on fpa.dim_individual_key = indiv.dim_individual_key
    left join "19_LOSANGELES-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_DATE" as adate on fpa.dim_assessment_date_key = adate.dim_assessment_date_key
   
UNION ALL


select 
    fpa.booking_key,
    county_name as county_name_orig,
    'Modoc' as county_name, 
    indiv.sex, 
    indiv.race, 
    indiv.ethnicity, 
    DATEDIFF(YEAR,indiv.DOB, CURRENT_DATE()) AS INDIVIDUAL_AGE,
    pta.charge_level,
    adate.assessment_date, 
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_decision_prearraignment,
    dpdetail.release_decision_arraignment,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    dtool.tool_name,
    dpdetail.risk_level -- need access to individual tool scores
from "25_MODOC-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
    left join "25_MODOC-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta on fpa.booking_key = pta.booking_key
    left join "25_MODOC-DWH_PROD"."SECURE_SHARE"."DIM_COUNTY" as dcounty on fpa.dim_county_key = dcounty.dim_county_key
    left join "25_MODOC-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_TOOL" as dtool on fpa.dim_assessment_tool_key = dtool.dim_assessment_tool_key
    left join "25_MODOC-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "25_MODOC-DWH_PROD"."SECURE_SHARE"."DIM_INDIVIDUAL" as indiv on fpa.dim_individual_key = indiv.dim_individual_key
    left join "25_MODOC-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_DATE" as adate on fpa.dim_assessment_date_key = adate.dim_assessment_date_key
   
UNION ALL


select 
    fpa.booking_key,
    county_name as county_name_orig,
    'Napa' as county_name, 
    indiv.sex, 
    indiv.race, 
    indiv.ethnicity, 
    DATEDIFF(YEAR,indiv.DOB, CURRENT_DATE()) AS INDIVIDUAL_AGE,
    pta.charge_level,
    adate.assessment_date, 
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_decision_prearraignment,
    dpdetail.release_decision_arraignment,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    dtool.tool_name,
    dpdetail.risk_level -- need access to individual tool scores
from "28_NAPA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
    left join "28_NAPA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta on fpa.booking_key = pta.booking_key
    left join "28_NAPA-DWH_PROD"."SECURE_SHARE"."DIM_COUNTY" as dcounty on fpa.dim_county_key = dcounty.dim_county_key
    left join "28_NAPA-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_TOOL" as dtool on fpa.dim_assessment_tool_key = dtool.dim_assessment_tool_key
    left join "28_NAPA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "28_NAPA-DWH_PROD"."SECURE_SHARE"."DIM_INDIVIDUAL" as indiv on fpa.dim_individual_key = indiv.dim_individual_key
    left join "28_NAPA-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_DATE" as adate on fpa.dim_assessment_date_key = adate.dim_assessment_date_key
   
UNION ALL


select 
    fpa.booking_key,
    county_name as county_name_orig,
    'Nevada' as county_name, 
    indiv.sex, 
    indiv.race, 
    indiv.ethnicity, 
    DATEDIFF(YEAR,indiv.DOB, CURRENT_DATE()) AS INDIVIDUAL_AGE,
    pta.charge_level,
    adate.assessment_date, 
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_decision_prearraignment,
    dpdetail.release_decision_arraignment,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    dtool.tool_name,
    dpdetail.risk_level -- need access to individual tool scores
from "29_NEVADA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
    left join "29_NEVADA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta on fpa.booking_key = pta.booking_key
    left join "29_NEVADA-DWH_PROD"."SECURE_SHARE"."DIM_COUNTY" as dcounty on fpa.dim_county_key = dcounty.dim_county_key
    left join "29_NEVADA-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_TOOL" as dtool on fpa.dim_assessment_tool_key = dtool.dim_assessment_tool_key
    left join "29_NEVADA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "29_NEVADA-DWH_PROD"."SECURE_SHARE"."DIM_INDIVIDUAL" as indiv on fpa.dim_individual_key = indiv.dim_individual_key
    left join "29_NEVADA-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_DATE" as adate on fpa.dim_assessment_date_key = adate.dim_assessment_date_key
   
UNION ALL


select 
    fpa.booking_key,
    county_name as county_name_orig,
    'Sacramento' as county_name, 
    indiv.sex, 
    indiv.race, 
    indiv.ethnicity, 
    DATEDIFF(YEAR,indiv.DOB, CURRENT_DATE()) AS INDIVIDUAL_AGE,
    pta.charge_level,
    adate.assessment_date, 
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_decision_prearraignment,
    dpdetail.release_decision_arraignment,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    dtool.tool_name,
    dpdetail.risk_level -- need access to individual tool scores
from "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
    left join "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta on fpa.booking_key = pta.booking_key
    left join "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."DIM_COUNTY" as dcounty on fpa.dim_county_key = dcounty.dim_county_key
    left join "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_TOOL" as dtool on fpa.dim_assessment_tool_key = dtool.dim_assessment_tool_key
    left join "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."DIM_INDIVIDUAL" as indiv on fpa.dim_individual_key = indiv.dim_individual_key
    left join "34_SACRAMENTO-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_DATE" as adate on fpa.dim_assessment_date_key = adate.dim_assessment_date_key
   
UNION ALL


select 
    fpa.booking_key,
    county_name as county_name_orig,
    'San Joaquin' as county_name,
    indiv.sex, 
    indiv.race, 
    indiv.ethnicity, 
    DATEDIFF(YEAR,indiv.DOB, CURRENT_DATE()) AS INDIVIDUAL_AGE,
    pta.charge_level,
    adate.assessment_date, 
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_decision_prearraignment,
    dpdetail.release_decision_arraignment,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    dtool.tool_name,
    dpdetail.risk_level -- need access to individual tool scores
from "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
    left join "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta on fpa.booking_key = pta.booking_key
    left join "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."DIM_COUNTY" as dcounty on fpa.dim_county_key = dcounty.dim_county_key
    left join "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_TOOL" as dtool on fpa.dim_assessment_tool_key = dtool.dim_assessment_tool_key
    left join "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."DIM_INDIVIDUAL" as indiv on fpa.dim_individual_key = indiv.dim_individual_key
    left join "39_SANJOAQUIN-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_DATE" as adate on fpa.dim_assessment_date_key = adate.dim_assessment_date_key
   
UNION ALL


select 
    fpa.booking_key,
    county_name as county_name_orig,
    'San Mateo' as county_name, 
    indiv.sex, 
    indiv.race, 
    indiv.ethnicity, 
    DATEDIFF(YEAR,indiv.DOB, CURRENT_DATE()) AS INDIVIDUAL_AGE,
    pta.charge_level,
    adate.assessment_date, 
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_decision_prearraignment,
    dpdetail.release_decision_arraignment,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    dtool.tool_name,
    dpdetail.risk_level -- need access to individual tool scores
from "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
    left join "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta on fpa.booking_key = pta.booking_key
    left join "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."DIM_COUNTY" as dcounty on fpa.dim_county_key = dcounty.dim_county_key
    left join "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_TOOL" as dtool on fpa.dim_assessment_tool_key = dtool.dim_assessment_tool_key
    left join "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."DIM_INDIVIDUAL" as indiv on fpa.dim_individual_key = indiv.dim_individual_key
    left join "41_SANMATEO-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_DATE" as adate on fpa.dim_assessment_date_key = adate.dim_assessment_date_key
   
UNION ALL


select 
    fpa.booking_key,
    county_name as county_name_orig,
    'Santa Barbara' as county_name,
    indiv.sex, 
    indiv.race, 
    indiv.ethnicity, 
    DATEDIFF(YEAR,indiv.DOB, CURRENT_DATE()) AS INDIVIDUAL_AGE,
    pta.charge_level,
    adate.assessment_date, 
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_decision_prearraignment,
    dpdetail.release_decision_arraignment,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    dtool.tool_name,
    dpdetail.risk_level -- need access to individual tool scores
from "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
    left join "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta on fpa.booking_key = pta.booking_key
    left join "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."DIM_COUNTY" as dcounty on fpa.dim_county_key = dcounty.dim_county_key
    left join "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_TOOL" as dtool on fpa.dim_assessment_tool_key = dtool.dim_assessment_tool_key
    left join "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."DIM_INDIVIDUAL" as indiv on fpa.dim_individual_key = indiv.dim_individual_key
    left join "42_SANTABARBARA-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_DATE" as adate on fpa.dim_assessment_date_key = adate.dim_assessment_date_key
   
UNION ALL


select 
    fpa.booking_key,
    county_name as county_name_orig,
    'Sierra' as county_name,
    indiv.sex, 
    indiv.race, 
    indiv.ethnicity, 
    DATEDIFF(YEAR,indiv.DOB, CURRENT_DATE()) AS INDIVIDUAL_AGE,
    pta.charge_level,
    adate.assessment_date, 
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_decision_prearraignment,
    dpdetail.release_decision_arraignment,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    dtool.tool_name,
    dpdetail.risk_level -- need access to individual tool scores
from "46_SIERRA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
    left join "46_SIERRA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta on fpa.booking_key = pta.booking_key
    left join "46_SIERRA-DWH_PROD"."SECURE_SHARE"."DIM_COUNTY" as dcounty on fpa.dim_county_key = dcounty.dim_county_key
    left join "46_SIERRA-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_TOOL" as dtool on fpa.dim_assessment_tool_key = dtool.dim_assessment_tool_key
    left join "46_SIERRA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "46_SIERRA-DWH_PROD"."SECURE_SHARE"."DIM_INDIVIDUAL" as indiv on fpa.dim_individual_key = indiv.dim_individual_key
    left join "46_SIERRA-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_DATE" as adate on fpa.dim_assessment_date_key = adate.dim_assessment_date_key
   
UNION ALL


select 
    fpa.booking_key,
    county_name as county_name_orig,
    'Sonoma' as county_name, 
    indiv.sex, 
    indiv.race, 
    indiv.ethnicity, 
    DATEDIFF(YEAR,indiv.DOB, CURRENT_DATE()) AS INDIVIDUAL_AGE,
    pta.charge_level,
    adate.assessment_date, 
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_decision_prearraignment,
    dpdetail.release_decision_arraignment,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    dtool.tool_name,
    dpdetail.risk_level -- need access to individual tool scores
from "49_SONOMA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
    left join "49_SONOMA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta on fpa.booking_key = pta.booking_key
    left join "49_SONOMA-DWH_PROD"."SECURE_SHARE"."DIM_COUNTY" as dcounty on fpa.dim_county_key = dcounty.dim_county_key
    left join "49_SONOMA-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_TOOL" as dtool on fpa.dim_assessment_tool_key = dtool.dim_assessment_tool_key
    left join "49_SONOMA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "49_SONOMA-DWH_PROD"."SECURE_SHARE"."DIM_INDIVIDUAL" as indiv on fpa.dim_individual_key = indiv.dim_individual_key
    left join "49_SONOMA-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_DATE" as adate on fpa.dim_assessment_date_key = adate.dim_assessment_date_key
   
UNION ALL


select 
    fpa.booking_key,
    county_name as county_name_orig,
    'Tulare' as county_name, 
    indiv.sex, 
    indiv.race, 
    indiv.ethnicity, 
    DATEDIFF(YEAR,indiv.DOB, CURRENT_DATE()) AS INDIVIDUAL_AGE,
    pta.charge_level,
    adate.assessment_date, 
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_decision_prearraignment,
    dpdetail.release_decision_arraignment,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    dtool.tool_name,
    dpdetail.risk_level -- need access to individual tool scores
from "54_TULARE-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
    left join "54_TULARE-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta on fpa.booking_key = pta.booking_key
    left join "54_TULARE-DWH_PROD"."SECURE_SHARE"."DIM_COUNTY" as dcounty on fpa.dim_county_key = dcounty.dim_county_key
    left join "54_TULARE-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_TOOL" as dtool on fpa.dim_assessment_tool_key = dtool.dim_assessment_tool_key
    left join "54_TULARE-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "54_TULARE-DWH_PROD"."SECURE_SHARE"."DIM_INDIVIDUAL" as indiv on fpa.dim_individual_key = indiv.dim_individual_key
    left join "54_TULARE-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_DATE" as adate on fpa.dim_assessment_date_key = adate.dim_assessment_date_key
   
UNION ALL


select 
    fpa.booking_key,
    county_name as county_name_orig,
    'Tuolumne' as county_name, 
    indiv.sex, 
    indiv.race, 
    indiv.ethnicity, 
    DATEDIFF(YEAR,indiv.DOB, CURRENT_DATE()) AS INDIVIDUAL_AGE,
    pta.charge_level,
    adate.assessment_date, 
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_decision_prearraignment,
    dpdetail.release_decision_arraignment,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    dtool.tool_name,
    dpdetail.risk_level -- need access to individual tool scores
from "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
    left join "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta on fpa.booking_key = pta.booking_key
    left join "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."DIM_COUNTY" as dcounty on fpa.dim_county_key = dcounty.dim_county_key
    left join "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_TOOL" as dtool on fpa.dim_assessment_tool_key = dtool.dim_assessment_tool_key
    left join "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."DIM_INDIVIDUAL" as indiv on fpa.dim_individual_key = indiv.dim_individual_key
    left join "55_TUOLUMNE-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_DATE" as adate on fpa.dim_assessment_date_key = adate.dim_assessment_date_key
   
UNION ALL


select 
    fpa.booking_key,
    county_name as county_name_orig,
    'Ventura' as county_name, 
    indiv.sex, 
    indiv.race, 
    indiv.ethnicity, 
    DATEDIFF(YEAR,indiv.DOB, CURRENT_DATE()) AS INDIVIDUAL_AGE,
    pta.charge_level,
    adate.assessment_date, 
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_decision_prearraignment,
    dpdetail.release_decision_arraignment,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    dtool.tool_name,
    dpdetail.risk_level -- need access to individual tool scores
from "56_VENTURA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
    left join "56_VENTURA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta on fpa.booking_key = pta.booking_key
    left join "56_VENTURA-DWH_PROD"."SECURE_SHARE"."DIM_COUNTY" as dcounty on fpa.dim_county_key = dcounty.dim_county_key
    left join "56_VENTURA-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_TOOL" as dtool on fpa.dim_assessment_tool_key = dtool.dim_assessment_tool_key
    left join "56_VENTURA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "56_VENTURA-DWH_PROD"."SECURE_SHARE"."DIM_INDIVIDUAL" as indiv on fpa.dim_individual_key = indiv.dim_individual_key
    left join "56_VENTURA-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_DATE" as adate on fpa.dim_assessment_date_key = adate.dim_assessment_date_key
   
UNION ALL


select 
    fpa.booking_key,
    county_name as county_name_orig,
    'Yuba' as county_name, 
    indiv.sex, 
    indiv.race, 
    indiv.ethnicity, 
    DATEDIFF(YEAR,indiv.DOB, CURRENT_DATE()) AS INDIVIDUAL_AGE,
    pta.charge_level,
    adate.assessment_date, 
    fpa.risk_score,
    fpa.risk_score_raw,
    fpa.score_failure_to_appear,
    fpa.score_new_criminal_activity,
    fpa.score_new_criminal_violent_activity,
    fpa.generic_tool_total_score, fpa.generic_tool_total_score_raw,
    dpdetail.monitoring_level,
    dpdetail.release_decision_prearraignment,
    dpdetail.release_decision_arraignment,
    dpdetail.release_recommendation,
    dpdetail.pretrial_termination_reason,
    dpdetail.pretrial_termination_outcome,
    dtool.tool_name,
    dpdetail.risk_level -- need access to individual tool scores
from "58_YUBA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_ASSESSMENT" as fpa
    left join "58_YUBA-DWH_PROD"."SECURE_SHARE"."FACT_PRETRIAL_EPISODE" as pta on fpa.booking_key = pta.booking_key
    left join "58_YUBA-DWH_PROD"."SECURE_SHARE"."DIM_COUNTY" as dcounty on fpa.dim_county_key = dcounty.dim_county_key
    left join "58_YUBA-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_TOOL" as dtool on fpa.dim_assessment_tool_key = dtool.dim_assessment_tool_key
    left join "58_YUBA-DWH_PROD"."SECURE_SHARE"."DIM_PRETRIAL_DETAIL" as dpdetail on fpa.dim_pretrial_detail_key = dpdetail.dim_pretrial_detail_key
    left join "58_YUBA-DWH_PROD"."SECURE_SHARE"."DIM_INDIVIDUAL" as indiv on fpa.dim_individual_key = indiv.dim_individual_key
    left join "58_YUBA-DWH_PROD"."SECURE_SHARE"."DIM_ASSESSMENT_DATE" as adate on fpa.dim_assessment_date_key = adate.dim_assessment_date_key

;
