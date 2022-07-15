create or replace view DWH_PROD.DATA_SCIENCE.JOIN_DF_STD as

select join_df.*,
    case when doj.arrest_date_doj is null then 0 else 1 end as doj_matched,
    doj.pretrial_period_end_date_doj, 
    doj.recid_arrested_flag_doj, 
    case when join_df.new_criminal_activity_count = 1 then 1
         when doj.recid_arrested_flag_doj = 1 then 1
         else 0 end
         as recid_county_or_doj,
    case when join_df.failure_to_appear_count = 1 then 1
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
     CASE
          WHEN join_df.charge_level in ('Felony', 'F') THEN 'F'
          WHEN join_df.charge_level in ('Misdemeanor', 'M') THEN 'M'
          WHEN join_df.charge_level in ('Infraction', 'I') THEN 'I'
          ELSE 'Other/Unknown'
     END AS charge_level_standard,
     CASE
        WHEN upper(join_df.ethnicity) in ('HISPANIC', 'HISP', 'HIS', 'H', 'CUBA') OR upper(join_df.race) in ('HISPANIC', 'MEX', 'LAT', 'HIS', 'CUBA') THEN 'Hispanic'
        WHEN upper(join_df.race) in ('AFR', 'BLACK') THEN 'Black'
        WHEN upper(join_df.race) in ('WHITE') THEN 'White'
        WHEN upper(join_df.race) in ('FILIPINO', 'KOREAN', 'OTHER ASIAN', 'LOATIAN', 'LAOTIAN',
                      'GUAMANIAN', 'CAMBODIAN', 'HAWAIIAN', 'PACIFIC ISLANDER', 
                      'VIETNAMESE', 'SAMOAN', 'CHINESE','ASIAN INDIAN', 'JAPANESE', 'ASIAN', 'PACIFIC ISLANDR', 'ASIA') THEN 'Asian'
        WHEN upper(join_df.race) in ('AMERICAN INDIAN') THEN 'Other/Unknown'
        WHEN upper(join_df.race) in ('', 'UNKNOWN', 'OTHER', 'ALL OTHERS', 'UNDEFINED', 'OTHER/UNKNOWN', 'ENG', 'NHIS') THEN 'Other/Unknown'
        ELSE join_df.race
     END AS race_standard,
     CASE
        WHEN doj.pretrial_period_end_date_doj is not null then 1
        WHEN join_df.pretrial_period_end_count = 1 then 1
        else 0
      END AS pretrial_period_end_count_standard,
      CASE
        WHEN join_df.released_pretrial_count = 1
            AND join_df.release_type = 'Bail Bond' then 'Bail'
        WHEN join_df.released_pretrial_count = 1
            AND join_df.release_type = 'Zero Bail' then 'Zero Bail'
        WHEN join_df.released_pretrial_count = 1
            AND join_df.booking_assessed_count = 1 then cast(join_df.risk_score as varchar)
        END AS risk_or_bail,
      CASE WHEN upper(join_df.pretrial_termination_outcome) = 'UNSUCCESSFUL'
        AND join_df.pretrial_termination_reason = 'Technical Violation' then 1
         else 0
    END AS technical_violation_termination_flag,
    CASE
        WHEN FTA_COUNTY_OR_DOJ = 1 AND recid_county_or_doj = 0 then 'FTA only'
        WHEN recid_county_or_doj = 1 AND FTA_COUNTY_OR_DOJ = 0 then 'NCA only'
        WHEN fta_county_or_doj = 1 AND recid_county_or_doj = 1 then 'FTA and NCA'
        WHEN fta_county_or_doj = 0 AND recid_county_or_doj = 0 then 'Neither'
     END AS fta_or_nca_indicator,
     CASE
        WHEN join_df.release_date is NULL then 0
        WHEN join_df.release_date is not NULL then 1
     END as release_indicator,
     CONCAT(join_df.county, '.', join_df.monitoring_level) as county_monitoring_level,
     CASE
        WHEN county_monitoring_level in ('Alameda.Level I', 'Calaveras.2', 'Nevada.Low Supervision',
                                        'Sacramento.1', 'Sacramento.2', 'Sacramento.3',
                                        'San Joaquin.Basic Pretrial Monitoring', 
                                        'San Mateo.MOR Basic', 'Santa Barbara.Basic',
                                        'Santa Barbara.Level 1', 'Santa Barbara.Level 2',
                                        'Santa Barbara.Out of County/Level 1',
                                        'Sonoma.Basic Supervision', 'Sonoma.Level 1 Monitoring',
                                         'Sonoma.Release to Pretrial Services with Court Reminder',
                                        'Tulare.PML-1', 'Tuolumne.1', 'Tuolumne.2', 
                                         'Kings.Reminder only', 'Kings.Basic Monitoring',
                                        'Napa.Basic Monitoring') then 'Lowest Levels'
        WHEN county_monitoring_level in ('Alameda.Level II', 'Calaveras.3', 'Nevada.Moderate Supervision',
                                        'Sacramento.4', 'Sacramento.5', 'San Joaquin.Enhanced Pretrial Monitoring',
                                        'San Mateo.MOR Regular', 'Santa Barbara.Elevated', 'Santa Barbara.Level 3',
                                        'Santa Barbara.Level 4', 'Santa Barbara.Standard',
                                        'Sonoma.Level 2 Monitoring', 'Sonoma.Moderate Supervision',
                                        'Tulare.PML-2', 'Tuolumne.3',
                                        'Kings.Enhanced Monitoring', 'Napa.Enhanced Monitoring') then 'Medium Levels'
        WHEN county_monitoring_level in ('Alameda.Level III', 'Calaveras.4', 'Calaveras.5',
                                        'Nevada.High Supervision',
                                        'Sacramento.6', 'San Joaquin.Intensive Pretrial Monitoring',
                                        'San Mateo.MOR Enhanced',
                                        'Santa Barbara.Bail with GPS', 'Santa Barbara.Bail with GPS and Check-ins',
                                        'Santa Barbara.Bail with GPS and SCRAM', 'Santa Barbara.Bail with SCRAM',
                                        'Santa Barbara.GPS Only', 'Santa Barbara.Intensive',
                                        'Santa Barbara.Intensive (historical)', 
                                        'Santa Barbara.Intensive PTS-High VPRAI',
                                        'Santa Barbara.Intensive PTS-Low VPRAI',
                                        'Santa Barbara.Intensive PTS-Moderate VPRAI',
                                        'Santa Barbara.Level 5', 'Santa Barbara.Level 6',
                                        'Santa Barbara.SCRAM Only',
                                        'Sonoma.Enhanced Supervision',
                                        'Sonoma.Level 3 Monitoring',
                                        'Tulare.PML-1GPS', 'Tulare.PML-1GPS/TAD',
                                        'Tulare.PML-1TAD', 'Tulare.PML-1TAD',
                                        'Tulare.PML-2GPS', 'Tulare.PML-2GPS/TAD',
                                        'Tulare.PML-2TAD', 'Tulare.PML-3',
                                        'Tulare.PML-3GPS', 'Tulare.PML-3GPS/TAD',
                                        'Tulare.PML-3TAD', 'Tuolumne.4',
                                        'Kings.Intensive Monitoring',
                                        'Napa.High Monitoring') then 'Highest Levels'
         ELSE  county_monitoring_level
         END AS monitoring_level_grouped
                                   

from "DWH_PROD"."DATA_SCIENCE"."JOIN_DF" AS join_df
LEFT JOIN DWH_PROD.REPORTING.AGG_PRETRIAL_KPI_DOJ_ENHANCED AS doj
	ON join_df.COUNTY = doj.COUNTY AND join_df.BOOKING_KEY = doj.BOOKING_KEY 
;




