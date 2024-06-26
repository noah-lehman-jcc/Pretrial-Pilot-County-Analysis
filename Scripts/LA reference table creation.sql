USE DATABASE DWH_PROD;
USE ROLE QA_ANALYST;

/************************************************/ 
/***** CREATE PUBLIC.PRETRIAL_COLUMN_CHECKS *****/
/************************************************/ 
     
CREATE OR REPLACE
TEMPORARY TABLE PUBLIC.PRETRIAL_JCC_STD_VALUES (
     SOURCE_FILE_NAME VARCHAR,
     LOAD_COLUMN VARCHAR,
     LOAD_COLUMN_TRIMMED VARCHAR,
     JCC_STANDARDIZED_COLUMN VARCHAR,
     COLUMN_VALUE VARCHAR,
     JCC_STD_COLUMN_VALUE VARCHAR,
     RECORD_CNT NUMBER
);

/**********************************************/ 
/***** LOAD PUBLIC.PRETRIAL_COLUMN_CHECKS *****/
/**********************************************/ 
INSERT INTO PUBLIC.PRETRIAL_JCC_STD_VALUES
SELECT
     SOURCE_FILE_NAME,
     LOAD_COLUMN,
     CASE
          WHEN Load_Column LIKE '%_CHARGE_CODE' THEN 'CHARGE_CODE'
          WHEN Load_Column LIKE '%_CHARGE_LEVEL' THEN 'CHARGE_LEVEL'
          ELSE Load_Column
     END AS LOAD_COLUMN_TRIMMED,
     JCC_STANDARDIZED_COLUMN,
     COLUMN_VALUE,
     JCC_STD_COLUMN_VALUE,
     RECORD_CNT
FROM
(SELECT 'COURT_CASE_CHARGE' AS SOURCE_FILE_NAME, 'PLEA_TYPE' AS LOAD_COLUMN, 'PLEA_TYPE_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, PLEA_TYPE AS COLUMN_VALUE, PLEA_TYPE_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_COURT_CASE_CHARGE WHERE ( OPERATION_TYPE IS NULL OR OPERATION_TYPE <> 'D' ) GROUP BY PLEA_TYPE, PLEA_TYPE_JCC_STANDARDIZED UNION
SELECT 'COURT_CHARGE_DISPOSITION' AS SOURCE_FILE_NAME, 'DISPOSITION_TYPE' AS LOAD_COLUMN, 'DISPOSITION_TYPE_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, DISPOSITION_TYPE AS COLUMN_VALUE, DISPOSITION_TYPE_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_COURT_CHARGE_DISPOSITION WHERE ( OPERATION_TYPE IS NULL OR OPERATION_TYPE <> 'D' ) GROUP BY DISPOSITION_TYPE, DISPOSITION_TYPE_JCC_STANDARDIZED UNION
SELECT 'COURT_CHARGE_DISPOSITION' AS SOURCE_FILE_NAME, 'SUB_DISPOSITION_TYPE' AS LOAD_COLUMN, 'SUB_DISPOSITION_TYPE_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, SUB_DISPOSITION_TYPE AS COLUMN_VALUE, SUB_DISPOSITION_TYPE_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_COURT_CHARGE_DISPOSITION WHERE ( OPERATION_TYPE IS NULL OR OPERATION_TYPE <> 'D' ) GROUP BY SUB_DISPOSITION_TYPE, SUB_DISPOSITION_TYPE_JCC_STANDARDIZED UNION
SELECT 'COURT_CRIMINAL_CASE_DEFENDANT' AS SOURCE_FILE_NAME, 'CASE_STATUS' AS LOAD_COLUMN, 'CASE_STATUS_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, CASE_STATUS AS COLUMN_VALUE, CASE_STATUS_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_COURT_CRIMINAL_CASE_DEFENDANT WHERE ( OPERATION_TYPE IS NULL OR OPERATION_TYPE <> 'D' ) GROUP BY CASE_STATUS, CASE_STATUS_JCC_STANDARDIZED UNION
SELECT 'COURT_CRIMINAL_CASE_DEFENDANT' AS SOURCE_FILE_NAME, 'REPRESENTATION_STATUS' AS LOAD_COLUMN, 'REPRESENTATION_STATUS_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, REPRESENTATION_STATUS AS COLUMN_VALUE, REPRESENTATION_STATUS_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_COURT_CRIMINAL_CASE_DEFENDANT WHERE ( OPERATION_TYPE IS NULL OR OPERATION_TYPE <> 'D' ) GROUP BY REPRESENTATION_STATUS, REPRESENTATION_STATUS_JCC_STANDARDIZED UNION
SELECT 'COURT_HEARING' AS SOURCE_FILE_NAME, 'COURT_RELEASE_DECISION' AS LOAD_COLUMN, 'COURT_RELEASE_DECISION_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, COURT_RELEASE_DECISION AS COLUMN_VALUE, COURT_RELEASE_DECISION_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_COURT_HEARING WHERE ( OPERATION_TYPE IS NULL OR OPERATION_TYPE <> 'D' ) GROUP BY COURT_RELEASE_DECISION, COURT_RELEASE_DECISION_JCC_STANDARDIZED UNION
SELECT 'COURT_HEARING' AS SOURCE_FILE_NAME, 'COURT_RELEASE_TYPE' AS LOAD_COLUMN, 'COURT_RELEASE_TYPE_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, COURT_RELEASE_TYPE AS COLUMN_VALUE, COURT_RELEASE_TYPE_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_COURT_HEARING WHERE ( OPERATION_TYPE IS NULL OR OPERATION_TYPE <> 'D' ) GROUP BY COURT_RELEASE_TYPE, COURT_RELEASE_TYPE_JCC_STANDARDIZED UNION
SELECT 'COURT_HEARING' AS SOURCE_FILE_NAME, 'HEARING_RESULT' AS LOAD_COLUMN, 'HEARING_RESULT_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, HEARING_RESULT AS COLUMN_VALUE, HEARING_RESULT_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_COURT_HEARING WHERE ( OPERATION_TYPE IS NULL OR OPERATION_TYPE <> 'D' ) GROUP BY HEARING_RESULT, HEARING_RESULT_JCC_STANDARDIZED UNION
SELECT 'COURT_HEARING' AS SOURCE_FILE_NAME, 'HEARING_STATUS' AS LOAD_COLUMN, 'HEARING_STATUS_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, HEARING_STATUS AS COLUMN_VALUE, HEARING_STATUS_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_COURT_HEARING WHERE ( OPERATION_TYPE IS NULL OR OPERATION_TYPE <> 'D' ) GROUP BY HEARING_STATUS, HEARING_STATUS_JCC_STANDARDIZED UNION
SELECT 'COURT_HEARING' AS SOURCE_FILE_NAME, 'HEARING_TYPE' AS LOAD_COLUMN, 'HEARING_TYPE_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, HEARING_TYPE AS COLUMN_VALUE, HEARING_TYPE_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_COURT_HEARING WHERE ( OPERATION_TYPE IS NULL OR OPERATION_TYPE <> 'D' ) GROUP BY HEARING_TYPE, HEARING_TYPE_JCC_STANDARDIZED UNION
SELECT 'COURT_INDIVIDUAL' AS SOURCE_FILE_NAME, 'ETHNICITY' AS LOAD_COLUMN, 'ETHNICITY_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, ETHNICITY AS COLUMN_VALUE, ETHNICITY_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_COURT_INDIVIDUAL WHERE ( OPERATION_TYPE IS NULL OR OPERATION_TYPE <> 'D' ) GROUP BY ETHNICITY, ETHNICITY_JCC_STANDARDIZED UNION
SELECT 'COURT_INDIVIDUAL' AS SOURCE_FILE_NAME, 'LANGUAGE' AS LOAD_COLUMN, 'LANGUAGE_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, LANGUAGE AS COLUMN_VALUE, LANGUAGE_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_COURT_INDIVIDUAL WHERE ( OPERATION_TYPE IS NULL OR OPERATION_TYPE <> 'D' ) GROUP BY LANGUAGE, LANGUAGE_JCC_STANDARDIZED UNION
SELECT 'COURT_INDIVIDUAL' AS SOURCE_FILE_NAME, 'RACE' AS LOAD_COLUMN, 'RACE_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, RACE AS COLUMN_VALUE, RACE_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_COURT_INDIVIDUAL WHERE ( OPERATION_TYPE IS NULL OR OPERATION_TYPE <> 'D' ) GROUP BY RACE, RACE_JCC_STANDARDIZED UNION
SELECT 'COURT_INDIVIDUAL' AS SOURCE_FILE_NAME, 'SEX' AS LOAD_COLUMN, 'SEX_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, SEX AS COLUMN_VALUE, SEX_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_COURT_INDIVIDUAL WHERE ( OPERATION_TYPE IS NULL OR OPERATION_TYPE <> 'D' ) GROUP BY SEX, SEX_JCC_STANDARDIZED UNION
SELECT 'COURT_SENTENCE' AS SOURCE_FILE_NAME, 'SENTENCE_TYPE' AS LOAD_COLUMN, 'SENTENCE_TYPE_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, SENTENCE_TYPE AS COLUMN_VALUE, SENTENCE_TYPE_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_COURT_SENTENCE WHERE ( OPERATION_TYPE IS NULL OR OPERATION_TYPE <> 'D' ) GROUP BY SENTENCE_TYPE, SENTENCE_TYPE_JCC_STANDARDIZED UNION
SELECT 'COURT_WARRANT' AS SOURCE_FILE_NAME, 'WARRANT_REASON' AS LOAD_COLUMN, 'WARRANT_REASON_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, WARRANT_REASON AS COLUMN_VALUE, WARRANT_REASON_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_COURT_WARRANT WHERE ( OPERATION_TYPE IS NULL OR OPERATION_TYPE <> 'D' ) GROUP BY WARRANT_REASON, WARRANT_REASON_JCC_STANDARDIZED UNION
SELECT 'COURT_WARRANT' AS SOURCE_FILE_NAME, 'WARRANT_STATUS' AS LOAD_COLUMN, 'WARRANT_STATUS_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, WARRANT_STATUS AS COLUMN_VALUE, WARRANT_STATUS_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_COURT_WARRANT WHERE ( OPERATION_TYPE IS NULL OR OPERATION_TYPE <> 'D' ) GROUP BY WARRANT_STATUS, WARRANT_STATUS_JCC_STANDARDIZED UNION
SELECT 'COURT_WARRANT' AS SOURCE_FILE_NAME, 'WARRANT_TYPE' AS LOAD_COLUMN, 'WARRANT_TYPE_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, WARRANT_TYPE AS COLUMN_VALUE, WARRANT_TYPE_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_COURT_WARRANT WHERE ( OPERATION_TYPE IS NULL OR OPERATION_TYPE <> 'D' ) GROUP BY WARRANT_TYPE, WARRANT_TYPE_JCC_STANDARDIZED UNION
SELECT 'JAIL_BOOKING' AS SOURCE_FILE_NAME, 'BOOKING_TYPE' AS LOAD_COLUMN, 'BOOKING_TYPE_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, BOOKING_TYPE AS COLUMN_VALUE, BOOKING_TYPE_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_JAIL_BOOKING GROUP BY BOOKING_TYPE, BOOKING_TYPE_JCC_STANDARDIZED UNION
SELECT 'JAIL_BOOKING' AS SOURCE_FILE_NAME, 'RELEASE_TYPE' AS LOAD_COLUMN, 'RELEASE_TYPE_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, RELEASE_TYPE AS COLUMN_VALUE, RELEASE_TYPE_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_JAIL_BOOKING GROUP BY RELEASE_TYPE, RELEASE_TYPE_JCC_STANDARDIZED UNION
SELECT 'JAIL_BOOKING_CHARGE' AS SOURCE_FILE_NAME, 'BOOKING_CHARGE_CODE' AS LOAD_COLUMN, 'BOOKING_CHARGE_CODE_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, BOOKING_CHARGE_CODE AS COLUMN_VALUE, BOOKING_CHARGE_CODE_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_JAIL_BOOKING_CHARGE GROUP BY BOOKING_CHARGE_CODE, BOOKING_CHARGE_CODE_JCC_STANDARDIZED UNION
SELECT 'JAIL_BOOKING_CHARGE' AS SOURCE_FILE_NAME, 'BOOKING_CHARGE_LEVEL' AS LOAD_COLUMN, 'BOOKING_CHARGE_LEVEL_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, BOOKING_CHARGE_LEVEL AS COLUMN_VALUE, BOOKING_CHARGE_LEVEL_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_JAIL_BOOKING_CHARGE GROUP BY BOOKING_CHARGE_LEVEL, BOOKING_CHARGE_LEVEL_JCC_STANDARDIZED UNION
SELECT 'JAIL_INDIVIDUAL' AS SOURCE_FILE_NAME, 'ETHNICITY' AS LOAD_COLUMN, 'ETHNICITY_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, ETHNICITY AS COLUMN_VALUE, ETHNICITY_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_JAIL_INDIVIDUAL GROUP BY ETHNICITY, ETHNICITY_JCC_STANDARDIZED UNION
SELECT 'JAIL_INDIVIDUAL' AS SOURCE_FILE_NAME, 'LANGUAGE' AS LOAD_COLUMN, 'LANGUAGE_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, LANGUAGE AS COLUMN_VALUE, LANGUAGE_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_JAIL_INDIVIDUAL GROUP BY LANGUAGE, LANGUAGE_JCC_STANDARDIZED UNION
SELECT 'JAIL_INDIVIDUAL' AS SOURCE_FILE_NAME, 'RACE' AS LOAD_COLUMN, 'RACE_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, RACE AS COLUMN_VALUE, RACE_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_JAIL_INDIVIDUAL GROUP BY RACE, RACE_JCC_STANDARDIZED UNION
SELECT 'JAIL_INDIVIDUAL' AS SOURCE_FILE_NAME, 'SEX' AS LOAD_COLUMN, 'SEX_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, SEX AS COLUMN_VALUE, SEX_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_JAIL_INDIVIDUAL GROUP BY SEX, SEX_JCC_STANDARDIZED UNION
SELECT 'PRETRIAL_ASSESSMENT' AS SOURCE_FILE_NAME, 'PRETRIAL_RELEASE_TYPE' AS LOAD_COLUMN, 'PRETRIAL_RELEASE_TYPE_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, PRETRIAL_RELEASE_TYPE AS COLUMN_VALUE, PRETRIAL_RELEASE_TYPE_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_PRETRIAL_ASSESSMENT GROUP BY PRETRIAL_RELEASE_TYPE, PRETRIAL_RELEASE_TYPE_JCC_STANDARDIZED UNION
SELECT 'PRETRIAL_ASSESSMENT' AS SOURCE_FILE_NAME, 'PRETRIAL_TERMINATION_OUTCOME' AS LOAD_COLUMN, 'PRETRIAL_TERMINATION_OUTCOME_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, PRETRIAL_TERMINATION_OUTCOME AS COLUMN_VALUE, PRETRIAL_TERMINATION_OUTCOME_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_PRETRIAL_ASSESSMENT GROUP BY PRETRIAL_TERMINATION_OUTCOME, PRETRIAL_TERMINATION_OUTCOME_JCC_STANDARDIZED UNION
SELECT 'PRETRIAL_ASSESSMENT' AS SOURCE_FILE_NAME, 'PRETRIAL_TERMINATION_REASON' AS LOAD_COLUMN, 'PRETRIAL_TERMINATION_REASON_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, PRETRIAL_TERMINATION_REASON AS COLUMN_VALUE, PRETRIAL_TERMINATION_REASON_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_PRETRIAL_ASSESSMENT GROUP BY PRETRIAL_TERMINATION_REASON, PRETRIAL_TERMINATION_REASON_JCC_STANDARDIZED UNION
SELECT 'PRETRIAL_ASSESSMENT' AS SOURCE_FILE_NAME, 'RELEASE_AUTHORIZATION' AS LOAD_COLUMN, 'RELEASE_AUTHORIZATION_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, RELEASE_AUTHORIZATION AS COLUMN_VALUE, RELEASE_AUTHORIZATION_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_PRETRIAL_ASSESSMENT GROUP BY RELEASE_AUTHORIZATION, RELEASE_AUTHORIZATION_JCC_STANDARDIZED UNION
SELECT 'PRETRIAL_ASSESSMENT' AS SOURCE_FILE_NAME, 'RELEASE_DECISION_ARRAIGNMENT' AS LOAD_COLUMN, 'RELEASE_DECISION_ARRAIGNMENT_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, RELEASE_DECISION_ARRAIGNMENT AS COLUMN_VALUE, RELEASE_DECISION_ARRAIGNMENT_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_PRETRIAL_ASSESSMENT GROUP BY RELEASE_DECISION_ARRAIGNMENT, RELEASE_DECISION_ARRAIGNMENT_JCC_STANDARDIZED UNION
SELECT 'PRETRIAL_ASSESSMENT' AS SOURCE_FILE_NAME, 'RELEASE_DECISION_PREARRAIGNMENT' AS LOAD_COLUMN, 'RELEASE_DECISION_PREARRAIGNMENT_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, RELEASE_DECISION_PREARRAIGNMENT AS COLUMN_VALUE, RELEASE_DECISION_PREARRAIGNMENT_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_PRETRIAL_ASSESSMENT GROUP BY RELEASE_DECISION_PREARRAIGNMENT, RELEASE_DECISION_PREARRAIGNMENT_JCC_STANDARDIZED UNION
SELECT 'PRETRIAL_ASSESSMENT' AS SOURCE_FILE_NAME, 'RELEASE_RECOMMENDATION' AS LOAD_COLUMN, 'RELEASE_RECOMMENDATION_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, RELEASE_RECOMMENDATION AS COLUMN_VALUE, RELEASE_RECOMMENDATION_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_PRETRIAL_ASSESSMENT GROUP BY RELEASE_RECOMMENDATION, RELEASE_RECOMMENDATION_JCC_STANDARDIZED UNION
SELECT 'PRETRIAL_INDIVIDUAL' AS SOURCE_FILE_NAME, 'ETHNICITY' AS LOAD_COLUMN, 'ETHNICITY_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, ETHNICITY AS COLUMN_VALUE, ETHNICITY_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_PRETRIAL_INDIVIDUAL GROUP BY ETHNICITY, ETHNICITY_JCC_STANDARDIZED UNION
SELECT 'PRETRIAL_INDIVIDUAL' AS SOURCE_FILE_NAME, 'LANGUAGE' AS LOAD_COLUMN, 'LANGUAGE_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, LANGUAGE AS COLUMN_VALUE, LANGUAGE_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_PRETRIAL_INDIVIDUAL GROUP BY LANGUAGE, LANGUAGE_JCC_STANDARDIZED UNION
SELECT 'PRETRIAL_INDIVIDUAL' AS SOURCE_FILE_NAME, 'RACE' AS LOAD_COLUMN, 'RACE_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, RACE AS COLUMN_VALUE, RACE_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_PRETRIAL_INDIVIDUAL GROUP BY RACE, RACE_JCC_STANDARDIZED UNION
SELECT 'PRETRIAL_INDIVIDUAL' AS SOURCE_FILE_NAME, 'SEX' AS LOAD_COLUMN, 'SEX_JCC_STANDARDIZED' AS JCC_STANDARDIZED_COLUMN, SEX AS COLUMN_VALUE, SEX_JCC_STANDARDIZED AS JCC_STD_COLUMN_VALUE,  COUNT(*) AS RECORD_CNT FROM LOAD.LD_PRETRIAL_INDIVIDUAL GROUP BY SEX, SEX_JCC_STANDARDIZED
) AS a
ORDER BY
     SOURCE_FILE_NAME,
     LOAD_COLUMN,
     CASE
          WHEN Load_Column LIKE '%_CHARGE_CODE' THEN 'CHARGE_CODE'
          WHEN Load_Column LIKE '%_CHARGE_LEVEL' THEN 'CHARGE_LEVEL'
          ELSE Load_Column
     END,
     JCC_STANDARDIZED_COLUMN,
     COLUMN_VALUE,
     JCC_STD_COLUMN_VALUE,
     RECORD_CNT;

/*********************************************************/ 
/***** Script to get unstandardized to standardized mappings *****/
/*********************************************************/ 


SELECT *
FROM PUBLIC.PRETRIAL_JCC_STD_VALUES
WHERE JCC_STD_COLUMN_VALUE IS NOT NULL 
ORDER BY
     SOURCE_FILE_NAME,
     LOAD_COLUMN,
     JCC_STANDARDIZED_COLUMN,
     RECORD_CNT DESC,
     COLUMN_VALUE,
     JCC_STD_COLUMN_VALUE;

