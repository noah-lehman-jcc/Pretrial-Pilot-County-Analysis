---------------------------------------------------------------------------------------------------------------------------
---- Tuolumne -----
---------------------------------------------------------------------------------------------------------------------------

SELECT DIM_COUNTY_KEY, DIM_INDIVIDUAL_KEY, DIM_ARREST_DATE_KEY, DIM_BOOKING_DATE_KEY, DIM_RELEASE_DATE_KEY, DIM_BOOKING_DETAIL_KEY, DIM_CHARGE_KEY, BOOKING_KEY, BOOKING_CHARGE_KEY
FROM "55_TUOLUMNE-DWH_PROD".SECURE_SHARE.FACT_BOOKING_CHARGE;

-- tuolumne just has nothing in fact_booking_charge. 
-- looked in county tenant and booking charge tables are filled out in load through datastore, then goes blank in data warehouse. ??? 
-- emailed Correna and Preston 4/26/22

---------------------------------------------------------------------------------------------------------------------------
---- Sacramento -----
---------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM "34_SACRAMENTO-DWH_PROD".SECURE_SHARE.FACT_BOOKING_CHARGE;

-- fact_booking_charge is populated in Sac
