{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE EGGOZDB.MAPLEMONK.RETAILER_COHORT AS SELECT X.*, MIN(X.DATE::DATE) OVER(PARTITION BY X.RETAILER_ID) AS ACQ_DATE, DATEDIFF(MONTH,MIN(X.DATE::DATE) OVER(PARTITION BY X.RETAILER_ID),X.DATE::DATE) AS AGE_MONTHS, DATE_TRUNC(MONTH,MIN(X.DATE::DATE) OVER(PARTITION BY X.RETAILER_ID)) AS ACQ_MONTHYEAR_DATE, TO_VARCHAR(TO_DATE(MIN(X.DATE::DATE) OVER(PARTITION BY X.RETAILER_ID)),\'YYYY-MM\') AS \"ACQ-MONTH-YEAR\", DATE_TRUNC(MONTH,X.DATE::DATE) AS MONTHYEAR_DATE, TO_VARCHAR(TO_DATE(X.DATE::DATE),\'YYYY-MM\') AS \"MONTH-YEAR\" FROM (SELECT O.RETAILER_ID, date(timestampadd(minute,330,O.DATE)) AS DATE, O.ID, O.ORDER_PRICE_AMOUNT, R.AREA_CLASSIFICATION, R.code FROM EGGOZDB.MAPLEMONK.MY_SQL_ORDER_ORDER O LEFT JOIN EGGOZDB.MAPLEMONK.MY_SQL_RETAILER_RETAILER R ON O.RETAILER_ID = R.ID WHERE O.RETAILER_ID IS NOT NULL AND O.DATE::DATE>=\'2020-05-01\' AND O.STATUS IN (\'delivered\', \'completed\') AND O.IS_TRIAL = \'FALSE\' AND O.ORDER_BRAND_TYPE = \'branded\') X",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        