{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE EGGOZDB.MAPLEMONK.RETAILER_COHORT AS SELECT X.*, MIN(X.DATE::DATE) OVER(PARTITION BY X.RETAILER_ID) AS ACQ_DATE, DATEDIFF(MONTH,MIN(X.DATE::DATE) OVER(PARTITION BY X.RETAILER_ID),X.DATE::DATE) AS AGE_MONTHS, DATE_TRUNC(MONTH,MIN(X.DATE::DATE) OVER(PARTITION BY X.RETAILER_ID)) AS ACQ_MONTHYEAR_DATE, TO_VARCHAR(TO_DATE(MIN(X.DATE::DATE) OVER(PARTITION BY X.RETAILER_ID)),\'YYYY-MM\') AS \"ACQ-MONTH-YEAR\", DATE_TRUNC(MONTH,X.DATE::DATE) AS MONTHYEAR_DATE, TO_VARCHAR(TO_DATE(X.DATE::DATE),\'YYYY-MM\') AS \"MONTH-YEAR\" FROM (SELECT O.RETAILER_ID, date(timestampadd(minute,330,O.DATE)) AS DATE, O.ID, O.ORDER_PRICE_AMOUNT, R.AREA_CLASSIFICATION, R.code, rc.name classification_name FROM EGGOZDB.MAPLEMONK.MY_SQL_ORDER_ORDER O LEFT JOIN EGGOZDB.MAPLEMONK.MY_SQL_RETAILER_RETAILER R ON O.RETAILER_ID = R.ID left join eggozdb.Maplemonk.my_sql_retailer_classification rc ON r.classification_id = rc.id WHERE O.RETAILER_ID IS NOT NULL AND O.DATE::DATE>=\'2020-05-01\' AND O.STATUS IN (\'delivered\', \'completed\') AND O.IS_TRIAL = \'FALSE\' AND O.ORDER_BRAND_TYPE = \'branded\') X ; create or replace table eggozdb.Maplemonk.retailer_list AS select rr.code as party_name, rcc.name as category, rr.area_classification, rc.name as classification, rcl.number as commission_slab, cau.name as sales_person, cast(timestampadd(minute,660,rr.onboarding_date) as date) as onboarding_date, rr.onboarding_status, rr.beat_number, rrpc.number as payment_cycle, rrp.name as parent_name from eggozdb.maplemonk.my_sql_retailer_retailer rr left join eggozdb.maplemonk.my_sql_retailer_customer_category rcc on rcc.id = rr.category_id left join eggozdb.maplemonk.my_sql_retailer_commissionslab rcl on rcl.id = rr.commission_slab_id left join eggozdb.maplemonk.my_sql_retailer_classification rc on rc.id = rr.classification_id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp on ssp.id = rr.salespersonprofile_id left join eggozdb.maplemonk.my_sql_custom_auth_user cau on cau.id = ssp.user_id left join eggozdb.maplemonk.my_sql_retailer_retailerpaymentcycle rrpc on rrpc.id = rr.payment_cycle_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rrp.id = rr.parent_id ;",
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
                        