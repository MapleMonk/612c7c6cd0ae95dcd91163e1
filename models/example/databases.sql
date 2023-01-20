{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE EGGOZDB.MAPLEMONK.RETAILER_COHORT AS SELECT X.*, MIN(X.DATE::DATE) OVER(PARTITION BY X.RETAILER_ID) AS ACQ_DATE, DATEDIFF(MONTH,MIN(X.DATE::DATE) OVER(PARTITION BY X.RETAILER_ID),X.DATE::DATE) AS AGE_MONTHS, DATE_TRUNC(MONTH,MIN(X.DATE::DATE) OVER(PARTITION BY X.RETAILER_ID)) AS ACQ_MONTHYEAR_DATE, TO_VARCHAR(TO_DATE(MIN(X.DATE::DATE) OVER(PARTITION BY X.RETAILER_ID)),\'YYYY-MM\') AS \"ACQ-MONTH-YEAR\", DATE_TRUNC(MONTH,X.DATE::DATE) AS MONTHYEAR_DATE, TO_VARCHAR(TO_DATE(X.DATE::DATE),\'YYYY-MM\') AS \"MONTH-YEAR\" FROM (SELECT O.RETAILER_ID, O.date AS DATE, O.revenue order_price_amount , R.AREA_CLASSIFICATION, R.code, rc.name classification_name FROM (select date, retailer_name, area_classification, beat_number_original, onboarding_date, onboarding_status, retailer_id, distributor_id, category_id, city_id, name, parent_retailer_name, sum(revenue) revenue, sum(eggs_sold) eggs_sold, sum(eggs_replaced) eggs_replaced, sum(eggs_return) eggs_return, sum(amount_return) amount_return, sum(eggs_promo) eggs_promo,sum(collections) collections, retailer_type, distributor from EGGOZDB.MAPLEMONK.primary_and_secondary where revenue is not null and retailer_type <> \'Distributor\' group by date, retailer_name, area_classification, beat_number_original, onboarding_date, onboarding_status, retailer_id, distributor_id, category_id, city_id, name, parent_retailer_name, retailer_type, distributor ) O LEFT JOIN EGGOZDB.MAPLEMONK.MY_SQL_RETAILER_RETAILER R ON O.RETAILER_ID = R.ID left join eggozdb.Maplemonk.my_sql_retailer_classification rc ON r.classification_id = rc.id WHERE O.RETAILER_ID IS NOT NULL AND O.DATE::DATE>=\'2020-05-01\') X ; create or replace table eggozdb.Maplemonk.retailer_list AS select rr.code as party_name, rcc.name as category, rr.area_classification, rc.name as classification, rcl.number as commission_slab, cau.name as sales_person, cast(timestampadd(minute,660,rr.onboarding_date) as date) as onboarding_date, rr.onboarding_status, rr.beat_number, rrpc.number as payment_cycle, rrp.name as parent_name from eggozdb.maplemonk.my_sql_retailer_retailer rr left join eggozdb.maplemonk.my_sql_retailer_customer_category rcc on rcc.id = rr.category_id left join eggozdb.maplemonk.my_sql_retailer_commissionslab rcl on rcl.id = rr.commission_slab_id left join eggozdb.maplemonk.my_sql_retailer_classification rc on rc.id = rr.classification_id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp on ssp.id = rr.salespersonprofile_id left join eggozdb.maplemonk.my_sql_custom_auth_user cau on cau.id = ssp.user_id left join eggozdb.maplemonk.my_sql_retailer_retailerpaymentcycle rrpc on rrpc.id = rr.payment_cycle_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rrp.id = rr.parent_id ; create or replace table eggozdb.maplemonk.active_retailers as select date, area_classification, beat_number_original, retailer_name, parent_retailer_name, onboarding_date, distributor, revenue as sale, eggs_sold, eggs_replaced, eggs_return, eggs_promo, retailer_type from eggozdb.maplemonk.primary_and_secondary where revenue is not null and (revenue > 0 or eggs_sold > 0 or eggs_replaced > 0 or eggs_return > 0 or eggs_promo > 0) ;",
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
                        