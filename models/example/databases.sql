{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE EGGOZDB.MAPLEMONK.RETAILER_COHORT AS SELECT X.*, X.onboarding_date::DATE AS ACQ_DATE, DATEDIFF(MONTH,X.onboarding_date::DATE,X.DATE::DATE) AS AGE_MONTHS, DATE_TRUNC(MONTH,X.onboarding_date::DATE) AS ACQ_MONTHYEAR_DATE, TO_VARCHAR(TO_DATE(X.onboarding_date::DATE),\'YYYY-MM\') AS \"ACQ-MONTH-YEAR\", DATE_TRUNC(MONTH,X.DATE::DATE) AS MONTHYEAR_DATE, TO_VARCHAR(TO_DATE(X.DATE::DATE),\'YYYY-MM\') AS \"MONTH-YEAR\", DATE_TRUNC(MONTH,X.first_order_date::DATE) AS FOD_MONTHYEAR_DATE, TO_VARCHAR(TO_DATE(X.first_order_date::DATE),\'YYYY-MM\') AS \"FOD-MONTH-YEAR\", case when X.code in (select distinct mm.retailer_name from (select date, retailer_name, area_classification, beat_number_original, onboarding_date, onboarding_status, retailer_id, distributor_id, category_id, city_id, retailer_category, parent_retailer_name, min(date) over (partition by retailer_name order by date) first_order_date, sum(revenue) over (partition by date, retailer_name order by date) revenue, sum(eggs_sold) over (partition by date, retailer_name order by date) eggs_sold, retailer_type, distributor from EGGOZDB.MAPLEMONK.primary_and_secondary where revenue is not null and (revenue>0 or eggs_sold>0) ) mm where month(mm.date)=month(current_date) and year(mm.date)=year(current_date)) then X.code else \'null\' end as current_month_activity FROM (SELECT O.RETAILER_ID, O.retailer_category, O.date AS DATE, O.retailer_type, O.onboarding_date, O.first_order_date, O.revenue order_price_amount, R.AREA_CLASSIFICATION, R.code, rc.name classification_name FROM (select date, retailer_name, area_classification, beat_number_original, onboarding_date, onboarding_status, retailer_id, distributor_id, category_id, city_id, retailer_category, parent_retailer_name, min(date) over (partition by retailer_name order by date) first_order_date, sum(revenue) over (partition by date, retailer_name order by date) revenue, sum(eggs_sold) over (partition by date, retailer_name order by date) eggs_sold, retailer_type, distributor from EGGOZDB.MAPLEMONK.primary_and_secondary where revenue is not null and (revenue>0 or eggs_sold>0) ) O LEFT JOIN EGGOZDB.MAPLEMONK.MY_SQL_RETAILER_RETAILER R ON O.RETAILER_ID = R.ID left join eggozdb.Maplemonk.my_sql_retailer_classification rc ON r.classification_id = rc.id WHERE O.RETAILER_ID IS NOT NULL AND o.retailer_id<>7518 and O.DATE::DATE>=\'2020-05-01\') X order by X.date desc ; create or replace table eggozdb.Maplemonk.retailer_list AS select rr2.code as distributor, ret.* from (select rr.code as party_name, rcc.name as retailer_category, rr.area_classification, rc.name as classification, rrslab.number as commission_slab, cau.name as sales_person, cast(timestampadd(minute,330,rr.onboarding_date) as date) as onboarding_date, rr.onboarding_status, rr.billing_name_of_shop as Entity_name,rr.GSTIN as GSTIN, rr.phone_no, rr.beat_number, rrpc.number as payment_cycle, rrp.name as parent_name, rr.id as retailer_id, rr.distributor_id, rr.latitude, rr.longitude, rr.accuracy, qq.name as Sub_category, mm.cluster_name, caa.address_name, caa.building_address, caa.street_address, caa.billing_city, caa.landmark, caa.pinCode, caa.ecommerce_sector_char, caa.society_name, bc.city_name from eggozdb.maplemonk.my_sql_retailer_retailer rr left join eggozdb.maplemonk.my_sql_retailer_customer_category rcc on rcc.id = rr.category_id left join eggozdb.maplemonk.my_sql_retailer_commissionslab rcl on rcl.id = rr.commission_slab_id left join eggozdb.maplemonk.my_sql_retailer_classification rc on rc.id = rr.classification_id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp on ssp.id = rr.salespersonprofile_id left join eggozdb.maplemonk.my_sql_custom_auth_user cau on cau.id = ssp.user_id left join eggozdb.maplemonk.my_sql_retailer_retailerpaymentcycle rrpc on rrpc.id = rr.payment_cycle_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rrp.id = rr.parent_id left join eggozdb.maplemonk.my_sql_retailer_rateslab rrslab on rrslab.retailer_id = rr.id left join eggozdb.maplemonk.my_sql_custom_auth_address caa on rr.billing_address_id = caa.id left join eggozdb.maplemonk.my_sql_base_city bc on bc.id = rr.city_id left join eggozdb.maplemonk.my_sql_retailer_customer_subcategory qq on rr.category_id = qq.id left join eggozdb.maplemonk.my_sql_base_cluster mm on rr.cluster_id = mm.id where rr.id<>7518 ) ret left join eggozdb.maplemonk.my_sql_retailer_retailer rr2 on ret.distributor_id = rr2.id ; create or replace table eggozdb.maplemonk.active_retailers as select date, area_classification, beat_number_original, retailer_name, parent_retailer_name, onboarding_date, distributor, revenue as sale, eggs_sold, eggs_replaced, eggs_return, eggs_promo, retailer_type, onboarding_status from eggozdb.maplemonk.primary_and_secondary where revenue is not null and retailer_id<>7518 and (revenue > 0 or eggs_sold > 0 or eggs_replaced > 0 or eggs_return > 0 or eggs_promo > 0) ; create or replace table eggozdb.maplemonk.billing_retailers as select date, area_classification, beat_number_original, retailer_name, parent_retailer_name, onboarding_date, distributor, revenue as sale, eggs_sold, eggs_replaced, eggs_return, eggs_promo, retailer_type, onboarding_status from eggozdb.maplemonk.primary_and_secondary where revenue is not null and retailer_id<>7518 and (revenue > 0 or eggs_sold > 0) ;",
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
                        