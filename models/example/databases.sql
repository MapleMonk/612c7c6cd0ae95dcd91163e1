{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.influencer_marketing as select RVENUE_CODE as REVENUE_CODE, INFLUENCER_NAME, order_code as orderName, NO_OF_FOLLOWERS, INSTA_ID, PLATFORM, concept, type_of_collaboration, paid_amount, posted_date, olt1.order_date, olt.discount::int+ifnull(TRY_CAST(paid_amount AS INTEGER),0) as SALES_SENT_TO_INF, customer_sales from (select * from ( select *,row_number() over(partition by RVENUE_CODE order by posted_date desc) as rw from snitch_db.maplemonk.marketing_influencer) where rw = 1) mi left join (select order_name ,order_date,sum(discount) discount from snitch_db.snitch.order_lineitems_fact group by order_name,order_date) olt on olt.order_name::varchar = mi.order_code::varchar left join (select discount_code,order_date ,sum(selling_price) customer_sales from snitch_db.snitch.order_lineitems_fact group by discount_code,order_date) olt1 on replace(lower(olt1.discount_code::varchar),\' \',\'\') = replace(lower(mi.RVENUE_CODE::varchar),\' \',\'\') create or replace table snitch_db.maplemonk.influencer_marketing_test as select RVENUE_CODE as REVENUE_CODE, INFLUENCER_NAME, order_code as orderName, NO_OF_FOLLOWERS, INSTA_ID, PLATFORM, concept, type_of_collaboration, paid_amount, TO_DATE(posted_date::string, \'DD/MM/YYYY\') as posted_date, olt1.order_date, olt1.order_id, olt.discount::int+ifnull(TRY_CAST(paid_amount AS INTEGER),0) as SALES_SENT_TO_INF, customer_sales from (select * from ( select *,row_number() over(partition by RVENUE_CODE order by posted_date desc) as rw from snitch_db.maplemonk.marketing_influencer) where rw = 1) mi left join (select order_name ,order_date,sum(discount) discount from snitch_db.snitch.order_lineitems_fact group by order_name,order_date) olt on olt.order_name::varchar = mi.order_code::varchar left join (select discount_code,order_id,order_date ,sum(selling_price) customer_sales from snitch_db.snitch.order_lineitems_fact group by discount_code,order_date,order_id) olt1 on replace(lower(olt1.discount_code::varchar),\' \',\'\') = replace(lower(mi.RVENUE_CODE::varchar),\' \',\'\')",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from snitch_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        