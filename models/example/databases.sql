{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.influencer_marketing as select RVENUE_CODE as REVENUE_CODE, INFLUENCER_NAME, order_code as orderName, NO_OF_FOLLOWERS, INSTA_ID, PLATFORM, concept, type_of_collaboration, paid_amount, posted_date, olt1.order_date, olt.discount::int+ifnull(TRY_CAST(paid_amount AS INTEGER),0) as SALES_SENT_TO_INF, customer_sales from snitch_db.maplemonk.marketing_influencer mi left join (select order_name ,order_date,sum(discount) discount from snitch_db.snitch.order_lineitems_fact group by order_name,order_date) olt on olt.order_name::varchar = mi.order_code::varchar left join (select discount_code,order_date ,sum(mrp) customer_sales from snitch_db.snitch.order_lineitems_fact group by discount_code,order_date) olt1 on replace(lower(olt1.discount_code::varchar),\' \',\'\') = replace(lower(mi.RVENUE_CODE::varchar),\' \',\'\')",
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
                        