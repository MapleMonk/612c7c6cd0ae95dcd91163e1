{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.influencer_marketing as select t1.*, div0(t2.sales_sent_to_inf, count(1) over (partition by t1.revenue_code)) sales_sent_to_inf from ( select rvenue_code revenue_code, influencer_name, no_of_followers,type_of_collaboration,concept,insta_id,reel_link, ordeR_name, order_date, gross_sales customer_sales from (select distinct rvenue_code, influencer_name,no_of_followers,type_of_collaboration,concept,insta_id,reel_link from snitch_db.maplemonk.MARKETING_INFLUENCER) a left join (select discount_code, ordeR_name, ORDER_TIMESTAMP::date ordeR_date,customer_flag, sum(gross_sales) gross_sales from snitch_db.maplemonk.fact_items_snitch group by 1,2,3,4) b on a.rvenue_code = b.discount_code ) t1 left join ( select rvenue_code revenue_code, sum(sales_sent_to_inf) sales_sent_to_inf from snitch_db.maplemonk.MARKETING_INFLUENCER a left join (select ordeR_name,order_date, sum(discount) sales_sent_to_inf, sum(discount) discount from snitch_db.maplemonk.unicommerce_fact_items_snitch group by 1,2) b on a.order_code = b.ordeR_name group by 1 ) t2 on t1.revenue_code = t2.revenue_code",
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
                        