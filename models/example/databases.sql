{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.influencer_marketing as select c.influencer_name, c.rvenue_code, c.sales_sent_to_inf, d.gross_sales customer_sales, c.order_name, c.order_date from (select influencer_name, rvenue_code, order_name, order_date, sum(sales_sent_to_inf) sales_sent_to_inf from snitch_db.maplemonk.MARKETING_INFLUENCER a left join (select ordeR_name,order_date, sum(discount) sales_sent_to_inf, sum(discount) discount from snitch_db.maplemonk.unicommerce_fact_items_snitch group by 1,2) b on a.order_code = b.ordeR_name group by 1,2,3,4 ) c left join (select rvenue_code, gross_sales from (select distinct rvenue_code from snitch_db.maplemonk.MARKETING_INFLUENCER) a left join (select discount_code, sum(gross_sales) gross_sales from snitch_db.maplemonk.fact_items_snitch group by 1) b on a.rvenue_code = b.discount_code ) d on c.rvenue_code = d.rvenue_code",
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
                        