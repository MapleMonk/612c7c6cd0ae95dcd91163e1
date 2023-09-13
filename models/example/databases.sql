{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.influencer_marketing as with fact_discounts as ( SELECT discount_code, sum(gross_sales) as total_sales, SUM(discount) AS total_discount FROM snitch_db.maplemonk.fact_items_snitch group by discount_code ), influencers as ( WITH tempInfluencers AS ( SELECT rvenue_code, influencer_name, no_of_followers, order_code, reel_link, sum(paid_amount) AS PAID, ROW_NUMBER() OVER (PARTITION BY rvenue_code ORDER BY influencer_name) AS rn FROM snitch_db.maplemonk.MARKETING_INFLUENCER Group by rvenue_code, influencer_name,no_of_followers,order_code,reel_link ) SELECT rvenue_code, influencer_name, no_of_followers,order_code,reel_link, sum(PAID) AS Amount_Paid FROM tempInfluencers WHERE rn = 1 GROUP BY rvenue_code, influencer_name, no_of_followers,order_code,reel_link ), all_order_details as ( SELECT order_date,order_name, sum(mrp) as total_order_value FROM snitch_db.maplemonk.unicommerce_fact_items_snitch group by order_date,order_name ) select order_date, rvenue_code, influencer_name, no_of_followers, discount_code, order_code, order_name, reel_link, coalesce(sum(total_discount),0) as discount,sum(total_order_value) as order_sent_to_inf, coalesce(sum(total_sales),0) as sales, coalesce(sum(Amount_Paid),0) as Paid_to_inf from influencers LEFT JOIN fact_discounts on rvenue_code = discount_code LEFT JOIN all_order_details on order_code = order_name group by 1,2,3,4,5,6,7,8 order by sales desc",
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
                        