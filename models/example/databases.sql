{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.zero_billing_Ranking_Final as Select xx.*, yy.\"Rank_Dec\", yy.\"Revenue\", yy.\"Cluster_Dec\", yy.\"Cumulative_Revenue_Contribution\", yy.\"Ranking_Average\", zz.\"Cluster_Jan\", zz.\"Rank_Jan\" from ( select rr.code as retailer_name, rr.area_classification, rr.beat_number, rrp.name as parent_name, lod.last_order_date, rr.onboarding_status, cast(timestampadd(minute, 660, rr.onboarding_date) as date) onboarding_date, datediff(day,cast(timestampadd(minute, 660, rr.onboarding_date) as date),current_date()) days_since_onboarded, sum(oo.order_price_amount) total_bill_amount, count(distinct(oo.id)) as frequency, sum(oo.order_price_amount)/count(distinct(oo.id)) as average_order_amount, datediff(day,lod.last_order_date, cast(timestampadd(minute, 660, current_date()) as date)) as recency from eggozdb.maplemonk.my_sql_order_order oo right join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rrp.id = rr.parent_id left join ( select retailer_id, max(cast(timestampadd(minute, 660, delivery_date) as date)) last_order_date from eggozdb.maplemonk.my_sql_order_order where lower(status) in (\'completed\',\'delivered\') group by retailer_id ) lod on lod.retailer_id = rr.id where lower(oo.status) in (\'delivered\') and rr.distributor_id is null group by rr.code , rr.area_classification , rr.beat_number , rrp.name , rr.onboarding_status , cast(timestampadd(minute, 660, rr.onboarding_date) as date) , lod.last_order_date )xx left join ( SELECT \"Retailer_name\" AS \"Retailer_name\", \"Area\" AS \"Area\", DATE_TRUNC(\'MONTH\', date_) AS \"date_\", sum(score) AS \"Score\", max(\"Rank\") AS \"Rank_Dec\", max(\"Revenue\") AS \"Revenue\", sum(\"Eggs Sold\") AS \"Eggs_Sold\", SUM(\"eggs_replaced\") AS \"Eggs_Replaced\", sum(\"eggs_return\") AS \"Return\", sum(\"landing_price\") AS \"Landing_Price\", sum(cumulative_revenue_contribution) AS \"Cumulative_Revenue_Contribution\", sum(ranking_average)/2 AS \"Ranking_Average\", case when round(sum(ranking_average)/2, 1)*10 = 0 then 1 else (case when sum(cumulative_revenue_contribution) < 0.30 then (case when round(sum(ranking_average)/2, 1)*10 < 5 then round(sum(ranking_average)/2, 1)*10 +1 else round(sum(ranking_average)/2, 1)*10 end) else (case when sum(cumulative_revenue_contribution) > 0.70 then (case when round(sum(ranking_average)/2, 1)*10 > 2 then round(sum(ranking_average)/2, 1)*10 -1 else round(sum(ranking_average)/2, 1)*10 end) else round(sum(ranking_average)/2, 1)*10 END) end) end AS \"Cluster_Dec\" FROM maplemonk.retailer_ranking WHERE date_ >= TO_DATE(\'2022-12-01\') AND date_ < TO_DATE(\'2023-01-01\') and ((\"Area\" not like \'%UB%\')) GROUP BY \"Retailer_name\", \"Area\", DATE_TRUNC(\'MONTH\', date_) )yy on xx.retailer_name =yy.\"Retailer_name\" and xx.area_classification = yy.\"Area\" left join ( SELECT \"Retailer_name\" AS \"Retailer_name\", \"Area\" AS \"Area\", DATE_TRUNC(\'MONTH\', date_) AS \"date_\", sum(score) AS \"Score\", max(\"Rank\") AS \"Rank_Jan\", max(\"Revenue\") AS \"Revenue\", sum(\"Eggs Sold\") AS \"Eggs_Sold\", SUM(\"eggs_replaced\") AS \"Eggs_Replaced\", sum(\"eggs_return\") AS \"Return\", sum(\"landing_price\") AS \"Landing_Price\", sum(cumulative_revenue_contribution) AS \"Cumulative_Revenue_Contribution\", sum(ranking_average)/2 AS \"Ranking_Average\", case when round(sum(ranking_average)/2, 1)*10 = 0 then 1 else (case when sum(cumulative_revenue_contribution) < 0.30 then (case when round(sum(ranking_average)/2, 1)*10 < 5 then round(sum(ranking_average)/2, 1)*10 +1 else round(sum(ranking_average)/2, 1)*10 end) else (case when sum(cumulative_revenue_contribution) > 0.70 then (case when round(sum(ranking_average)/2, 1)*10 > 2 then round(sum(ranking_average)/2, 1)*10 -1 else round(sum(ranking_average)/2, 1)*10 end) else round(sum(ranking_average)/2, 1)*10 END) end) end AS \"Cluster_Jan\" FROM maplemonk.retailer_ranking WHERE date_ >= TO_DATE(\'2022-01-01\') AND date_ < TO_DATE(\'2023-02-01\') and ((\"Area\" not like \'%UB%\')) GROUP BY \"Retailer_name\", \"Area\", DATE_TRUNC(\'MONTH\', date_) )zz on yy.\"Retailer_name\" = zz.\"Retailer_name\" and yy.\"Area\" = zz.\"Area\" ; create or replace table eggozdb.maplemonk.zero_billing_Ranking_Secondary_Final as Select xx.*, yy.\"Rank_Dec\", yy.\"Revenue\", yy.\"Cluster_Dec\", yy.\"Cumulative_Revenue_Contribution\", yy.\"Ranking_Average\", zz.\"Rank_Jan\", zz.\"Cluster_Jan\" from ( select rr2.code as distributor, tt.* from (select rr.code as retailer_name, rr.distributor_id, rr.area_classification, rr.beat_number, rrp.name as parent_name, lod.last_order_date, rr.onboarding_status, cast(timestampadd(minute, 660, rr.onboarding_date) as date) onboarding_date, datediff(day,cast(timestampadd(minute, 660, rr.onboarding_date) as date), cast(timestampadd(minute, 660, current_date()) as date)) days_since_onboarded, sum(oo.order_price_amount) total_bill_amount, count(distinct(oo.id)) as frequency, sum(oo.order_price_amount)/count(distinct(oo.id)) as average_order_amount, datediff(day,lod.last_order_date,cast(timestampadd(minute, 660, current_date()) as date)) as recency from eggozdb.maplemonk.my_sql_distributor_sales_secondaryorder oo join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join ( select retailer_id, max(cast(timestampadd(minute, 660, delivery_date) as date)) last_order_date from eggozdb.maplemonk.my_sql_distributor_sales_secondaryorder group by retailer_id ) lod on lod.retailer_id = rr.id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rrp.id = rr.parent_id where lower(oo.status) in (\'created\') and rr.distributor_id is not null group by rr.code , rr.area_classification , rr.beat_number , rrp.name , cast(timestampadd(minute, 660, rr.last_order_date) as date) , cast(timestampadd(minute, 660, rr.onboarding_date) as date) , rr.distributor_id , rr.onboarding_status , lod.last_order_date ) tt join eggozdb.maplemonk.my_sql_retailer_retailer rr2 on tt.distributor_id = rr2.id )xx left join ( SELECT \"Retailer_name\" AS \"Retailer_name\", \"Area\" AS \"Area\", DATE_TRUNC(\'MONTH\', date_) AS \"date_\", max(\"Rank\") AS \"Rank_Dec\", max(\"Revenue\") AS \"Revenue\", sum(\"Eggs Sold\") AS \"Eggs_Sold\", SUM(\"eggs_replaced\") AS \"Eggs_Replaced\", sum(\"eggs_return\") AS \"Return\", sum(\"landing_price\") AS \"Landing_Price\", sum(cumulative_revenue_contribution) AS \"Cumulative_Revenue_Contribution\", sum(ranking_average)/2 AS \"Ranking_Average\", case when round(sum(ranking_average)/2, 1)*10 = 0 then 1 else (case when sum(cumulative_revenue_contribution) < 0.30 then (case when round(sum(ranking_average)/2, 1)*10 < 5 then round(sum(ranking_average)/2, 1)*10 +1 else round(sum(ranking_average)/2, 1)*10 end) else (case when sum(cumulative_revenue_contribution) > 0.70 then (case when round(sum(ranking_average)/2, 1)*10 > 2 then round(sum(ranking_average)/2, 1)*10 -1 else round(sum(ranking_average)/2, 1)*10 end) else round(sum(ranking_average)/2, 1)*10 END) end) end AS \"Cluster_Dec\" FROM maplemonk.retailer_ranking WHERE date_ >= TO_DATE(\'2022-12-01\') AND date_ < TO_DATE(\'2023-01-01\') and ((\"Area\" not like \'%UB%\')) GROUP BY \"Retailer_name\", \"Area\", DATE_TRUNC(\'MONTH\', date_) )yy on xx.retailer_name =yy.\"Retailer_name\" and xx.area_classification = yy.\"Area\" left join ( SELECT \"Retailer_name\" AS \"Retailer_name\", \"Area\" AS \"Area\", DATE_TRUNC(\'MONTH\', date_) AS \"date_\", max(\"Rank\") AS \"Rank_Jan\", max(\"Revenue\") AS \"Revenue\", sum(\"Eggs Sold\") AS \"Eggs_Sold\", SUM(\"eggs_replaced\") AS \"Eggs_Replaced\", sum(\"eggs_return\") AS \"Return\", sum(\"landing_price\") AS \"Landing_Price\", sum(cumulative_revenue_contribution) AS \"Cumulative_Revenue_Contribution\", sum(ranking_average)/2 AS \"Ranking_Average\", case when round(sum(ranking_average)/2, 1)*10 = 0 then 1 else (case when sum(cumulative_revenue_contribution) < 0.30 then (case when round(sum(ranking_average)/2, 1)*10 < 5 then round(sum(ranking_average)/2, 1)*10 +1 else round(sum(ranking_average)/2, 1)*10 end) else (case when sum(cumulative_revenue_contribution) > 0.70 then (case when round(sum(ranking_average)/2, 1)*10 > 2 then round(sum(ranking_average)/2, 1)*10 -1 else round(sum(ranking_average)/2, 1)*10 end) else round(sum(ranking_average)/2, 1)*10 END) end) end AS \"Cluster_Jan\" FROM maplemonk.retailer_ranking WHERE date_ >= TO_DATE(\'2023-01-01\') AND date_ < TO_DATE(\'2023-02-01\') and ((\"Area\" not like \'%UB%\')) GROUP BY \"Retailer_name\", \"Area\", DATE_TRUNC(\'MONTH\', date_) ) zz on yy.\"Retailer_name\" = zz.\"Retailer_name\" and yy.\"Area\" = zz.\"Area\" ;",
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
                        