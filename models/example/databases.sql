{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE eggozdb.maplemonk.Zero_Billing_Ranking as select xx.* , yy.\"Cluster\", yy.\"Score\", yy.\"Rank\", yy.\"Revenue\", yy.\"Cumulative_Revenue_Contribution\", yy.\"Ranking_Average\" from (SELECT retailer_name AS \"retailer_name\", area_classification AS \"area_classification\", beat_number AS \"beat_number\", parent_name AS \"parent_name\", days_since_onboarded AS \"days_since_onboarded\", recency AS \"recency\", frequency AS \"frequency\", total_bill_amount AS \"total_bill_amount\", average_order_amount AS \"average_order_amount\" FROM maplemonk.zero_billing_retailers WHERE ((recency < 32 or days_since_onboarded < 32) AND (recency > 7 or recency is NULL) AND (onboarding_status = \'Active\')) GROUP BY retailer_name, area_classification, beat_number, parent_name, days_since_onboarded, recency, frequency, total_bill_amount, average_order_amount )xx left join ( SELECT \"Retailer_name\" AS \"Retailer_name\", \"Area\" AS \"Area\", DATE_TRUNC(\'MONTH\', date_) AS \"date_\", sum(score) AS \"Score\", max(\"Rank\") AS \"Rank\", max(\"Revenue\") AS \"Revenue\", sum(\"Eggs Sold\") AS \"Eggs_Sold\", SUM(\"eggs_replaced\") AS \"Eggs_Replaced\", sum(\"eggs_return\") AS \"Return\", sum(\"landing_price\") AS \"Landing Price\", sum(cumulative_revenue_contribution) AS \"Cumulative_Revenue_Contribution\", sum(ranking_average)/2 AS \"Ranking_Average\", case when round(sum(ranking_average)/2, 1)*10 = 0 then 1 else (case when sum(cumulative_revenue_contribution) < 0.30 then (case when round(sum(ranking_average)/2, 1)*10 < 5 then round(sum(ranking_average)/2, 1)*10 +1 else round(sum(ranking_average)/2, 1)*10 end) else (case when sum(cumulative_revenue_contribution) > 0.70 then (case when round(sum(ranking_average)/2, 1)*10 > 2 then round(sum(ranking_average)/2, 1)*10 -1 else round(sum(ranking_average)/2, 1)*10 end) else round(sum(ranking_average)/2, 1)*10 END) end) end AS \"Cluster\" FROM maplemonk.retailer_ranking WHERE ((\"Area\" not like \'%UB%\')) GROUP BY \"Retailer_name\", \"Area\", DATE_TRUNC(\'MONTH\', date_) ORDER BY \"Score\" DESC )yy on xx.\"retailer_name\" =yy.\"Retailer_name\" and xx.\"area_classification\" = yy.\"Area\" ;",
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
                        