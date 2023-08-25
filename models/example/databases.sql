{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.marketing_cluster_april_comparison_table as select t1.*, t2.revenue as april_revenue, t2.eggs_sold as april_eggs_sold, t2.eggs_replaced as april_eggs_replaced, t2.days_in_month as april_days_in_month from ( select retailer_id, retailer_name, area_classification, beat_number_original, marketing_cluster, society_name, activity_status, date_from_parts(year(date), month(date), 01) date, sum(revenue) revenue, sum(eggs_sold) eggs_sold, sum(amount_return) amount_return, sum(eggs_replaced) eggs_replaced, onboarding_date, onboarding_status, datediff(\'day\',min(date),MAX(date))+1 as days_in_month from eggozdb.maplemonk.primary_and_secondary where marketing_cluster is not null and date >= \'2023-04-01\' and date < getdate() and onboarding_status = \'Active\' group by retailer_id, month(date), year(date), retailer_name, area_classification, beat_number_original, marketing_cluster, society_name, onboarding_date, onboarding_status, activity_status ) t1 left join ( select retailer_id, retailer_name, area_classification, beat_number_original, marketing_cluster, society_name, date_from_parts(year(date), month(date), 01) date, sum(revenue) revenue, sum(eggs_sold) eggs_sold, sum(eggs_replaced) eggs_replaced, sum(amount_return) amount_return, datediff(\'day\',min(date),MAX(date))+1 as days_in_month from eggozdb.maplemonk.primary_and_secondary where marketing_cluster is not null and month(date) = 04 and year(date) = 2023 and onboarding_status = \'Active\' group by retailer_id, month(date), year(date), retailer_name, area_classification, beat_number_original, marketing_cluster, society_name ) t2 on t1.retailer_id = t2.retailer_id ;",
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
                        