{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE eggozdb.maplemonk.Beat_Utilization_ff as SELECT DATE_TRUNC(\'DAY\', date) AS date, beat_number_original AS beat_number_original, operator AS operator, onboarding_status AS onboarding_status, area AS area, retailer_name AS retailer_name, count(DISTINCT retailer_name) AS No_of_Vists, AVG(total_onboarded) AS Total_Onboarded, count(DISTINCT(retailer_name))/avg(total_onboarded) AS Utilization, sum(sold) AS Eggs_Sold, sum(revenue) AS Revenue, sum(collections) AS Collections, sum(today_billing_collections) AS Today_Billing_Collections, sum(replaced) AS Replaced, case when sum(sold) = 0 then 0 else sum(replaced)/sum(sold) end AS Replaced, sum(returned) AS Returned, case when sum(sold) = 0 then 0 else sum(returned)/sum(sold) END AS Returned\" FROM maplemonk.beat_utilization_test WHERE ((onboarding_status = \'Active\')) GROUP BY DATE_TRUNC(\'DAY\', date), beat_number_original, operator, onboarding_status, area, retailer_name ;",
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
                        