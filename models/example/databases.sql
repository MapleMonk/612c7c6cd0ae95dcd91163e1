{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.area_classification_target as SELECT sku.area, bgt.region, (bgt.mtd_target::FLOAT/30)*datediff(day,\'2022-06-01\',getdate()) as mtd_target, sum(sku.net_sales) AS mtd_actuals, sum(sku.net_sales)/sum(sku.eggs_sold) as average_egg_price, sum(sku.eggs_sold) as eggs_sold, sum(sku.net_sales)/datediff(day,\'2022-06-01\',getdate()) as daily_runrate, (sum(sku.net_sales)/datediff(day,\'2022-06-01\',getdate())) * 30 as current_trajectory_forecast, bgt.mtd_target::FLOAT as revenue_target, -1*(bgt.mtd_target::FLOAT - (sum(sku.net_sales)/datediff(day,\'2022-06-01\',getdate()) * 30)) as shortfall_projected, ((bgt.mtd_target::FLOAT - (sum(sku.net_sales)/datediff(day,\'2022-06-01\',getdate()) * 30))/(bgt.mtd_target::FLOAT))*-100 as shortfall_percent, (bgt.mtd_target::FLOAT-sum(sku.net_sales))/(30-datediff(day,\'2022-06-01\',getdate())) as required_run_rate, /*((bgt.mtd_target::FLOAT/30)*datediff(day,\'2022-06-01\',getdate()))-sum(sku.net_sales) as deviation,*/ ((bgt.mtd_target::FLOAT-sum(sku.net_sales))/(30-datediff(day,\'2022-06-01\',getdate())))/(sum(sku.net_sales)/sum(sku.eggs_sold)) as required_des_runrate, sum(sku.eggs_sold)/datediff(day,\'2022-06-01\',getdate()) as Current_DES, ((bgt.mtd_target::FLOAT-sum(sku.net_sales))/(30-datediff(day,\'2022-06-01\',getdate()))-sum(sku.eggs_sold)/datediff(day,\'2022-06-01\',getdate())) AS daily_egg_sale_increase_to_meet_target FROM maplemonk.summary_reporting_table_beat_retailer_sku sku join maplemonk.beat_target_region_wise_target_plan bgt on lower(bgt.area_classification) = lower(sku.area) WHERE sku.date >= TO_DATE(\'2022-06-01\') AND sku.date < getdate() GROUP BY sku.area, bgt.region, bgt.mtd_target::FLOAT",
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
                        