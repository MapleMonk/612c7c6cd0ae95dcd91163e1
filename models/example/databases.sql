{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.area_classification_target as SELECT sku.area, bgt.region, count(distinct(sku.date)) selling_days, sum(sku.net_sales) AS mtd_actuals, sum(sku.net_sales)/count(distinct(sku.date)) as daily_average_revenue, sum(sku.net_sales)/count(distinct(sku.date)) * 30 as current_trajectory_forecast, bgt.mtd_target::FLOAT as target, (bgt.mtd_target::FLOAT/30)*count(distinct(sku.date)) as mtd_target, bgt.mtd_target::FLOAT - sum(sku.net_sales)/count(distinct(sku.date)) * 30 as shortfall_projected, (bgt.mtd_target::FLOAT - sum(sku.net_sales)/count(distinct(sku.date)) * 30 )/(bgt.mtd_target::FLOAT) as deviation, (bgt.mtd_target::FLOAT-sum(sku.net_sales))/(30-count(distinct(sku.date))) as required_run_rate FROM maplemonk.summary_reporting_table_beat_retailer_sku sku join maplemonk.beat_target_region_wise_target_plan bgt on bgt.area_classification = sku.area WHERE sku.date >= TO_DATE(\'2022-06-01\') AND sku.date < getdate() GROUP BY sku.area, bgt.region, bgt.mtd_target::FLOAT;",
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
                        