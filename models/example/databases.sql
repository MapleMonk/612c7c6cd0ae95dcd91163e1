{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.area_classification_target as select mm.Date, mm.Area, mm.Region, mm.MTD_Sales, mm.Revenue_target, mm.MTD_Eggs_Sold, mm.no_of_days from (select sku.date, sku.area_classification as area, bgt.region, sku.mtd_net_sales as mtd_sales ,sku2.mtd_eggs_sold ,bgt.mtd_target::FLOAT as revenue_target ,datediff(day,DATE_TRUNC(\'month\', current_date()), sku.date)+1 no_of_days, row_number() over (partition by sku.date, sku.area_classification order by sku.date, sku.area_classification) rownumber from maplemonk.sales_summary sku join maplemonk.target_region_wise_target_plan bgt on lower(bgt.area_classification) = lower(sku.area_classification) join maplemonk.eggs_sold_summary sku2 on lower(sku.area_classification) = lower(sku2.area_classification) and sku.date = sku2.date WHERE sku.date >= TO_DATE(DATE_TRUNC(\'month\', current_date())) AND sku.date <= getdate() ) mm where mm.rownumber = 1 ; create or replace table eggozdb.maplemonk.parent_retailer_target as SELECT sku.date, bgt.parent as parent_retailer_name, bgt.classification as area, sum(sku.net_sales) AS mtd_actuals, sum(sku.eggs_sold) as eggs_sold, replace(bgt.\"Target-June\",\',\',\'\')::FLOAT as revenue_target ,datediff(day,DATE_TRUNC(\'month\', current_date()), sku.date)+1 no_of_days FROM eggozdb.maplemonk.target_mt_target bgt left join (select * from eggozdb.maplemonk.summary_reporting_table_beat_retailer_sku where date >= TO_DATE(DATE_TRUNC(\'month\', current_date())) and date <= getdate() )sku on lower(bgt.Parent) = lower(sku.parent_retailer_name) and lower(bgt.classification) = lower(sku.area) GROUP BY bgt.parent, bgt.classification, replace(bgt.\"Target-June\",\',\',\'\')::FLOAT, sku.date ;",
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
                        