{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.area_classification_target as select mm.Date, mm.Area, mm.Region, mm.MTD_Sales, mm.Revenue_target, mm.MTD_Eggs_Sold, mm.no_of_days, mm.collections_target, mm.mtd_collections from (select sku.date, sku.area_c as area, bgt.region, sku.mtd_net_sales as mtd_sales ,sku2.mtd_eggs_sold ,bgt.MTD_TARGET_JULY::FLOAT as revenue_target ,bgt.COLLECTIONS_TARGET_JULY::FLOAT as collections_target ,datediff(day,DATE_TRUNC(\'month\', current_date()), sku.date)+1 no_of_days ,cs.mtd_collections, row_number() over (partition by sku.date, sku.area_c order by sku.date, sku.area_c) rownumber from (select date, sum(mtd_net_sales) as mtd_net_sales, case when area_classification in (\'NCR-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area_classification end as area_c from maplemonk.sales_summary group by date, area_c) sku join maplemonk.target_region_wise_target_plan bgt on lower(bgt.area_classification) = lower(sku.area_c) join maplemonk.eggs_sold_summary sku2 on lower(sku.area_c) = lower(sku2.area_classification) and sku.date = sku2.date join eggozdb.maplemonk.Collection_Summary cs on lower(cs.area_classification) = lower(sku.area_c) and sku.date = cs.date WHERE sku.date >= TO_DATE(DATE_TRUNC(\'month\', current_date())) AND sku.date < getdate() ) mm where mm.rownumber = 1 ; create or replace table eggozdb.maplemonk.parent_retailer_target as SELECT bgt.parent as parent_retailer_name, bgt.classification as area, sum(sku.net_sales) AS mtd_sales, sum(sku.eggs_sold) as mtd_eggs_sold, replace(bgt.\"Target-July\",\',\',\'\')::FLOAT as revenue_target ,datediff(day,DATE_TRUNC(\'month\', current_date()), dateadd(day,\'-1\',getdate()))+1 no_of_days FROM eggozdb.maplemonk.target_mt_target bgt left join (select area, parent_retailer_name, sum(net_sales) as net_sales, sum(eggs_sold) as eggs_sold from eggozdb.maplemonk.summary_reporting_table_beat_retailer_sku where date >= TO_DATE(DATE_TRUNC(\'month\', current_date())) and date <= getdate() group by area, parent_retailer_name )sku on lower(bgt.Parent) = lower(sku.parent_retailer_name) and lower(bgt.classification) = lower(sku.area) GROUP BY bgt.parent, bgt.classification, replace(bgt.\"Target-July\",\',\',\'\')::FLOAT ; create or replace table eggozdb.maplemonk.beat_jse_target as SELECT sku.beat_number_original, bgt.\"JSE\", sku.area, bgt.\"SO\", (replace(bgt.Revenue_target,\',\',\'\')::FLOAT/31)*datediff(day,DATE_TRUNC(\'month\', current_date()),getdate()) as mtd_target, sum(sku.net_sales) AS mtd_actuals, sum(sku.collections) as mtd_collections, sum(sku.net_sales)/sum(sku.eggs_sold) as average_egg_price, sum(sku.eggs_sold) as eggs_sold, sum(sku.net_sales)/datediff(day,DATE_TRUNC(\'month\', current_date()),getdate()) as daily_runrate, (sum(sku.net_sales)/datediff(day,DATE_TRUNC(\'month\', current_date()),getdate())) * 31 as current_trajectory_forecast, replace(bgt.Revenue_target,\',\',\'\')::FLOAT as revenue_target, replace(bgt.Collections_target,\',\',\'\')::FLOAT as Collections_target, -1*(replace(bgt.Revenue_target,\',\',\'\')::FLOAT - (sum(sku.net_sales)/datediff(day,DATE_TRUNC(\'month\', current_date()),getdate()) * 31)) as shortfall_projected, ((replace(bgt.Revenue_target,\',\',\'\')::FLOAT - (sum(sku.net_sales)/datediff(day,DATE_TRUNC(\'month\', current_date()),getdate()) * 31))/(replace(bgt.Revenue_target,\',\',\'\')::FLOAT))*-1 as shortfall_percent, (replace(bgt.Revenue_target,\',\',\'\')::FLOAT-sum(sku.net_sales))/(31-datediff(day,DATE_TRUNC(\'month\', current_date()),getdate())) as required_run_rate, ((replace(bgt.Revenue_target,\',\',\'\')::FLOAT-sum(sku.net_sales))/(31-datediff(day,DATE_TRUNC(\'month\', current_date()),getdate())))/(sum(sku.net_sales)/sum(sku.eggs_sold)) as required_des_runrate, sum(sku.eggs_sold)/datediff(day,DATE_TRUNC(\'month\', current_date()),getdate()) as Current_DES, (((replace(bgt.Revenue_target,\',\',\'\')::FLOAT-sum(sku.net_sales))/(31-datediff(day,DATE_TRUNC(\'month\', current_date()),getdate())))/(sum(sku.net_sales)/sum(sku.eggs_sold)))-(sum(sku.eggs_sold)/datediff(day,DATE_TRUNC(\'month\', current_date()),getdate())) AS DES_increase FROM maplemonk.summary_reporting_table_beat_retailer sku join maplemonk.target_jse_gt bgt on lower(bgt.beat_number_original) = lower(sku.beat_number_original) and lower(sku.area) = lower(bgt.City) WHERE sku.date >= TO_DATE(DATE_TRUNC(\'month\', current_date())) AND sku.date <= getdate() GROUP BY sku.beat_number_original, bgt.JSE, bgt.SO, sku.area, replace(bgt.Revenue_target,\',\',\'\')::FLOAT, replace(bgt.Collections_target,\',\',\'\')::FLOAT ;",
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
                        