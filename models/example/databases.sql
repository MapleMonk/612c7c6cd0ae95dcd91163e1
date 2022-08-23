{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.area_classification_target as select mm.Date, mm.Area, mm.Region, mm.MTD_Sales, mm.Revenue_target, mm.MTD_Eggs_Sold, mm.no_of_days, mm.collections_target, mm.mtd_collections from (select sku.date, sku.area_c as area, bgt.region, sku.mtd_net_sales as mtd_sales ,sku2.mtd_eggs_sold ,bgt.MTD_TARGET_AUG::FLOAT as revenue_target ,bgt.COLLECTIONS_TARGET_AUG::FLOAT as collections_target ,datediff(day,DATE_TRUNC(\'month\', dateadd(\'day\',-1,current_date())), sku.date)+1 no_of_days ,cs.mtd_collections, row_number() over (partition by sku.date, sku.area_c order by sku.date, sku.area_c) rownumber from (select date, sum(mtd_net_sales) as mtd_net_sales, case when area_classification in (\'NCR-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area_classification end as area_c from maplemonk.sales_summary group by date, area_c) sku join maplemonk.target_region_wise_target_plan bgt on lower(bgt.area_classification) = lower(sku.area_c) join maplemonk.eggs_sold_summary sku2 on lower(sku.area_c) = lower(sku2.area_classification) and sku.date = sku2.date join eggozdb.maplemonk.Collection_Summary cs on lower(cs.area_classification) = lower(sku.area_c) and sku.date = cs.date WHERE sku.date >= TO_DATE(DATE_TRUNC(\'month\', dateadd(\'day\',-1,cast(timestampadd(minute,660,current_date()) as date)))) AND sku.date <= dateadd(\'day\',-1,cast(timestampadd(minute, 660, getdate()) as date)) ) mm where mm.rownumber = 1 ;",
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
                        