{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.area_classification_target as select mm.Date, mm.Area, mm.Region, mm.MTD_Sales, mm.Revenue_target, mm.MTD_Eggs_Sold, mm.no_of_days, mm.days_in_month, mm.collections_target, mm.mtd_collections from (select sku.date, sku.area_c as area, bgt.region, sku.mtd_net_sales as mtd_sales ,sku2.mtd_eggs_sold ,bgt.MTD_TARGET_AUG::FLOAT as revenue_target ,bgt.COLLECTIONS_TARGET_AUG::FLOAT as collections_target ,datediff(day,DATE_TRUNC(\'month\', dateadd(\'day\',-1,current_date())), sku.date)+1 no_of_days ,datediff(\'day\',date_trunc(\'month\',current_date()), last_day(current_date(),\'month\'))+1 days_in_month ,cs.mtd_collections, row_number() over (partition by sku.date, sku.area_c order by sku.date, sku.area_c) rownumber from (select date, sum(mtd_net_sales) as mtd_net_sales, case when area_classification in (\'NCR-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area_classification end as area_c from maplemonk.sales_summary group by date, area_c) sku join maplemonk.target_region_wise_target_plan bgt on lower(bgt.area_classification) = lower(sku.area_c) join maplemonk.eggs_sold_summary sku2 on lower(sku.area_c) = lower(sku2.area_classification) and sku.date = sku2.date join eggozdb.maplemonk.Collection_Summary cs on lower(cs.area_classification) = lower(sku.area_c) and sku.date = cs.date WHERE sku.date >= TO_DATE(DATE_TRUNC(\'month\', dateadd(\'day\',-1,cast(timestampadd(minute,660,current_date()) as date)))) AND sku.date <= dateadd(\'day\',-1,cast(timestampadd(minute, 660, getdate()) as date)) ) mm where mm.rownumber = 1 ; create or replace table eggozdb.maplemonk.parent_retailer_target as SELECT bgt.parent as parent_retailer_name, bgt.classification as area, sum(sku.net_sales) AS mtd_sales, sum(sku.eggs_sold) as mtd_eggs_sold, replace(bgt.\"Target-Aug\",\',\',\'\')::FLOAT as revenue_target ,datediff(day, date_trunc(\'month\',current_date()), current_date())+1 no_of_days ,datediff(\'day\',date_trunc(\'month\',current_date()), last_day(current_date(),\'month\'))+1 days_in_month FROM eggozdb.maplemonk.target_mt_target bgt left join (select area, parent_retailer_name, sum(net_sales) as net_sales, sum(eggs_sold) as eggs_sold from eggozdb.maplemonk.summary_reporting_table_beat_retailer_sku where date >= TO_DATE(DATE_TRUNC(\'month\', dateadd(\'day\',-1,current_date()))) and date <= dateadd(\'day\',-1,getdate()) group by area, parent_retailer_name )sku on lower(bgt.Parent) = lower(sku.parent_retailer_name) and lower(bgt.classification) = lower(sku.area) GROUP BY bgt.parent, bgt.classification, replace(bgt.\"Target-Aug\",\',\',\'\')::FLOAT ; create or replace table eggozdb.maplemonk.beat_jse_target as SELECT sku.beat_number_original, bgt.\"JSE\", sku.area, bgt.\"SO\", tt.total_onboarded, tt1.new_onboarded, tt2.billed_retailers, replace(bgt.Revenue_Aug_target,\',\',\'\')::FLOAT as revenue_target, replace(bgt.Collections_Aug_target,\',\',\'\')::FLOAT as collections_target, sum(sku.net_sales) AS mtd_sales, sum(sku.collections) as mtd_collections, sum(sku.eggs_sold) as mtd_eggs_sold, datediff(day, date_trunc(\'month\',current_date()), current_date())+1 no_of_days, datediff(\'day\',date_trunc(\'month\',current_date()), last_day(current_date(),\'month\'))+1 days_in_month FROM maplemonk.summary_reporting_table_beat_retailer sku join maplemonk.target_jse_gt bgt on lower(bgt.beat_number_original) = lower(sku.beat_number_original) and lower(sku.area) = lower(bgt.City) left join (select count(code) as total_onboarded, beat_number, area_classification from eggozdb.maplemonk.my_sql_retailer_retailer where lower(onboarding_status) = \'onboarded\' group by beat_number, area_classification) tt on sku.beat_number_original = tt.beat_number and sku.area = tt.area_classification left join (select count(code) as new_onboarded, beat_number, area_classification from eggozdb.maplemonk.my_sql_retailer_retailer where lower(onboarding_status) = \'onboarded\' and cast(timestampadd(minute, 660, onboarding_date) as date) between TO_DATE(DATE_TRUNC(\'month\', current_date())) and getdate() group by area_classification, beat_number) tt1 on sku.beat_number_original = tt1.beat_number and sku.area = tt1.area_classification left join (select count(distinct(rr.code)) as billed_retailers, rr.beat_number as beat_number_original, rr.area_classification from eggozdb.maplemonk.my_sql_order_order oo join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id where oo.is_trial <> True and lower(oo.status) = \'delivered\' and cast(timestampadd(minute,660,oo.delivery_date) as date) between TO_DATE(DATE_TRUNC(\'month\', current_date())) and getdate() group by rr.beat_number, rr.area_classification) tt2 on sku.beat_number_original = tt2.beat_number_original and sku.area = tt2.area_classification WHERE sku.date >= TO_DATE(DATE_TRUNC(\'month\', current_date())) AND sku.date <= getdate() GROUP BY sku.beat_number_original, bgt.JSE, bgt.SO, sku.area, replace(bgt.Revenue_Aug_target,\',\',\'\')::FLOAT, replace(bgt.Collections_Aug_target,\',\',\'\')::FLOAT, tt.total_onboarded, tt1.new_onboarded, tt2.billed_retailers ;",
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
                        