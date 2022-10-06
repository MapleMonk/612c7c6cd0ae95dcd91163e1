{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.area_classification_target as select mm.Date, mm.Area, mm.Region, mm.MTD_Sales, mm.Revenue_target, mm.MTD_Eggs_Sold, mm.no_of_days, mm.days_in_month, mm.collections_target, mm.mtd_collections from (select sku.date, sku.area_c as area, bgt.region, (sku.mtd_net_sales-rs.mtd_amount_return) as mtd_sales ,(sku2.mtd_eggs_sold-rs2.mtd_eggs_returned) as mtd_eggs_sold ,bgt.MTD_TARGET_oct::FLOAT as revenue_target ,bgt.COLLECTIONS_TARGET_oct::FLOAT as collections_target ,datediff(day,DATE_TRUNC(\'month\', cast(timestampadd(minute,660,current_date()) as date)), sku.date)+1 no_of_days ,datediff(\'day\',date_trunc(\'month\',cast(timestampadd(minute,660,current_date()) as date)), last_day(cast(timestampadd(minute,660,current_date()) as date),\'month\'))+1 days_in_month ,cs.mtd_collections, row_number() over (partition by sku.date, sku.area_c order by sku.date, sku.area_c) rownumber from (select date, sum(mtd_net_sales) as mtd_net_sales, area_classification as area_c from maplemonk.sales_summary group by date, area_c) sku join maplemonk.target_region_wise_target_plan bgt on lower(bgt.area_classification) = lower(sku.area_c) join (select date, eggs_sold, mtd_eggs_sold, area_classification as area_classification from maplemonk.eggs_sold_summary) sku2 on lower(sku.area_c) = lower(sku2.area_classification) and sku.date = sku2.date join (select date, collections, mtd_collections, area_classification as area_classification from maplemonk.Collection_Summary) cs on lower(cs.area_classification) = lower(sku.area_c) and sku.date = cs.date join (select date, mtd_amount_return, area_classification as area_classification from maplemonk.returnamount_Summary) rs on lower(rs.area_classification) = lower(sku.area_c) and sku.date = rs.date join (select date, mtd_eggs_returned, area_classification as area_classification from maplemonk.return_Summary) rs2 on lower(rs2.area_classification) = lower(sku.area_c) and sku.date = rs2.date WHERE sku.date >= TO_DATE(DATE_TRUNC(\'month\', dateadd(\'day\',-1,cast(timestampadd(minute,660,current_date()) as date)))) AND sku.date <= dateadd(\'day\',-1,cast(timestampadd(minute, 660, getdate()) as date)) ) mm where mm.rownumber = 1 ; create or replace table eggozdb.maplemonk.parent_retailer_target as SELECT bgt.parent as parent_retailer_name, bgt.classification as area, sum(sku.net_sales) AS mtd_sales, sum(sku.eggs_sold) as mtd_eggs_sold, replace(bgt.\"Target-Oct\",\',\',\'\')::FLOAT as revenue_target ,datediff(day,DATE_TRUNC(\'month\',cast(timestampadd(minute,660,current_date()) as date)), dateadd(day,\'-1\',cast(timestampadd(minute, 660, getdate()) as date)))+1 no_of_days ,datediff(\'day\',date_trunc(\'month\',cast(timestampadd(minute,660,current_date()) as date)), last_day(cast(timestampadd(minute,660,current_date()) as date),\'month\'))+1 days_in_month FROM eggozdb.maplemonk.target_mt_target bgt left join (select case when area in (\'NCR-MT\',\'NCR-OF-MT\',\'NCR-ON-MT\') then \'NCR-MT\' when area in (\'Bangalore-MT\', \'Bangalore-ON-MT\', \'Bangalore-OF-MT\') then \'Bangalore-MT\' when area in (\'UP-MT\' , \'UP-ON-MT\', \'UP-OF-MT\') then \'UP-MT\' when area in (\'MP-MT\', \'MP-ON-MT\', \'MP-OF-MT\') then \'MP-MT\' when area in (\'East-MT\',\'East-ON-MT\',\'East-OF-MT\') then \'East-MT\' else area end as area, parent_retailer_name, sum(net_sales) as net_sales, sum(eggs_sold) as eggs_sold from eggozdb.maplemonk.summary_reporting_table_beat_retailer where date >= TO_DATE(DATE_TRUNC(\'month\', dateadd(\'day\',-1,cast(timestampadd(minute,660,current_date()) as date)))) and date <= dateadd(\'day\',-1,cast(timestampadd(minute, 660, getdate()) as date)) group by area, parent_retailer_name )sku on lower(bgt.Parent) = lower(sku.parent_retailer_name) and lower(bgt.classification) = lower(sku.area) GROUP BY bgt.parent, bgt.classification, replace(bgt.\"Target-Oct\",\',\',\'\')::FLOAT ; create or replace table eggozdb.maplemonk.beat_jse_target as SELECT sku.beat_number_original, bgt.\"JSE\", sku.area, bgt.\"SO\", tt.total_onboarded, tt1.new_onboarded, tt2.billed_retailers, replace(bgt.Revenue_oct_target,\',\',\'\')::FLOAT as revenue_target, replace(bgt.Collections_oct_target,\',\',\'\')::FLOAT as collections_target, sum(sku.net_sales) AS mtd_sales, sum(sku.collections) as mtd_collections, sum(sku.eggs_sold) as mtd_eggs_sold, datediff(day,DATE_TRUNC(\'month\', cast(timestampadd(minute,660,current_date()) as date)), dateadd(day,\'-1\',cast(timestampadd(minute, 660, getdate()) as date)))+1 no_of_days, datediff(\'day\',date_trunc(\'month\',cast(timestampadd(minute,660,current_date()) as date)), last_day(cast(timestampadd(minute,660,current_date()) as date),\'month\'))+1 days_in_month FROM maplemonk.summary_reporting_table_beat_retailer sku join maplemonk.target_jse_gt bgt on lower(bgt.beat_number_original) = lower(sku.beat_number_original) and lower(sku.area) = lower(bgt.City) left join (select count(code) as total_onboarded, beat_number, area_classification from eggozdb.maplemonk.my_sql_retailer_retailer where lower(onboarding_status) = \'onboarded\' group by beat_number, area_classification) tt on sku.beat_number_original = tt.beat_number and sku.area = tt.area_classification left join (select count(code) as new_onboarded, beat_number, area_classification from eggozdb.maplemonk.my_sql_retailer_retailer where lower(onboarding_status) = \'onboarded\' and cast(timestampadd(minute, 660, onboarding_date) as date) between TO_DATE(DATE_TRUNC(\'month\', cast(timestampadd(minute,660,current_date()) as date))) and cast(timestampadd(minute, 660, getdate()) as date) group by area_classification, beat_number) tt1 on sku.beat_number_original = tt1.beat_number and sku.area = tt1.area_classification left join (select count(distinct(rr.code)) as billed_retailers, rr.beat_number as beat_number_original, rr.area_classification from eggozdb.maplemonk.my_sql_order_order oo join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id where oo.is_trial <> True and lower(oo.status) = \'delivered\' and cast(timestampadd(minute,660,oo.delivery_date) as date) between TO_DATE(DATE_TRUNC(\'month\', cast(timestampadd(minute,660,current_date()) as date))) and cast(timestampadd(minute, 660, getdate()) as date) group by rr.beat_number, rr.area_classification) tt2 on sku.beat_number_original = tt2.beat_number_original and sku.area = tt2.area_classification WHERE sku.date >= TO_DATE(DATE_TRUNC(\'month\', cast(timestampadd(minute,660,current_date()) as date))) AND sku.date <= cast(timestampadd(minute, 660, getdate()) as date) GROUP BY sku.beat_number_original, bgt.JSE, bgt.SO, sku.area, replace(bgt.Revenue_oct_target,\',\',\'\')::FLOAT, replace(bgt.Collections_oct_target,\',\',\'\')::FLOAT, tt.total_onboarded, tt1.new_onboarded, tt2.billed_retailers ;",
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
                        