{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.area_classification_target as select ra.date, rt.area, rt.region, rt.revenue_target, rt.collection_target as collections_target, (ra.mtd_net_sales - ra.mtd_amount_return) as mtd_sales, (ra.mtd_eggs_sold - ra.mtd_eggs_returned) as mtd_eggs_sold, ra.mtd_collections, ra.mtd_eggs_replaced, ra.mtd_eggs_returned, max(ra.date) over (partition by month(ra.date), rt.area) - min(ra.date) over (partition by month(ra.date), rt.area) + 1 as no_of_days, datediff(\'day\',date_trunc(\'month\',ra.date), last_day(ra.date,\'month\'))+1 days_in_month from eggozdb.maplemonk.bi_region_wise_target rt right join ( select t1.date, t2.area_classification, t1.mtd_net_sales, t2.mtd_eggs_sold, t3.mtd_collections, t4.mtd_amount_return, t5.mtd_eggs_replaced, t6.mtd_eggs_returned from eggozdb.maplemonk.sales_summary t1 full outer join eggozdb.maplemonk.eggs_sold_summary t2 on t2.date = t1.date and t2.area_classification = t1.area_classification full outer join eggozdb.maplemonk.collection_summary t3 on t3.date = t1.date and t3.area_classification = t1.area_classification full outer join eggozdb.maplemonk.returnamount_summary t4 on t4.date = t1.date and t4.area_classification = t1.area_classification full outer join eggozdb.maplemonk.replacement_summary t5 on t5.date = t1.date and t5.area_classification = t1.area_classification full outer join eggozdb.maplemonk.return_summary t6 on t6.date = t1.date and t6.area_classification = t1.area_classification ) ra on lower(rt.area) = lower(ra.area_classification) and rt.month = month(ra.date) and rt.year = year(ra.date) where year(ra.date) >=2023 and ra.date< cast(timestampadd(minute, 660, getdate()) as date) and rt.area is not null ; create or replace table eggozdb.maplemonk.parent_retailer_target as select distinct tt.date, tt.parent_retailer_name, tt.area , sum(tt.net_sales) over (partition by tt.parent_retailer_name, tt.area, year(tt.date), month(tt.date) order by year(tt.date), month(tt.date), tt.date) - sum(tt.amount_return) over (partition by tt.parent_retailer_name, tt.area, year(tt.date), month(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_sales , sum(tt.eggs_sold) over (partition by tt.parent_retailer_name, tt.area, year(tt.date), month(tt.date) order by year(tt.date), month(tt.date), tt.date) - sum(tt.eggs_return) over (partition by tt.parent_retailer_name, tt.area, year(tt.date), month(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_eggs_sold , sum(tt.eggs_return) over (partition by tt.parent_retailer_name, tt.area, year(tt.date), month(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_eggs_returned , sum(tt.eggs_replaced) over (partition by tt.parent_retailer_name, tt.area, year(tt.date), month(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_eggs_replaced , pt.revenue_target , datediff(\'day\',date_trunc(\'month\',tt.date),tt.date)+1 no_of_days , datediff(\'day\',date_trunc(\'month\',tt.date), last_day(tt.date,\'month\'))+1 days_in_month from ( select t1.date, t1.area_classification area, t1.parent_name parent_retailer_name, ifnull(t2.net_sales,0) net_sales, ifnull(t2.eggs_sold,0) eggs_sold, ifnull(t2.eggs_replaced,0) eggs_replaced, ifnull(t2.eggs_return,0) eggs_return, ifnull(t2.amount_return,0) amount_return from (select * from eggozdb.maplemonk.date_area_parent_dim where year(date)>=2023 and date < cast(timestampadd(minute, 660, getdate()) as date)) t1 left join ( select date, area, parent_retailer_name, sum(net_sales) net_sales, sum(eggs_sold) eggs_sold, sum(eggs_replaced) eggs_replaced, sum(eggs_return) eggs_return, sum(amount_return) amount_return from eggozdb.maplemonk.summary_reporting_table_beat_retailer group by date, area, parent_retailer_name) t2 on t1.date = t2.date and lower(t1.parent_name) = lower(t2.parent_retailer_name) and lower(t1.area_classification) = lower(t2.area) ) tt left join eggozdb.maplemonk.bi_parent_wise_target pt on pt.year = year(tt.date) and pt.month = month(tt.date) and lower(pt.area_classification) = lower(tt.area) and lower(pt.parent) = lower(tt.parent_retailer_name) ; create or replace table eggozdb.maplemonk.beat_jse_target as SELECT sku.beat_number_original \"beat number original\", bgt.\"JSE\" JSE, sku.area_classification, bgt.\"SO\" SO, bgt.\"Sales Head\", active_onboarded.active_retailers, active_onboarded.retailers_onboarded new_onboarded, replace(bgt.Revenue_Feb23_target,\',\',\'\')::FLOAT as revenue_target, replace(bgt.Collections_Feb23_target,\',\',\'\')::FLOAT as collections_target, sum(sku.revenue-sku.amount_return) AS mtd_sales, sum(sku.collections) as collections, sum(sku.eggs_sold) as mtd_eggs_sold, datediff(day,DATE_TRUNC(\'month\', cast(timestampadd(minute,330,current_date()) as date)), dateadd(day,\'-1\',cast(timestampadd(minute, 330, getdate()) as date)))+1 no_of_days, datediff(\'day\',date_trunc(\'month\',cast(timestampadd(minute,330,current_date()) as date)), last_day(cast(timestampadd(minute,330,current_date()) as date),\'month\'))+1 days_in_month FROM (select * from eggozdb.maplemonk.primary_and_secondary where date between TO_DATE(DATE_TRUNC(\'month\', dateadd(\'day\',-1,cast(timestampadd(minute,330,current_date()) as date)))) and dateadd(\'day\',-1,cast(timestampadd(minute, 330, getdate()) as date)) ) sku join maplemonk.target_jse_gt bgt on lower(bgt.beat_number_original) = lower(sku.beat_number_original) and lower(sku.area_classification) = lower(bgt.City) left join ( select coalesce(tt.onboarding_month,nn.activity_month) month, coalesce(tt.area_classification,nn.area_classification) area_classification, coalesce(tt.beat_number,nn.beat_number_original) beat_number_original, tt.retailers_onboarded, nn.active_retailers from ( select month(cast(timestampadd(minute, 330, onboarding_date) as date)) onboarding_month, area_classification, beat_number, count(distinct code) retailers_onboarded from eggozdb.maplemonk.my_sql_retailer_retailer where cast(timestampadd(minute, 330, onboarding_date) as date) between TO_DATE(DATE_TRUNC(\'month\', dateadd(\'day\',-1,cast(timestampadd(minute,330,current_date()) as date)))) and dateadd(\'day\',-1,cast(timestampadd(minute, 330, getdate()) as date)) group by month(cast(timestampadd(minute, 330, onboarding_date) as date)), area_classification, beat_number ) tt full outer join ( select month(date) activity_month, count(distinct retailer_name) as active_retailers, area_classification, beat_number_original from eggozdb.maplemonk.active_retailers where date between TO_DATE(DATE_TRUNC(\'month\', dateadd(\'day\',-1,cast(timestampadd(minute,330,current_date()) as date)))) and dateadd(\'day\',-1,cast(timestampadd(minute, 330, getdate()) as date)) group by month(date), area_classification, beat_number_original ) nn on tt.onboarding_month = nn.activity_month and tt.area_classification = nn.area_classification and tt.beat_number = nn.beat_number_original ) active_onboarded on active_onboarded.beat_number_original = sku.beat_number_original and active_onboarded.area_classification = sku.area_classification GROUP BY sku.beat_number_original, bgt.JSE, bgt.SO, bgt.\"Sales Head\", sku.area_classification, replace(bgt.Revenue_Feb23_target,\',\',\'\')::FLOAT, replace(bgt.Collections_Feb23_target,\',\',\'\')::FLOAT, active_onboarded.active_retailers, active_onboarded.retailers_onboarded ;",
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
                        