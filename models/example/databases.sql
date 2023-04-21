{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.area_classification_target as select ra.date, rt.area, rt.region, rt.revenue_target, rt.collection_target as collections_target, (ra.mtd_net_sales - ra.mtd_amount_return) as mtd_sales, (ra.mtd_eggs_sold - ra.mtd_eggs_returned) as mtd_eggs_sold, ra.mtd_collections, ra.mtd_eggs_replaced, ra.mtd_eggs_returned, max(ra.date) over (partition by month(ra.date), rt.area) - min(ra.date) over (partition by month(ra.date), rt.area) + 1 as no_of_days, datediff(\'day\',date_trunc(\'month\',ra.date), last_day(ra.date,\'month\'))+1 days_in_month from eggozdb.maplemonk.bi_region_wise_target rt right join ( select t1.date, t2.area_classification, t1.mtd_net_sales, t2.mtd_eggs_sold, t3.mtd_collections, t4.mtd_amount_return, t5.mtd_eggs_replaced, t6.mtd_eggs_returned from eggozdb.maplemonk.sales_summary t1 full outer join eggozdb.maplemonk.eggs_sold_summary t2 on t2.date = t1.date and t2.area_classification = t1.area_classification full outer join eggozdb.maplemonk.collection_summary t3 on t3.date = t1.date and t3.area_classification = t1.area_classification full outer join eggozdb.maplemonk.returnamount_summary t4 on t4.date = t1.date and t4.area_classification = t1.area_classification full outer join eggozdb.maplemonk.replacement_summary t5 on t5.date = t1.date and t5.area_classification = t1.area_classification full outer join eggozdb.maplemonk.return_summary t6 on t6.date = t1.date and t6.area_classification = t1.area_classification ) ra on lower(rt.area) = lower(ra.area_classification) and rt.month = month(ra.date) and rt.year = year(ra.date) where year(ra.date) >=2023 and ra.date< cast(timestampadd(minute, 660, getdate()) as date) and rt.area is not null ; create or replace table eggozdb.maplemonk.parent_retailer_target as select distinct tt.date, tt.parent_retailer_name, tt.area , sum(tt.net_sales) over (partition by tt.parent_retailer_name, tt.area, year(tt.date), month(tt.date) order by year(tt.date), month(tt.date), tt.date) - sum(tt.amount_return) over (partition by tt.parent_retailer_name, tt.area, year(tt.date), month(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_sales , sum(tt.eggs_sold) over (partition by tt.parent_retailer_name, tt.area, year(tt.date), month(tt.date) order by year(tt.date), month(tt.date), tt.date) - sum(tt.eggs_return) over (partition by tt.parent_retailer_name, tt.area, year(tt.date), month(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_eggs_sold , sum(tt.eggs_return) over (partition by tt.parent_retailer_name, tt.area, year(tt.date), month(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_eggs_returned , sum(tt.eggs_replaced) over (partition by tt.parent_retailer_name, tt.area, year(tt.date), month(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_eggs_replaced , pt.revenue_target , datediff(\'day\',date_trunc(\'month\',tt.date),tt.date)+1 no_of_days , datediff(\'day\',date_trunc(\'month\',tt.date), last_day(tt.date,\'month\'))+1 days_in_month from ( select t1.date, t1.area_classification area, t1.parent_name parent_retailer_name, ifnull(t2.net_sales,0) net_sales, ifnull(t2.eggs_sold,0) eggs_sold, ifnull(t2.eggs_replaced,0) eggs_replaced, ifnull(t2.eggs_return,0) eggs_return, ifnull(t2.amount_return,0) amount_return from (select * from eggozdb.maplemonk.date_area_parent_dim where year(date)>=2023 and date < cast(timestampadd(minute, 660, getdate()) as date)) t1 left join ( select date, area, parent_retailer_name, sum(net_sales) net_sales, sum(eggs_sold) eggs_sold, sum(eggs_replaced) eggs_replaced, sum(eggs_return) eggs_return, sum(amount_return) amount_return from eggozdb.maplemonk.summary_reporting_table_beat_retailer group by date, area, parent_retailer_name) t2 on t1.date = t2.date and lower(t1.parent_name) = lower(t2.parent_retailer_name) and lower(t1.area_classification) = lower(t2.area) ) tt left join eggozdb.maplemonk.bi_parent_wise_target pt on pt.year = year(tt.date) and pt.month = month(tt.date) and lower(pt.area_classification) = lower(tt.area) and lower(pt.parent) = lower(tt.parent_retailer_name) ; create or replace table eggozdb.maplemonk.beat_level_sales_summary as select tt.date, tt.area_classification, tt.beat_number, tt.revenue, sum(tt.revenue) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_revenue, tt.eggs_sold, sum(tt.eggs_sold) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_eggs_sold, tt.collections, sum(tt.collections) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_collections, tt.eggs_replaced, sum(tt.eggs_replaced) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_eggs_replaced, tt.eggs_promo, sum(tt.eggs_promo) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_eggs_promo, tt.eggs_return, sum(tt.eggs_return) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_eggs_return, tt.amount_return, sum(tt.amount_return) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_amount_return, tt.active_retailers, sum(tt.active_retailers) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_active_retailers, tt.billing_retailers, sum(tt.billing_retailers) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_billing_retailers, sum(tt.mtd_unique_billing_retailers) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_unique_billing_retailers, tt.retailers_onboarded, sum(tt.retailers_onboarded) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_retailers_onboarded, datediff(\'day\',date_trunc(\'month\',tt.date), last_day(tt.date,\'month\'))+1 days_in_month, max(tt.date) over (partition by month(tt.date)) - min(tt.date) over (partition by month(tt.date)) + 1 as no_of_days, tt.total_onboarded_retailers, (sum(case when tt.active_retailers>4 then tt.active_retailers end) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date))/nullifzero(sum(case when tt.active_retailers >4 then tt.total_onboarded_retailers end) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date)) utilization, tt.outlets_touched, sum(tt.outlets_touched) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_outlets_touched, (sum(case when tt.outlets_touched>0 then tt.outlets_touched end) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date))/nullifzero(sum(case when tt.outlets_touched >0 then tt.total_onboarded_retailers end) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date)) soh_utilization, tt.soh_person, tt.zero_billing_retailers, tt.fresh_in_eggs, sum(tt.fresh_in_eggs) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_fresh_in_eggs, tt.out_eggs, sum(tt.out_eggs) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_out_eggs, tt.supply_eggs, sum(tt.supply_eggs) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_supply_eggs from ( select t1.date, t1.area_classification, t1.beat_number, ifnull(t2.revenue,0) revenue, ifnull(t2.eggs_sold,0) eggs_sold, ifnull(t2.collections,0) collections, ifnull(t2.eggs_replaced,0) eggs_replaced, ifnull(t2.eggs_promo,0) eggs_promo, ifnull(t2.eggs_return,0) eggs_return, ifnull(t2.amount_return,0) amount_return, ifnull(t2.active_retailers,0) active_retailers, ifnull(t3.retailers_onboarded,0) retailers_onboarded, ifnull(t4.total_onboarded_retailers,0) total_onboarded_retailers, ifnull(t2.billing_retailers,0) billing_retailers, ifnull(t2.mtd_unique_billing_retailers,0) mtd_unique_billing_retailers, ifnull(t5.outlets_touched,0) outlets_touched, t5.soh_person, ifnull(t6.retailers_count,0) as zero_billing_retailers, ifnull(t7.fresh_in_eggs,0) fresh_in_eggs, ifnull(t7.out_eggs,0) out_eggs, ifnull(t7.supply_eggs,0) supply_eggs from eggozdb.maplemonk.date_area_beat_dim t1 left join ( select ps.date, ps.area_classification, ps.beat_number_original, sum(ps.revenue) revenue, sum(ps.eggs_sold) eggs_sold, sum(ps.collections) collections, sum(ps.eggs_replaced) eggs_replaced, sum(ps.eggs_promo) eggs_promo, sum(ps.eggs_return) eggs_return, sum(ps.amount_return) amount_return, ps2.active_retailers, ps3.billing_retailers, ps4.mtd_unique_billing_retailers from eggozdb.maplemonk.primary_and_secondary ps left join (select date, count(distinct retailer_name) active_retailers, area_classification, beat_number_original from eggozdb.maplemonk.primary_and_secondary where (revenue>0 or eggs_sold>0 or eggs_replaced>0 or eggs_return>0) and retailer_type in (\'Secondary_Retailer\',\'Primary_Retailer\') group by date, area_classification, beat_number_original) ps2 on ps.date = ps2.date and ps.area_classification = ps2.area_classification and ps.beat_number_original = ps2.beat_number_original left join (select date, count(distinct retailer_name) billing_retailers, area_classification, beat_number_original from eggozdb.maplemonk.primary_and_secondary where revenue>0 and retailer_type in (\'Secondary_Retailer\',\'Primary_Retailer\') group by date, area_classification, beat_number_original ) ps3 on ps.date = ps3.date and ps.area_classification = ps3.area_classification and ps.beat_number_original = ps3.beat_number_original left join (select date, count(rank) mtd_unique_billing_retailers, area_classification, beat_number_original from (select date, retailer_name, dense_rank() over (partition by retailer_name, month(date), area_classification, beat_number_original, year(date) order by date) rank, area_classification, beat_number_original from eggozdb.maplemonk.primary_and_secondary where revenue>0 and retailer_type in (\'Secondary_Retailer\',\'Primary_Retailer\') ) where rank=1 group by date, area_classification, beat_number_original ) ps4 on ps.date = ps4.date and ps.area_classification = ps4.area_classification and ps.beat_number_original = ps4.beat_number_original where ps.revenue>0 or ps.eggs_sold>0 or ps.collections>0 or ps.eggs_replaced>0 or ps.eggs_promo>0 or ps.eggs_return>0 or ps.amount_return>0 and ps.retailer_type in (\'Secondary_Retailer\',\'Primary_Retailer\') group by ps.date, ps.area_classification, ps.beat_number_original, ps2.active_retailers, ps3.billing_retailers, ps4.mtd_unique_billing_retailers ) t2 on t1.date = t2.date and t1.area_classification = t2.area_classification and t1.beat_number = t2.beat_number_original left join ( select count(code) retailers_onboarded, area_classification, beat_number, cast(timestampadd(minute, 660, onboarding_date) as date) onboarding_date from eggozdb.maplemonk.my_sql_retailer_retailer where onboarding_status = \'Active\' group by area_classification, beat_number, cast(timestampadd(minute, 660, onboarding_date) as date) ) t3 on t1.date = t3.onboarding_date and t1.area_classification = t3.area_classification and t1.beat_number = t3.beat_number left join ( select count(code) total_onboarded_retailers, area_classification, beat_number from eggozdb.maplemonk.my_sql_retailer_retailer where onboarding_status = \'Active\' group by area_classification, beat_number ) t4 on t4.area_classification = t1.area_classification and t4.beat_number = t1.beat_number left join ( SELECT date, beat_number, no_of_outlets, sum(outlets_touched) outlets_touched, area_classification, soh_person FROM eggozdb.maplemonk.pjp_soh_adherance group by date, beat_number, no_of_outlets, area_classification, soh_person ) t5 on t5.date = t1.date and t5.area_classification = t1.area_classification and t5.beat_number = t1.beat_number left join ( select count(retailer_name) retailers_count, area_classification, beat_number from eggozdb.maplemonk.zero_billing_retailers where recency between 7 and 30 group by area_classification, beat_number union select count(retailer_name) retailers_count, area_classification, beat_number from eggozdb.maplemonk.secondary_zero_billing_retailers where recency between 7 and 30 group by area_classification, beat_number ) t6 on t6.area_classification = t1.area_classification and t6.beat_number = t1.beat_number left join ( select date, beat_number, area as area_classification, sum(fresh_in_eggs) fresh_in_eggs, sum(out_eggs) out_eggs, sum(supply_eggs) supply_eggs from eggozdb.maplemonk.beat_material_kpi group by date, beat_number, area ) t7 on t7.date = t1.date and t7.area_classification = t1.area_classification and t7.beat_number = t1.beat_number ) tt ; create or replace table eggozdb.maplemonk.beat_jse_target as select t1.* from ( select ra.date, rt.area as area_classification, rt.beat_number_original, cast(rt.revenue_target as number) revenue_target, rt.jse, rt.so, rt.tsi, (ra.mtd_revenue - ra.mtd_amount_return) as mtd_sales, (ra.mtd_eggs_sold - ra.mtd_eggs_return) as mtd_eggs_sold, ra.mtd_collections, ra.mtd_eggs_replaced, ra.mtd_eggs_return, ra.mtd_eggs_promo, ra.mtd_active_retailers, ra.active_retailers, ra.mtd_retailers_onboarded, ra.retailers_onboarded, ra.total_onboarded_retailers, ra.utilization, ra.soh_utilization, ra.outlets_touched, ra.mtd_outlets_touched, ra.soh_person, ra.zero_billing_retailers, ra.mtd_fresh_in_eggs, ra.mtd_out_eggs, ra.mtd_supply_eggs, ra.mtd_billing_retailers, ra.mtd_unique_billing_retailers, max(ra.date) over (partition by month(ra.date)) - min(ra.date) over (partition by month(ra.date)) + 1 as no_of_days, datediff(\'day\',date_trunc(\'month\',ra.date), last_day(ra.date,\'month\'))+1 days_in_month from eggozdb.maplemonk.bi_beat_wise_target rt full outer join eggozdb.maplemonk.beat_level_sales_summary ra on lower(rt.area) = lower(ra.area_classification) and rt.month = month(ra.date) and rt.year = year(ra.date) and rt.beat_number_original = ra.beat_number where year(ra.date) >=2023 and ra.date< cast(timestampadd(minute, 660, getdate()) as date) and rt.area is not null ) t1 ;",
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
                        