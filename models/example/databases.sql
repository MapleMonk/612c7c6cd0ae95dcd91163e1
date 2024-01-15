{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.area_classification_target as select ra.date, rt.area, rt.region, rt.revenue_target, rt.collection_target as collections_target, (ra.mtd_net_sales - ra.mtd_amount_return) as mtd_sales, (ra.mtd_eggs_sold - ra.mtd_eggs_returned) as mtd_eggs_sold, ra.mtd_eggs_replaced, ra.mtd_eggs_returned, 0 as mtd_collections, max(ra.date) over (partition by month(ra.date), rt.area) - min(ra.date) over (partition by month(ra.date), rt.area) + 1 as no_of_days, datediff(\'day\',date_trunc(\'month\',ra.date), last_day(ra.date,\'month\'))+1 days_in_month from eggozdb.maplemonk.bi_region_wise_target rt full outer join ( select date, area_classification, sum(revenue) over (partition by area_classification, year(date), month(date) order by date) mtd_net_sales, sum(eggs_sold) over (partition by area_classification, year(date), month(date) order by date) mtd_eggs_sold, sum(sku_return_amount) over (partition by area_classification, year(date), month(date) order by date) mtd_amount_return, sum(eggs_replaced) over (partition by area_classification, year(date), month(date) order by date) mtd_eggs_replaced, sum(eggs_return) over (partition by area_classification, year(date), month(date) order by date) mtd_eggs_returned from (select date, area_classification, sum(ifnull(revenue,0)) revenue, sum(ifnull(eggs_sold,0)) eggs_sold, sum(ifnull(sku_return_amount,0)) sku_return_amount, sum(ifnull(eggs_replaced,0)) eggs_replaced, sum(ifnull(eggs_return,0)) eggs_return from eggozdb.maplemonk.primary_and_secondary_sku where year(date)>=2023 and date <= cast(timestampadd(minute, 660, getdate()) as date) and (lower(sale_type) = \'primary_sale\' or sale_type is null) group by date, area_classification order by date desc) tt ) ra on lower(rt.area) = lower(ra.area_classification) and rt.month = month(ra.date) and rt.year = year(ra.date) where year(ra.date) >=2023 and ra.date< cast(timestampadd(minute, 660, getdate()) as date) and rt.area is not null ; create or replace table eggozdb.maplemonk.parent_retailer_target as select distinct tt.date, tt.parent_retailer_name, tt.area , sum(tt.revenue) over (partition by tt.parent_retailer_name, tt.area, year(tt.date), month(tt.date) order by year(tt.date), month(tt.date), tt.date) - sum(tt.amount_return) over (partition by tt.parent_retailer_name, tt.area, year(tt.date), month(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_sales , sum(tt.eggs_sold) over (partition by tt.parent_retailer_name, tt.area, year(tt.date), month(tt.date) order by year(tt.date), month(tt.date), tt.date) - sum(tt.eggs_return) over (partition by tt.parent_retailer_name, tt.area, year(tt.date), month(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_eggs_sold , sum(tt.eggs_return) over (partition by tt.parent_retailer_name, tt.area, year(tt.date), month(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_eggs_returned , sum(tt.eggs_replaced) over (partition by tt.parent_retailer_name, tt.area, year(tt.date), month(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_eggs_replaced , pt.revenue_target , datediff(\'day\',date_trunc(\'month\',tt.date),tt.date)+1 no_of_days , datediff(\'day\',date_trunc(\'month\',tt.date), last_day(tt.date,\'month\'))+1 days_in_month from ( select t1.date, t1.area_classification area, t1.parent_name parent_retailer_name, ifnull(t2.revenue,0) revenue, ifnull(t2.eggs_sold,0) eggs_sold, ifnull(t2.eggs_replaced,0) eggs_replaced, ifnull(t2.eggs_return,0) eggs_return, ifnull(t2.amount_return,0) amount_return from (select * from eggozdb.maplemonk.date_area_parent_dim where year(date)>=2023 and date < cast(timestampadd(minute, 660, getdate()) as date) and parent_name is not null) t1 left join ( select date, area_classification, parent_retailer_name, sum(ifnull(revenue,0)) revenue, sum(ifnull(eggs_sold,0)) eggs_sold, sum(ifnull(sku_return_amount,0)) amount_return, sum(ifnull(eggs_replaced,0)) eggs_replaced, sum(ifnull(eggs_return,0)) eggs_return from eggozdb.maplemonk.primary_and_secondary_sku where (area_classification like \'%ON-MT\' or area_classification like \'%OF-MT\') and revenue is not null and year(date)>=2023 and date <= cast(timestampadd(minute, 660, getdate()) as date) and lower(sale_type) = \'primary_sale\' group by date, area_classification, parent_retailer_name) t2 on t1.date = t2.date and lower(t1.parent_name) = lower(t2.parent_retailer_name) and lower(t1.area_classification) = lower(t2.area_classification) ) tt left join eggozdb.maplemonk.bi_parent_wise_target pt on pt.year = year(tt.date) and pt.month = month(tt.date) and lower(pt.area_classification) = lower(tt.area) and lower(pt.parent) = lower(tt.parent_retailer_name) ; create or replace table eggozdb.maplemonk.beat_level_sales_summary as select rt.month, rt.year, rt.jse, rt.so, rt.tsi, rt.revenue_target, al.* from (select tt.date, tt.area_classification, tt.beat_number, tt.revenue, sum(tt.revenue) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_revenue, tt.eggs_sold, sum(tt.eggs_sold) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_eggs_sold, tt.collections, sum(tt.collections) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_collections, tt.eggs_replaced, sum(tt.eggs_replaced) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_eggs_replaced, tt.eggs_promo, sum(tt.eggs_promo) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_eggs_promo, tt.eggs_return, sum(tt.eggs_return) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_eggs_return, tt.amount_return, sum(tt.amount_return) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_amount_return, tt.active_retailers, sum(tt.active_retailers) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_active_retailers, tt.billing_retailers, sum(tt.billing_retailers) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_billing_retailers, sum(tt.unique_billing_retailers) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_unique_billing_retailers, sum(tt.unique_active_retailers) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_unique_active_retailers, tt.retailers_onboarded, sum(tt.retailers_onboarded) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_retailers_onboarded, datediff(\'day\',date_trunc(\'month\',tt.date), last_day(tt.date,\'month\'))+1 days_in_month, max(tt.date) over (partition by month(tt.date)) - min(tt.date) over (partition by month(tt.date)) + 1 as no_of_days, tt.total_onboarded_retailers, case when tt.active_retailers>4 then tt.active_retailers/nullifzero(tt.total_onboarded_retailers) end as daily_beat_utilization, case when tt.outlets_touched>0 then tt.outlets_touched/nullifzero(tt.total_onboarded_retailers) end as daily_soh_utilization, (sum(case when tt.active_retailers>4 then tt.active_retailers end) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date))/nullifzero(sum(case when tt.active_retailers >4 then tt.total_onboarded_retailers end) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date)) utilization, tt.outlets_touched, sum(tt.outlets_touched) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_outlets_touched, (sum(case when tt.outlets_touched>0 then tt.outlets_touched end) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date))/nullifzero(sum(case when tt.outlets_touched >0 then tt.total_onboarded_retailers end) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date)) soh_utilization, tt.zero_billing_retailers, tt.fresh_in_eggs, sum(tt.fresh_in_eggs) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_fresh_in_eggs, tt.out_eggs, sum(tt.out_eggs) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_out_eggs, tt.supply_eggs, sum(tt.supply_eggs) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_supply_eggs, tt.\"1_star\", tt.\"2_star\", tt.\"3_star\", tt.\"4_star\", tt.\"5_star\", tt.new_onboarded_revenue, tt.\"1_star_potential\", tt.\"2_star_potential\", tt.\"3_star_potential\", tt.\"4_star_potential\", tt.\"5_star_potential\", tt.darjan_revenue, sum(tt.darjan_revenue) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_darjan_revenue, tt.dealers_onboarded, sum(tt.dealers_onboarded) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_dealers_onboarded, tt.dealer_net_sales, sum(tt.dealer_net_sales) over (partition by tt.area_classification, tt.beat_number, month(tt.date), year(tt.date) order by year(tt.date), month(tt.date), tt.date) mtd_dealer_net_sales from ( select t1.date, t1.area_classification, t1.beat_number, ifnull(t2.revenue,0) revenue, ifnull(t2.eggs_sold,0) eggs_sold, ifnull(t2.collections,0) collections, ifnull(t2.eggs_replaced,0) eggs_replaced, ifnull(t2.eggs_promo,0) eggs_promo, ifnull(t2.eggs_return,0) eggs_return, ifnull(t2.amount_return,0) amount_return, ifnull(t2.active_retailers,0) active_retailers, ifnull(t3.retailers_onboarded,0) retailers_onboarded, ifnull(t4.total_onboarded_retailers,0) total_onboarded_retailers, ifnull(t2.billing_retailers,0) billing_retailers, ifnull(t2.unique_billing_retailers,0) unique_billing_retailers, ifnull(t2.unique_active_retailers,0) unique_active_retailers, ifnull(t5.outlets_touched,0) outlets_touched, ifnull(t6.retailers_count,0) as zero_billing_retailers, ifnull(t7.fresh_in_eggs,0) fresh_in_eggs, ifnull(t7.out_eggs,0) out_eggs, ifnull(t7.supply_eggs,0) supply_eggs, t8.\"1_star\", t8.\"2_star\", t8.\"3_star\", t8.\"4_star\", t8.\"5_star\", ifnull(t9.new_onboarded_revenue,0) new_onboarded_revenue, t10.\"1_star_potential\", t10.\"2_star_potential\", t10.\"3_star_potential\", t10.\"4_star_potential\", t10.\"5_star_potential\", ifnull(t11.darjan_revenue,0) darjan_revenue, ifnull(t12.dealers_onboarded,0) dealers_onboarded, ifnull(t13.dealer_net_sales,0) dealer_net_sales from eggozdb.maplemonk.date_area_beat_dim t1 left join ( select ps.date, ps.area_classification, ps.beat_number_original, sum(ps.revenue) revenue, sum(ps.eggs_sold) eggs_sold, sum(ps.collections) collections, sum(ps.eggs_replaced) eggs_replaced, sum(ps.eggs_promo) eggs_promo, sum(ps.eggs_return) eggs_return, sum(ps.amount_return) amount_return, ps2.active_retailers, ps3.billing_retailers, ps4.unique_billing_retailers, ps5.unique_active_retailers from eggozdb.maplemonk.primary_and_secondary ps left join (select date, count(distinct retailer_name) active_retailers, area_classification, beat_number_original from eggozdb.maplemonk.primary_and_secondary where (revenue>0 or eggs_sold>0 or eggs_replaced>0 or eggs_return>0 or eggs_promo>0) and retailer_type in (\'Secondary_Retailer\',\'Primary_Retailer\') and onboarding_status = \'Active\' group by date, area_classification, beat_number_original ) ps2 on ps.date = ps2.date and ps.area_classification = ps2.area_classification and ps.beat_number_original = ps2.beat_number_original left join (select date, count(distinct retailer_name) billing_retailers, area_classification, beat_number_original from eggozdb.maplemonk.primary_and_secondary where revenue>0 and retailer_type in (\'Secondary_Retailer\',\'Primary_Retailer\') and onboarding_status = \'Active\' group by date, area_classification, beat_number_original ) ps3 on ps.date = ps3.date and ps.area_classification = ps3.area_classification and ps.beat_number_original = ps3.beat_number_original left join (select date, count(rank) unique_billing_retailers, area_classification, beat_number_original from (select date, retailer_name, dense_rank() over (partition by retailer_name, month(date), area_classification, beat_number_original, year(date) order by date) rank, area_classification, beat_number_original from eggozdb.maplemonk.primary_and_secondary where revenue>0 and retailer_type in (\'Secondary_Retailer\',\'Primary_Retailer\') and onboarding_status = \'Active\' ) where rank=1 group by date, area_classification, beat_number_original ) ps4 on ps.date = ps4.date and ps.area_classification = ps4.area_classification and ps.beat_number_original = ps4.beat_number_original left join (select date, count(rank) unique_active_retailers, area_classification, beat_number_original from (select date, retailer_name, dense_rank() over (partition by retailer_name, month(date), area_classification, beat_number_original, year(date) order by date) rank, area_classification, beat_number_original from eggozdb.maplemonk.primary_and_secondary where (revenue>0 or eggs_sold>0 or eggs_replaced>0 or eggs_return>0 or eggs_promo>0) and retailer_type in (\'Secondary_Retailer\',\'Primary_Retailer\') and onboarding_status = \'Active\' ) where rank=1 group by date, area_classification, beat_number_original ) ps5 on ps.date = ps5.date and ps.area_classification = ps5.area_classification and ps.beat_number_original = ps5.beat_number_original where ps.revenue>0 or ps.eggs_sold>0 or ps.collections>0 or ps.eggs_replaced>0 or ps.eggs_promo>0 or ps.eggs_return>0 or ps.amount_return>0 and ps.retailer_type in (\'Secondary_Retailer\',\'Primary_Retailer\') and onboarding_status = \'Active\' group by ps.date, ps.area_classification, ps.beat_number_original, ps2.active_retailers, ps3.billing_retailers, ps4.unique_billing_retailers, ps5.unique_active_retailers ) t2 on t1.date = t2.date and t1.area_classification = t2.area_classification and t1.beat_number = t2.beat_number_original left join ( select count(code) retailers_onboarded, area_classification, beat_number, cast(timestampadd(minute, 660, onboarding_date) as date) onboarding_date from eggozdb.maplemonk.my_sql_retailer_retailer where onboarding_status = \'Active\' group by area_classification, beat_number, cast(timestampadd(minute, 660, onboarding_date) as date) ) t3 on t1.date = t3.onboarding_date and t1.area_classification = t3.area_classification and t1.beat_number = t3.beat_number left join ( select count(code) total_onboarded_retailers, area_classification, beat_number from eggozdb.maplemonk.my_sql_retailer_retailer where onboarding_status = \'Active\' group by area_classification, beat_number ) t4 on t4.area_classification = t1.area_classification and t4.beat_number = t1.beat_number left join ( SELECT cast(timestampadd(minute, 660, entry_date) as date) date, psa.beat_number_original as beat_number, count(distinct psa.retailer_name) outlets_touched, psa.area_classification FROM eggozdb.maplemonk.eggoz_soh psa group by cast(timestampadd(minute, 660, entry_date) as date), psa.beat_number_original, psa.area_classification ) t5 on t5.date = t1.date and t5.area_classification = t1.area_classification and t5.beat_number = t1.beat_number left join ( select count(retailer_name) retailers_count, area_classification, beat_number from eggozdb.maplemonk.zero_billing_retailers where onboarding_status = \'Active\' and recency > 11 and recency < 150 group by area_classification, beat_number union select count(retailer_name) retailers_count, area_classification, beat_number from eggozdb.maplemonk.secondary_zero_billing_retailers where onboarding_status = \'Active\' and recency >11 and recency < 150 group by area_classification, beat_number ) t6 on t6.area_classification = t1.area_classification and t6.beat_number = t1.beat_number left join ( select date, beat_number, area as area_classification, sum(fresh_in_eggs) fresh_in_eggs, sum(out_eggs) out_eggs, sum(supply_eggs) supply_eggs from eggozdb.maplemonk.beat_material_kpi group by date, beat_number, area ) t7 on t7.date = t1.date and t7.area_classification = t1.area_classification and t7.beat_number = t1.beat_number left join ( select date, area, beat_number_original, ifnull(\"1_star\",0) as \"1_star\", ifnull(\"2_star\",0) as \"2_star\", ifnull(\"3_star\",0) as \"3_star\", ifnull(\"4_star\",0) as \"4_star\", ifnull(\"5_star\",0) as \"5_star\" from (select date, area, beat_number_original, \"1\" as \"1_star\", \"2\"as \"2_star\", \"3\" as \"3_star\", \"4\" as \"4_star\", \"5\" as \"5_star\" from (select distinct date_from_parts(year(date_),month(date_),01) date, area, beat_number_original, cluster, count(retailer_id) over (partition by area, beat_number_original, cluster, year(date_), month(date_) order by date_) retailer_count from beat_level_retailer_ranking where date_ >=\'2023-01-01\' ) pivot(sum(retailer_count) for cluster in (1,2,3,4,5)) as p ) ) t8 on month(t8.date) = month(t1.date) and year(t8.date) = year(t1.date) and t8.area = t1.area_classification and t8.beat_number_original = t1.beat_number left join ( select date_from_parts(year(onboarding_date),month(onboarding_date),01) date, sum(revenue) new_onboarded_revenue, area_classification, beat_number_original from eggozdb.maplemonk.primary_and_secondary where onboarding_status = \'Active\' and month(date)=month(onboarding_date) and year(onboarding_date)=year(date) group by area_classification, beat_number_original, month(onboarding_date), year(onboarding_date) ) t9 on month(t9.date)=month(t1.date) and year(t9.date)=year(t1.date) and t9.area_classification=t1.area_classification and t9.beat_number_original=t1.beat_number left join ( select date, area, beat_number_original, \"1\" as \"1_star_potential\", \"2\"as \"2_star_potential\", \"3\" as \"3_star_potential\", \"4\" as \"4_star_potential\", \"5\" as \"5_star_potential\" from (select distinct date_from_parts(year(date_),month(date_),01) date, area, beat_number_original, potential_cluster, count(retailer_id) over (partition by area, beat_number_original, potential_cluster, year(date_), month(date_) order by date_) retailer_count from beat_level_retailer_ranking where date_ >=\'2023-01-01\' ) pivot(sum(retailer_count) for potential_cluster in (1,2,3,4,5)) as p ) t10 on month(t10.date) = month(t1.date) and year(t10.date) = year(t1.date) and t10.area = t1.area_classification and t10.beat_number_original = t1.beat_number left join ( select sum(sales) darjan_revenue, area_classification, beat_number, date from darjan_sales group by date, beat_number, area_classification ) t11 on t11.date = t1.date and t11.area_classification = t1.area_classification and t11.beat_number = t1.beat_number left join ( select count(code) dealers_onboarded, area_classification, beat_number, cast(timestampadd(minute, 660, onboarding_date) as date) onboarding_date from eggozdb.maplemonk.my_sql_retailer_retailer where onboarding_status = \'Active\' and category_id = 10 group by area_classification, beat_number, cast(timestampadd(minute, 660, onboarding_date) as date) ) t12 on t1.date = t12.onboarding_date and t1.area_classification = t12.area_classification and t1.beat_number = t12.beat_number left join ( select sum(ifnull(revenue,0)-ifnull(amount_return,0)) dealer_net_sales, area_classification, beat_number_original, date from eggozdb.maplemonk.primary_and_secondary where retailer_category = \'Dealer\' and revenue is not null group by area_classification, beat_number_original, date ) t13 on t1.date = t13.date and t1.area_classification = t13.area_classification and t1.beat_number = t13.beat_number_original ) tt where tt.date between \'2023-09-01\' and getdate() ) al left join eggozdb.maplemonk.bi_beat_wise_target rt on lower(rt.area) = lower(al.area_classification) and rt.month = month(al.date) and rt.year = year(al.date) and rt.beat_number_original = al.beat_number ; create or replace table eggozdb.maplemonk.beat_jse_target as select ra.date, rt.area as area_classification, rt.beat_number_original, cast(rt.revenue_target as number) revenue_target, rt.jse, rt.so, rt.tsi, rt.jse_email, rt.so_email, rt.tsi_email, rt.jse_id, rt.so_id, rt.tsi_id, ra.revenue, ra.eggs_sold, ra.collections, ra.eggs_replaced, ra.eggs_promo, ra.eggs_return, ra.amount_return, ra.billing_retailers, ra.daily_beat_utilization, ra.daily_soh_utilization, ra.fresh_in_eggs, ra.out_eggs, ra.supply_eggs, ra.darjan_revenue, ra.dealers_onboarded, ra.dealer_net_sales, (ra.mtd_revenue - ra.mtd_amount_return) as mtd_sales, (ra.mtd_eggs_sold - ra.mtd_eggs_return) as mtd_eggs_sold, ra.mtd_collections, ra.mtd_eggs_replaced, ra.mtd_eggs_return, ra.mtd_eggs_promo, ra.mtd_active_retailers, ra.active_retailers, ra.mtd_retailers_onboarded, ra.retailers_onboarded, ra.total_onboarded_retailers, ra.utilization, ra.soh_utilization, ra.outlets_touched, ra.mtd_outlets_touched, ra.zero_billing_retailers, ra.mtd_fresh_in_eggs, ra.mtd_out_eggs, ra.mtd_supply_eggs, ra.mtd_billing_retailers, ra.mtd_unique_billing_retailers, ra.mtd_unique_active_retailers, max(ra.date) over (partition by month(ra.date)) - min(ra.date) over (partition by month(ra.date)) + 1 as no_of_days, datediff(\'day\',date_trunc(\'month\',ra.date), last_day(ra.date,\'month\'))+1 days_in_month, ra.\"1_star\", ra.\"2_star\", ra.\"3_star\", ra.\"4_star\", ra.\"5_star\", ra.new_onboarded_revenue, ra.\"1_star_potential\", ra.\"2_star_potential\", ra.\"3_star_potential\", ra.\"4_star_potential\", ra.\"5_star_potential\", ra.mtd_darjan_revenue, ra.mtd_dealers_onboarded, ra.mtd_dealer_net_sales, count(distinct beat_number_original) over (partition by rt.tsi, area_classification,date) beat_count from eggozdb.maplemonk.bi_beat_wise_target rt full outer join eggozdb.maplemonk.beat_level_sales_summary ra on lower(rt.area) = lower(ra.area_classification) and rt.month = month(ra.date) and rt.year = year(ra.date) and rt.beat_number_original = ra.beat_number where year(ra.date) >=2023 and ra.date< cast(timestampadd(minute, 660, getdate()) as date) and rt.area is not null ; create or replace table eggozdb.maplemonk.retailer_target as with retailer_level_sales_mtd_cte as ( with retailer_level_sales_cte as ( select t1.date, t1.retailer_name, t1.area_classification, t1.beat_number_original as beat_number, sum(ifnull(revenue,0)) revenue, sum(ifnull(eggs_sold,0)) eggs_sold, sum(ifnull(eggs_replaced,0)) eggs_replaced, sum(ifnull(eggs_return,0)) eggs_return, sum(ifnull(amount_return,0)) amount_return, sum(ifnull(eggs_promo,0)) eggs_promo, sum(ifnull(collections,0)) collections, distributor from eggozdb.maplemonk.date_area_retailer_dim t1 left join eggozdb.maplemonk.primary_and_secondary t2 on split_part(t1.retailer_name,\'*\',0) = split_part(t2.retailer_name,\'*\',0) and t1.date = t2.date where split_part(t1.retailer_name,\'*\',0) in (select distinct split_part(retailer_name,\'*\',0) from eggozdb.maplemonk.bi_retailer_wise_target) and t1.date >= \'2023-08-01\' group by 1,2,3,4,12 ) select date, retailer_name, area_classification, beat_number, distributor, sum(revenue) over (partition by retailer_name, month(date), year(date) order by year(date), month(date), date) mtd_revenue, sum(eggs_sold) over (partition by retailer_name, month(date), year(date) order by year(date), month(date), date) mtd_eggs_sold, sum(eggs_replaced) over (partition by retailer_name, month(date), year(date) order by year(date), month(date), date) mtd_eggs_replaced, sum(eggs_return) over (partition by retailer_name, month(date), year(date) order by year(date), month(date), date) mtd_eggs_return, sum(eggs_promo) over (partition by retailer_name, month(date), year(date) order by year(date), month(date), date) mtd_eggs_promo, sum(amount_return) over (partition by retailer_name, month(date), year(date) order by year(date), month(date), date) mtd_amount_return, sum(collections) over (partition by retailer_name, month(date), year(date) order by year(date), month(date), date) mtd_collections from retailer_level_sales_cte order by date desc ) select t1.retailer_name, t1.area as area_classification, t1.beat_number, t1.distributor, t1.region, t1.revenue_target, t2.date, t2.mtd_revenue, t2.mtd_eggs_sold, t2.mtd_eggs_replaced, t2.mtd_eggs_return, t2.mtd_eggs_promo, t2.mtd_amount_return from eggozdb.maplemonk.bi_retailer_wise_target t1 left join (select * from retailer_level_sales_mtd_cte) t2 on split_part(t1.retailer_name,\'*\',0) = split_part(t2.retailer_name,\'*\',0) and t1.month = month(t2.date) and t1.year = year(t2.date) ; create or replace table eggozdb.maplemonk.lead_and_onboarded_target as select coalesce(lt.date, lll.date, rrr.date, ddd.date, ddd_sale.date, ddd_lead.date) date, lt.asm, coalesce(rrr.sales_person, lll.sales_person, lt.sr) retailer_sales_person, coalesce(ddd.sales_person, ddd_sale.sales_person, ddd_lead.sales_person, lt.so) dealer_sales_person, coalesce(lt.area,lll.area,rrr.area, ddd.area, ddd_sale.area, lll.area, ddd_lead.area) area, lt.onboarding_target, lt.dealer_onboarding_target, lt.darjan_revenue_target, rrr.retailer_count, ddd.dealer_count, ddd_sale.dealer_revenue, ddd_lead.dealer_lead_count, lll.lead_count from (select dd.date, ot_dim.sr, ot_dim.so, ot_dim.asm, ot_dim.area, ot_dim.onboarding_target, ot_dim.dealer_onboarding_target, ot_dim.darjan_revenue_target, ot_dim.month, ot_dim.year, ot_dim.so_id, ot_dim.asm_id, ot_dim.sr_id from eggozdb.maplemonk.date_dim dd cross join (select sr, so, asm, area, onboarding_target, dealer_onboarding_target, darjan_revenue_target, month, year, sr_id, so_id, asm_id from eggozdb.maplemonk.bi_onboarding_targets) ot_dim where dd.date between \'2023-12-01\' and getdate() and month(dd.date::date) = ot_dim.month and year(dd.date::date) = ot_dim.year) lt full outer join ( select distinct date, count(distinct store_name) lead_count, sales_person, area, sales_person_id from ( SELECT distinct cast(timestampadd(minute, 660, created_at) as date) date, lrl.store_name, lrl.channel_classification as area, cau.name as sales_person, ssp.id as sales_person_id FROM eggozdb.maplemonk.my_sql_lead_retailerlead as lrl LEFT JOIN eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp ON ssp.id = lrl.sales_person_profile_id LEFT JOIN eggozdb.maplemonk.my_sql_custom_auth_user cau on ssp.user_id=cau.id LEFT JOIN eggozdb.maplemonk.my_sql_retailer_classification rc on lrl.classification = rc.name where cast(timestampadd(minute, 660, created_at) as date) >= \'2023-12-01\' ) tt group by date, sales_person, area, sales_person_id ) lll on lt.date = lll.date and (lower(lt.sr) = lower(lll.sales_person) or lower(lt.asm) = lower(lll.sales_person)) and lt.area = lll.area full outer join ( select distinct date, count(distinct store_name) dealer_lead_count, sales_person, area, sales_person_id from ( SELECT distinct cast(timestampadd(minute, 660, created_at) as date) date, lrl.store_name, lrl.channel_classification as area, cau.name as sales_person, ssp.id as sales_person_id FROM eggozdb.maplemonk.my_sql_lead_dealerlead as lrl LEFT JOIN eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp ON ssp.id = lrl.sales_person_profile_id LEFT JOIN eggozdb.maplemonk.my_sql_custom_auth_user cau on ssp.user_id=cau.id LEFT JOIN eggozdb.maplemonk.my_sql_retailer_classification rc on lrl.classification = rc.name where cast(timestampadd(minute, 660, created_at) as date) >= \'2023-12-01\' ) tt group by date, sales_person, area, sales_person_id ) ddd_lead on lt.date = ddd_lead.date and (lower(lt.so) = lower(ddd_lead.sales_person) or lower(lt.asm) = lower(ddd_lead.sales_person)) and lt.area = ddd_lead.area full outer join ( select distinct date, count(distinct retailer_name) retailer_count, sales_person, area, sales_person_id from ( select cast(timestampadd(minute, 660, rr.onboarding_date) as date) date, code as retailer_name, area_classification as area, cau.name as sales_person, ssp.id as sales_person_id from eggozdb.maplemonk.my_sql_retailer_retailer rr left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp on ssp.id = rr.salespersonprofile_id left join eggozdb.maplemonk.my_sql_custom_auth_user cau on cau.id = ssp.user_id where rr.onboarding_status = \'Active\' and rr.category_id <> 3 and rr.category_id <> 10 and cast(timestampadd(minute, 660, rr.onboarding_date) as date) >= \'2023-12-01\' ) tt group by date, sales_person, area, sales_person_id ) rrr on lt.date = rrr.date and (lower(lt.sr) = lower(rrr.sales_person)) and lt.area = rrr.area full outer join ( select distinct date, count(distinct retailer_name) dealer_count, sales_person, area, sales_person_id from ( select cast(timestampadd(minute, 660, rr.onboarding_date) as date) date, code as retailer_name, area_classification as area, cau.name as sales_person, ssp.id as sales_person_id from eggozdb.maplemonk.my_sql_retailer_retailer rr left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp on ssp.id = rr.salespersonprofile_id left join eggozdb.maplemonk.my_sql_custom_auth_user cau on cau.id = ssp.user_id where rr.onboarding_status = \'Active\' and rr.category_id = 10 and cast(timestampadd(minute, 660, rr.onboarding_date) as date) >= \'2023-12-01\' ) tt group by date, sales_person, area, sales_person_id ) ddd on lt.date = ddd.date and (lower(lt.so) = lower(ddd.sales_person)) and lt.area = ddd.area full outer join ( select distinct date, sum(order_price_amount) dealer_revenue, sales_person, area, sales_person_id from ( select rr.area_classification as area, cast(timestampadd(minute, 660, oo.delivery_date) as date) as date, oo.order_price_amount, rr.code, cau.name as sales_person, ssp.id as sales_person_id from eggozdb.maplemonk.my_sql_order_order oo left join eggozdb.maplemonk.my_sql_retailer_retailer rr on oo.retailer_id = rr.id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp on ssp.id = oo.salesPerson_id left join eggozdb.maplemonk.my_sql_custom_auth_user cau on cau.id = ssp.user_id where rr.category_id = 10 and oo.status in (\'completed\',\'delivered\') and oo.is_trial = 0 and cast(timestampadd(minute, 660, oo.delivery_date) as date) >= \'2023-12-01\' union all select rr.area_classification as area, cast(timestampadd(minute, 660, oo.delivery_date) as date) as date, oo.order_price_amount, rr.code, cau.name as sales_person, ssp.id as sales_person_id from eggozdb.maplemonk.my_sql_distributor_sales_secondaryorder oo left join eggozdb.maplemonk.my_sql_retailer_retailer rr on oo.retailer_id = rr.id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp on ssp.id = oo.salesPerson_id left join eggozdb.maplemonk.my_sql_custom_auth_user cau on cau.id = ssp.user_id where rr.category_id = 10 and oo.status in (\'created\') and cast(timestampadd(minute, 660, oo.delivery_date) as date) >= \'2023-12-01\' ) tt group by date, sales_person, area, sales_person_id ) ddd_sale on lt.date = ddd_sale.date and (lower(lt.so) = lower(ddd_sale.sales_person)) and lt.area = ddd_sale.area where coalesce(lt.date, lll.date, rrr.date, ddd.date, ddd_sale.date, ddd_lead.date) between \'2023-12-01\' and getdate() ;",
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
                        