{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.truegradient_suggested_sku as select bb.retailer_name, bb.sku, bb.sku_count, rr.id as retailer_id, bb.area_classification, bb.beat_number, rcc.name category, rcs.name as subcategpry, rr.onboarding_status, rr.marketing_cluster, rr.society_name, rr.category_id, rr.sub_category_id, cast(timestampadd(minute, 660, rr.onboarding_date) as date) onboarding_date, bb.total_suggested_packs, bb.suggested_packs_x_days, bb.total_eggs_placed_suggested, ifnull(pss.eggs_sold,0) eggs_sold, ifnull(pss.eggs_replaced,0) eggs_replaced, (ifnull(pss.eggs_sold,0) + ifnull(pss.eggs_replaced,0))/bb.sku_count as sku_placed, bb.experiment_name from ( select month_name_to_number(split_part(experiment_name,\'_\',0)) month, year(b._AIRBYTE_EMITTED_AT) year, b.retailer_name, b.sku, pp.sku_count, b.area_classification, b.beat_number_original as beat_number, total_eggs_placed_suggested, (b.total_eggs_placed_suggested/pp.sku_count) as total_suggested_packs, cast(ceil((b.total_eggs_placed_suggested/pp.sku_count)*6/30) as int) as suggested_packs_x_days, experiment_name from ( select experiment_name, _AIRBYTE_EMITTED_AT, retailer_name, sku, area_classification, beat_number_original, (\"Eggs Sold_new\" + \"Eggs Replaced_new\") as total_eggs_placed_suggested from eggozdb.maplemonk.TRUEGRADIENT_REPLACEMENT_OPTIMIZATION_2 where forecast_from = (select max(forecast_from) from eggozdb.maplemonk.TRUEGRADIENT_REPLACEMENT_OPTIMIZATION_2) ) b left join (select distinct sku_count,short_name from eggozdb.maplemonk.my_sql_product_product) pp on b.sku = concat(pp.sku_count,pp.short_name) ) bb left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.code = bb.retailer_name left join eggozdb.maplemonk.my_sql_retailer_customer_category rcc on rcc.id = rr.category_id left join eggozdb.maplemonk.my_sql_retailer_customer_subcategory rcs on rcs.id = rr.sub_category_id left join (select retailer_name, area_classification, sku, sum(ifnull(eggs_sold,0)) eggs_sold, sum(ifnull(eggs_replaced,0)+ifnull(eggs_return,0)) eggs_replaced, month(date) month, year(date) year from eggozdb.maplemonk.primary_and_secondary_sku where eggs_sold is not null and date >= LAST_DAY(TO_DATE(DATEADD(MONTH, -2, cast(timestampadd(minute, 810, CURRENT_TIMESTAMP()) as date)))) + 1 and date <= cast(timestampadd(minute, 810, CURRENT_TIMESTAMP()) as date) group by retailer_name, area_classification, sku, month(date), year(date) ) pss on pss.retailer_name = bb.retailer_name and pss.month = bb.month and pss.year = bb.year and pss.sku = bb.sku where bb.month is not null ; create or replace table eggozdb.maplemonk.tg_output_analysis as select al2.*, rrr.marketing_cluster, bc.city_name from ( select al.*, 0 as ideal_residual from ( with expected_cte as ( select distinct t1.retailer_name, t1.area_classification, t1.beat_number_original, cast(timestampadd(minute, 660, rr.onboarding_date) as date) onboarding_date, rr.onboarding_status, t1.sku, rl.retailer_category, round(ceil(t1.\"Eggs Sold_new\"*1.0/(datediff(\'day\', date_trunc(\'month\', cast(timestampadd(minute, 810, current_date()) as date)),cast(timestampadd(minute, 810, current_date()) as date))+1))/pp.sku_count)*pp.sku_count eggs_sold_predicted, round(ceil(t1.\"Eggs Replaced_new\"*1.0/(datediff(\'day\', date_trunc(\'month\', cast(timestampadd(minute, 810, current_date()) as date)),cast(timestampadd(minute, 810, current_date()) as date))+1))/pp.sku_count)*pp.sku_count eggs_replaced_predicted, pp.sku_count from TRUEGRADIENT_REPLACEMENT_OPTIMIZATION_2 t1 left join my_sql_retailer_retailer rr on rr.code = t1.retailer_name left join retailer_list rl on rl.party_name = t1.retailer_name left join my_sql_product_product pp on concat(pp.sku_count,pp.short_name) = t1.sku where forecast_from = (select max(forecast_from) from eggozdb.maplemonk.TRUEGRADIENT_REPLACEMENT_OPTIMIZATION_2) ), actual_cte as ( select distinct retailer_name, area_classification, beat_number_original, onboarding_date, onboarding_status, sku, retailer_category, sum(eggs_sold) eggs_sold, sum(eggs_replaced)+sum(eggs_return) eggs_replaced, pp.sku_count from primary_and_secondary_sku t1 left join (select distinct sku_count, short_name from my_sql_product_product where product_type = \'Eggoz Premium\') pp on concat(pp.sku_count,pp.short_name) = t1.sku where date between DATE_TRUNC(\'month\', cast(timestampadd(minute, 810, current_date()) as date)) and cast(timestampadd(minute, 810, current_date()) as date) and t1.area_classification in (\'Delhi-GT\',\'Noida-GT\',\'Gurgaon-GT\',\'NCR-OF-MT\',\'Bangalore-GT\',\'Bangalore-OF-MT\') and t1.revenue is not null group by retailer_name, area_classification, beat_number_original, onboarding_date, onboarding_status, sku, retailer_category, pp.sku_count, pp.short_name ) select distinct coalesce(tt.retailer_name, mm.retailer_name) retailer_name, coalesce(tt.area_classification,mm.area_classification) area_classification, coalesce(tt.sku, mm.sku) sku, coalesce(tt.beat_number_original, mm.beat_number_original) beat_number_original, coalesce(tt.onboarding_date, mm.onboarding_date) onboarding_date, coalesce(tt.onboarding_status, mm.onboarding_status) onboarding_status, coalesce(tt.retailer_category, mm.retailer_category) retailer_category, tt.eggs_sold_predicted, mm.eggs_sold eggs_sold_actual, tt.eggs_replaced_predicted, mm.eggs_replaced eggs_replaced_actual, coalesce(tt.sku_count, mm.sku_count) sku_count from expected_cte tt full outer join actual_cte mm on tt.retailer_name = mm.retailer_name and tt.sku = mm.sku where lower(tt.retailer_category) <> \'distributor\' and lower(mm.retailer_category) <> \'distributor\' ) al where al.onboarding_status = \'Active\' and al.onboarding_date < DATE_TRUNC(\'MONTH\', CURRENT_DATE) - INTERVAL \'1 MONTH\' ) al2 left join eggozdb.maplemonk.my_sql_retailer_retailer rrr on rrr.code = al2.retailer_name left join eggozdb.maplemonk.my_sql_base_city bc on bc.id = rrr.city_id ; create or replace table eggozdb.maplemonk.tg_output_analysis_stdaway_retailer_sku_level as SELECT area_classification AS area_classification, retailer_name AS retailer_name, marketing_cluster, city_name, sku AS sku, sum(eggs_sold_predicted) AS sold_pred, sum(eggs_sold_actual) AS sold_actual, sum(eggs_replaced_predicted) AS replaced_pred, sum(eggs_replaced_actual) AS replaced_actual, sqrt(sum((eggs_sold_actual/sku_count - eggs_sold_predicted/sku_count)*(eggs_sold_actual/sku_count - eggs_sold_predicted/sku_count))/COUNT(retailer_name)) AS sold_rmse, sqrt(sum((eggs_replaced_actual/sku_count - eggs_replaced_predicted/sku_count)*(eggs_replaced_actual/sku_count - eggs_replaced_predicted/sku_count))/COUNT(retailer_name)) AS rep_rmse, to_double(round(truncate((SUM(eggs_sold_actual-eggs_sold_predicted) - AVG(ideal_residual))/ (case when (0.30*(greatest(sum(eggs_sold_actual), sum(eggs_sold_predicted))))=0 then 1 else (0.30*(greatest(sum(eggs_sold_actual), sum(eggs_sold_predicted)))) END )) + case when abs(mod((SUM(eggs_sold_actual-eggs_sold_predicted) - AVG(ideal_residual))/ (case when (0.30*(greatest(sum(eggs_sold_actual), sum(eggs_sold_predicted))))=0 then 1 else (0.30*(greatest(sum(eggs_sold_actual), sum(eggs_sold_predicted)))) END), 1))<0.6 then 0 ELSE mod((SUM(eggs_sold_actual-eggs_sold_predicted) - AVG(ideal_residual))/(case when (0.30*(greatest(sum(eggs_sold_actual), sum(eggs_sold_predicted))))=0 then 1 else (0.30*(greatest(sum(eggs_sold_actual), sum(eggs_sold_predicted)))) END), 1) END)) AS std_away, to_double(round(truncate((SUM(eggs_replaced_actual-eggs_replaced_predicted) - AVG(ideal_residual))/ (case when (0.30*(greatest(sum(eggs_replaced_actual), sum(eggs_replaced_predicted))))=0 then 1 else (0.30*(greatest(sum(eggs_replaced_actual), sum(eggs_replaced_predicted)))) END )) + case when abs(mod((SUM(eggs_replaced_actual-eggs_replaced_predicted) - AVG(ideal_residual))/ (case when (0.30*(greatest(sum(eggs_replaced_actual), sum(eggs_replaced_predicted))))=0 then 1 else (0.30*(greatest(sum(eggs_replaced_actual), sum(eggs_replaced_predicted)))) END), 1))<0.6 then 0 ELSE mod((SUM(eggs_replaced_actual-eggs_replaced_predicted) - AVG(ideal_residual))/ (case when (0.30*(greatest(sum(eggs_replaced_actual), sum(eggs_replaced_predicted))))=0 then 1 else (0.30*(greatest(sum(eggs_replaced_actual), sum(eggs_replaced_predicted)))) END), 1) END)) AS std_away_rep FROM maplemonk.tg_output_analysis WHERE ((onboarding_status = \'Active\')) GROUP BY area_classification, retailer_name, marketing_cluster, city_name, sku ;",
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
                        