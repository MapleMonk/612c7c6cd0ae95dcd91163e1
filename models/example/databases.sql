{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.Overall_KPI as Select xx.*, yy.Demand, yy.Fresh_In , yy.Supply, yy.Old_In, yy.out,zz.No_of_Visit_P,zz.Total_Onboarded_P, xo.No_of_Visit_S,xo.Total_Onboarded_S,we.No_of_Visit_B , we.Total_Onboarded_B, px.No_of_Visit_B_S,px.Total_Onboarded_B_S from ( SELECT DATE_TRUNC(\'DAY\', date) AS date1, area AS area, sum(eggs_sold_white) AS Eggs_Sold_White, sum(eggs_sold_brown) AS Eggs_Sold_Brown, sum(eggs_sold_nutra) AS Eggs_Sold_Nutra, sum(eggs_sold) AS Eggs_Sold, sum(eggs_replaced) AS Eggs_Replaced, case when sum(eggs_sold)=0 then 0 else sum(eggs_replaced)/sum(eggs_sold) end AS Replacement_Per, case when sum(eggs_sold_white)=0 then 0 else sum(eggs_replaced_white)/sum(eggs_sold_white) end AS Eggs_Replacement_Per_White, case when sum(eggs_sold_nutra)=0 then 0 else sum(eggs_replaced_nutra)/sum(eggs_sold_nutra) end AS Eggs_Replacement_Per_Nutra, case when sum(eggs_sold_brown)=0 then 0 else sum(eggs_replaced_brown)/sum(eggs_sold_brown) end AS Eggs_Replacement_Per_Brown, sum(eggs_returned) AS Eggs_Returned, case when sum(eggs_sold)=0 then 0 else sum(eggs_returned)/sum(eggs_sold) end AS Returned_Per, sum(amount_return) AS Amount_Return, sum(net_sales) AS Revenue, sum(net_sales)-sum(amount_return) AS NET_SALES, case when sum(eggs_sold)=0 then 0 ELSE sum(net_sales)/sum(eggs_sold) END AS Landing_Price, sum(collections) AS Collections, case when sum(net_sales)=0 then 0 else sum(collections)/(sum(net_sales)-sum(amount_return)) end AS Collection_Per, sum(daily_retailers_onboarded) AS Retailers_Onboarded, sum(eggs_promo) AS Eggs_Promo, case when sum(eggs_sold)=0 then 0 else sum(eggs_promo)/sum(eggs_sold) end AS Promo_Per FROM maplemonk.summary_reporting_table GROUP BY area, DATE_TRUNC(\'DAY\', date) ORDER BY Eggs_Sold DESC )xx left join ( SELECT DATE_TRUNC(\'DAY\', date) AS date2, area AS area, sum(out)-sum(sold)-sum(replacement)+sum(transfer)-sum(promo)-sum(fresh_in) AS BRANDED_SHORTFALL, SUM(replacement)+SUM(return)-sum(damage)-SUM(old_in) AS NON_BRANDED_SHORTFALL, case when sum(demand)=0 then 0 ELSE -1*(sum(demand) - sum(out))/sum(demand) END AS LESS_SUPPLIED, case when sum(supply)=0 then 0 ELSE -1*(sum(supply) - sum(out))/sum(supply) END AS LESS_SUPPLIED_AFTER_COMMITMENT, case when sum(out)=0 then 0 ELSE sum(sold)/sum(out) END AS SOLD_VS_SUPPLY, case when sum(out)=0 then 0 ELSE (sum(sold)+sum(replacement))/sum(out) END AS SOLD_AND_REPLACEMENT_VS_SUPPLY, case when sum(out)=0 then 0 ELSE sum(fresh_in)/sum(out) END AS FRESH_RETURNED_TO_WAREHOUSE, sum(demand) AS Demand, sum(supply) AS Supply, sum(out) AS Out, sum(sold) AS Sold, sum(damage) AS Damage, sum(return) AS Return, sum(replacement) AS Replacement, sum(transfer) AS Transfer, sum(promo) AS Promo, sum(fresh_in) AS Fresh_In, sum(old_in) AS Old_In FROM maplemonk.beat_material_kpi GROUP BY DATE_TRUNC(\'DAY\', date), area ORDER BY Sold DESC )yy on xx.date1 = yy.date2 and xx.area = yy.area left join ( select distinct mm.Date3, mm.area, sum(mm.retailers_Covered) over (partition by mm.Date3 , mm.area order by mm.Date3 , mm.area) as No_of_Visit_P , sum(mm.total_onboarded) over (partition by mm.Date3 , mm.area order by mm.Date3 , mm.area) as Total_Onboarded_P , avg(mm.beat_Utilization_per) over (partition by mm.Date3 , mm.area order by mm.Date3 , mm.area) as Beat_Utilization_Perc_P from ( SELECT DATE_TRUNC(\'DAY\', date) AS date3, area AS area, beat_number_original AS beat_number_original, count(retailer_name)/(AVG(total_onboarded)*count(DISTINCT date)) AS Beat_Utilization_Per, COUNT(DISTINCT DATE) AS Beat_Visit_counts, count(DISTINCT retailer_name) AS Retailers_Covered, AVG(total_onboarded) AS Total_Onboarded, sum(replaced) AS Replaced, sum(returned) AS Returned, sum(sold) AS Eggs_Sold FROM maplemonk.beat_utilization_test GROUP BY DATE_TRUNC(\'DAY\', date), area, beat_number_original HAVING ((COUNT(DISTINCT(retailer_name)) > 4)) )mm )zz on xx.date1 = zz.date3 and xx.area =zz.area left join ( select distinct nn.Date4, nn.area_classification, sum(nn.Retailers_Covered) over (partition by nn.Date4 , nn.area_classification order by nn.Date4 , nn.area_classification) as No_of_Visit_S , sum(nn.Onboarded_Retailers) over (partition by nn.date4 , nn.area_classification order by nn.Date4 , nn.area_classification) as Total_Onboarded_S , avg(nn.beat_Utilization_per) over (partition by nn.date4, nn.area_classification order by nn.Date4 , nn.area_classification) as Beat_Utilization_Perc_S from ( SELECT DATE_TRUNC(\'DAY\', date) AS date4, area_classification AS area_classification, beat_number AS beat_number, count(retailer_id)/(AVG(onboarded_retailers_in_beat)*count(DISTINCT date)) AS Beat_Utilization_Per, COUNT(DISTINCT DATE) AS Beat_Visit_counts, count(DISTINCT retailer_id) AS Retailers_covered, AVG(onboarded_retailers_in_beat) AS Onboarded_Retailers, sum(eggs_sold) AS Eggs_Sold, sum(eggs_rep) AS Eggs_Replaced FROM maplemonk.secondary_untouched_retailers WHERE ((order_status is not null or return_status is not null or replacement_status is not NULL or promo_status is not NULL)) GROUP BY DATE_TRUNC(\'DAY\', date), area_classification, beat_number HAVING ((Count(DISTINCT(retailer_id))>4)) )nn )xo on xx.date1 = xo.date4 and xx.area = xo.area_classification left join ( select distinct uu.date5, uu.area, sum(uu.No_of_Visits) over (partition by uu.date5 , uu.area order by uu.date5 , uu.area) as No_of_Visit_B , sum(uu.Total_Onboarded) over (partition by uu.date5 , uu.area order by uu.date5 , uu.area) as Total_Onboarded_B from ( SELECT DATE_TRUNC(\'DAY\', date) AS date5, beat_number_original AS beat_number_original, area AS area, count(DISTINCT retailer_name) AS No_of_Visits, count(DISTINCT case when onboarding_status = \'Active\' then retailer_name end) AS active_Retailer_visits, count (distinct retailer_name) - count(DISTINCT case when onboarding_status = \'Active\' then retailer_name end) AS other_retailer_vists, AVG(total_onboarded) AS Total_Onboarded, count(DISTINCT(retailer_name))/avg(total_onboarded) AS Utilization_Per, sum(sold) AS Eggs_Sold, sum(revenue) AS Revenue, sum(today_billing_collections) AS Today_Billing_Collections, sum(collections) AS Collections, case when sum(sold) = 0 then 0 else sum(replaced)/sum(sold) end AS Replaced_Per, sum(returned) AS Returned, sum(replaced) AS Replaced, case when sum(sold) = 0 then 0 else sum(returned)/sum(sold) END AS Returned_Per FROM maplemonk.beat_utilization_test WHERE sold > 0 GROUP BY DATE_TRUNC(\'DAY\', date), beat_number_original, area )uu )we on xx.date1 = we.date5 and xx.area = we.area left join ( select distinct gg.date6, gg.area, sum(gg.No_of_Visits) over (partition by gg.date6 , gg.area order by gg.date6 , gg.area) as No_of_Visit_B_S , sum(gg.Onboarded_Retailers) over (partition by gg.date6 , gg.area order by gg.date6 , gg.area) as Total_Onboarded_B_S from ( SELECT beat_number AS beat_number, area AS area, DATE_TRUNC(\'DAY\', date) AS date6, count(DISTINCT retailer_id) AS No_of_visits, count(DISTINCT case when onboarding_status = \'Active\' then retailer_id end) AS Active_Retailer_visits, count (distinct retailer_id) - count(DISTINCT case when onboarding_status = \'Active\' then retailer_id end) AS Other_retailer_visits, AVG(onboarded_retailers_in_beat) AS Onboarded_Retailers, count(DISTINCT(retailer_id))/AVG(onboarded_retailers_in_beat) AS Beat_Utilization, sum(eggs_ret) AS Eggs_Returned, sum(total_return_amount) AS Amount_Returned, sum(sale) AS Sale, sum(eggs_promo) AS Eggs_Promo, sum(eggs_sold) AS Eggs_Sold, sum(eggs_rep) AS Eggs_Replaced FROM maplemonk.secondary_untouched_retailers WHERE eggs_sold > 0 AND ((order_status is not null or return_status is not null or replacement_status is not NULL or promo_status is not NULL)) GROUP BY beat_number, area, DATE_TRUNC(\'DAY\', date) )gg )px on xx.date1= px.date6 and xx.area = px.area ;",
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
                        