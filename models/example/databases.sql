{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.Overall_KPI as select bj.*, jb.Total_packaged_Eggs, jb.Total_Packaging_Cost, jb.Eggs_Replaced_Brown, jb.Eggs_Replaced_White, jb.Eggs_Replaced_Nutra , jb.Eggs_Return_Brown, jb.Eggs_Return_White, jb.Eggs_Return_Nutra ,jb.GRN_DATE, jb.region, jb.Total_Procurred_Egg, jb.Amount, jb.Procurred_Egg_White, jb.Amount_White, jb.Procurred_Egg_Brown,jb.Amount_Brown, jb.Procurred_Egg_Nutra,jb.Amount_Nutra , jb.Proc_Price, jb.Procured_Price_Brown, jb.Procured_Price_White, jb.Procured_Price_Nutra from ( Select xx.*, yy.Demand, yy.Fresh_In , yy.Supply, yy.Old_In, yy.out,zz.No_of_Visit_P,zz.Total_Onboarded_P, xo.No_of_Visit_S,xo.Total_Onboarded_S,we.No_of_Visit_B , we.Total_Onboarded_B, px.No_of_Visit_B_S,px.Total_Onboarded_B_S, gf.Total_Onboarded_Secondary_Retailer, gt.Total_Primary_Retailers, jj.Total_Onboarded_Primary, kk.Total_Onboarded_Secondary, jj.No_of_Visit_total_Primary,kk.No_of_Visit_Total_Secondary , ma.Mid_Mile_Cost,ma.Mid_Mile_Eggs_Out, ma.Mid_mile_Unloading_cost , na.Last_Mile_Cost, na.Last_Mile_Eggs_Out,na.Last_mile_Unloading_cost , df.First_Mile_Cost, df.First_Mile_Eggs, gm.no_of_days, gm.days_in_month from ( SELECT DATE_TRUNC(\'DAY\', date) AS date1, area AS area, sum(eggs_sold_white) AS Eggs_Sold_White, sum(eggs_sold_brown) AS Eggs_Sold_Brown, sum(eggs_sold_nutra) AS Eggs_Sold_Nutra, sum(eggs_sold) AS Eggs_Sold, sum(eggs_replaced) AS Eggs_Replaced, case when sum(eggs_sold)=0 then 0 else sum(eggs_replaced)/sum(eggs_sold) end AS Replacement_Per, case when sum(eggs_sold_white)=0 then 0 else sum(eggs_replaced_white)/sum(eggs_sold_white) end AS Eggs_Replacement_Per_White, case when sum(eggs_sold_nutra)=0 then 0 else sum(eggs_replaced_nutra)/sum(eggs_sold_nutra) end AS Eggs_Replacement_Per_Nutra, case when sum(eggs_sold_brown)=0 then 0 else sum(eggs_replaced_brown)/sum(eggs_sold_brown) end AS Eggs_Replacement_Per_Brown, sum(eggs_returned) AS Eggs_Returned, case when sum(eggs_sold)=0 then 0 else sum(eggs_returned)/sum(eggs_sold) end AS Returned_Per, sum(amount_return) AS Amount_Return, sum(net_sales) AS Revenue, sum(net_sales)-sum(amount_return) AS NET_SALES, case when sum(eggs_sold)=0 then 0 ELSE sum(net_sales)/sum(eggs_sold) END AS Landing_Price, sum(collections) AS Collections, case when sum(net_sales)=0 then 0 else sum(collections)/(sum(net_sales)-sum(amount_return)) end AS Collection_Per, sum(daily_retailers_onboarded) AS Retailers_Onboarded, sum(eggs_promo) AS Eggs_Promo, case when sum(eggs_sold)=0 then 0 else sum(eggs_promo)/sum(eggs_sold) end AS Promo_Per FROM maplemonk.summary_reporting_table GROUP BY area, DATE_TRUNC(\'DAY\', date) ORDER BY Eggs_Sold DESC )xx left join ( SELECT DATE_TRUNC(\'DAY\', date) AS date2, area AS area, sum(out)-sum(sold)-sum(replacement)+sum(transfer)-sum(promo)-sum(fresh_in) AS BRANDED_SHORTFALL, SUM(replacement)+SUM(return)-sum(damage)-SUM(old_in) AS NON_BRANDED_SHORTFALL, case when sum(demand)=0 then 0 ELSE -1*(sum(demand) - sum(out))/sum(demand) END AS LESS_SUPPLIED, case when sum(supply)=0 then 0 ELSE -1*(sum(supply) - sum(out))/sum(supply) END AS LESS_SUPPLIED_AFTER_COMMITMENT, case when sum(out)=0 then 0 ELSE sum(sold)/sum(out) END AS SOLD_VS_SUPPLY, case when sum(out)=0 then 0 ELSE (sum(sold)+sum(replacement))/sum(out) END AS SOLD_AND_REPLACEMENT_VS_SUPPLY, case when sum(out)=0 then 0 ELSE sum(fresh_in)/sum(out) END AS FRESH_RETURNED_TO_WAREHOUSE, sum(demand) AS Demand, sum(supply) AS Supply, sum(out) AS Out, sum(sold) AS Sold, sum(damage) AS Damage, sum(return) AS Return, sum(replacement) AS Replacement, sum(transfer) AS Transfer, sum(promo) AS Promo, sum(fresh_in) AS Fresh_In, sum(old_in) AS Old_In FROM maplemonk.beat_material_kpi GROUP BY DATE_TRUNC(\'DAY\', date), area ORDER BY Sold DESC )yy on xx.date1 = yy.date2 and xx.area = yy.area left join ( select distinct mm.Date3, mm.area, sum(mm.retailers_Covered) over (partition by mm.Date3 , mm.area order by mm.Date3 , mm.area) as No_of_Visit_P , sum(mm.total_onboarded) over (partition by mm.Date3 , mm.area order by mm.Date3 , mm.area) as Total_Onboarded_P , avg(mm.beat_Utilization_per) over (partition by mm.Date3 , mm.area order by mm.Date3 , mm.area) as Beat_Utilization_Perc_P from ( SELECT DATE_TRUNC(\'DAY\', date) AS date3, area AS area, beat_number_original AS beat_number_original, count(retailer_name)/(AVG(total_onboarded)*count(DISTINCT date)) AS Beat_Utilization_Per, COUNT(DISTINCT DATE) AS Beat_Visit_counts, count(DISTINCT retailer_name) AS Retailers_Covered, AVG(total_onboarded) AS Total_Onboarded, sum(replaced) AS Replaced, sum(returned) AS Returned, sum(sold) AS Eggs_Sold FROM maplemonk.beat_utilization_test GROUP BY DATE_TRUNC(\'DAY\', date), area, beat_number_original HAVING ((COUNT(DISTINCT(retailer_name)) > 4)) )mm )zz on xx.date1 = zz.date3 and xx.area =zz.area left join ( select distinct nn.Date4, nn.area_classification, sum(nn.Retailers_Covered) over (partition by nn.Date4 , nn.area_classification order by nn.Date4 , nn.area_classification) as No_of_Visit_S , sum(nn.Onboarded_Retailers) over (partition by nn.date4 , nn.area_classification order by nn.Date4 , nn.area_classification) as Total_Onboarded_S , avg(nn.beat_Utilization_per) over (partition by nn.date4, nn.area_classification order by nn.Date4 , nn.area_classification) as Beat_Utilization_Perc_S from ( SELECT DATE_TRUNC(\'DAY\', date) AS date4, area_classification AS area_classification, beat_number AS beat_number, count(retailer_id)/(AVG(onboarded_retailers_in_beat)*count(DISTINCT date)) AS Beat_Utilization_Per, COUNT(DISTINCT DATE) AS Beat_Visit_counts, count(DISTINCT retailer_id) AS Retailers_covered, AVG(onboarded_retailers_in_beat) AS Onboarded_Retailers, sum(eggs_sold) AS Eggs_Sold, sum(eggs_rep) AS Eggs_Replaced FROM maplemonk.secondary_untouched_retailers WHERE ((order_status is not null or return_status is not null or replacement_status is not NULL or promo_status is not NULL)) GROUP BY DATE_TRUNC(\'DAY\', date), area_classification, beat_number HAVING ((Count(DISTINCT(retailer_id))>4)) )nn )xo on xx.date1 = xo.date4 and xx.area = xo.area_classification left join ( select distinct uu.date5, uu.area, sum(uu.No_of_Visits) over (partition by uu.date5 , uu.area order by uu.date5 , uu.area) as No_of_Visit_B , sum(uu.Total_Onboarded) over (partition by uu.date5 , uu.area order by uu.date5 , uu.area) as Total_Onboarded_B from ( SELECT DATE_TRUNC(\'DAY\', date) AS date5, beat_number_original AS beat_number_original, area AS area, count(DISTINCT retailer_name) AS No_of_Visits, count(DISTINCT case when onboarding_status = \'Active\' then retailer_name end) AS active_Retailer_visits, count (distinct retailer_name) - count(DISTINCT case when onboarding_status = \'Active\' then retailer_name end) AS other_retailer_vists, AVG(total_onboarded) AS Total_Onboarded, count(DISTINCT(retailer_name))/avg(total_onboarded) AS Utilization_Per, sum(sold) AS Eggs_Sold, sum(revenue) AS Revenue, sum(today_billing_collections) AS Today_Billing_Collections, sum(collections) AS Collections, case when sum(sold) = 0 then 0 else sum(replaced)/sum(sold) end AS Replaced_Per, sum(returned) AS Returned, sum(replaced) AS Replaced, case when sum(sold) = 0 then 0 else sum(returned)/sum(sold) END AS Returned_Per FROM maplemonk.beat_utilization_test WHERE sold > 0 GROUP BY DATE_TRUNC(\'DAY\', date), beat_number_original, area )uu )we on xx.date1 = we.date5 and xx.area = we.area left join ( select distinct gg.date6, gg.area, sum(gg.No_of_Visits) over (partition by gg.date6 , gg.area order by gg.date6 , gg.area) as No_of_Visit_B_S , sum(gg.Onboarded_Retailers) over (partition by gg.date6 , gg.area order by gg.date6 , gg.area) as Total_Onboarded_B_S from ( SELECT beat_number AS beat_number, area AS area, DATE_TRUNC(\'DAY\', date) AS date6, count(DISTINCT retailer_id) AS No_of_visits, count(DISTINCT case when onboarding_status = \'Active\' then retailer_id end) AS Active_Retailer_visits, count (distinct retailer_id) - count(DISTINCT case when onboarding_status = \'Active\' then retailer_id end) AS Other_retailer_visits, AVG(onboarded_retailers_in_beat) AS Onboarded_Retailers, count(DISTINCT(retailer_id))/AVG(onboarded_retailers_in_beat) AS Beat_Utilization, sum(eggs_ret) AS Eggs_Returned, sum(total_return_amount) AS Amount_Returned, sum(sale) AS Sale, sum(eggs_promo) AS Eggs_Promo, sum(eggs_sold) AS Eggs_Sold, sum(eggs_rep) AS Eggs_Replaced FROM maplemonk.secondary_untouched_retailers WHERE eggs_sold > 0 AND ((order_status is not null or return_status is not null or replacement_status is not NULL or promo_status is not NULL)) GROUP BY beat_number, area, DATE_TRUNC(\'DAY\', date) )gg )px on xx.date1= px.date6 and xx.area = px.area left join ( select area_classification, count(distinct code) as Total_Onboarded_Secondary_Retailer from eggozdb.maplemonk.my_sql_retailer_retailer where distributor_id is not null and onboarding_status = \'Active\' group by area_classification ) gf on xx.area = gf.area_classification left join ( select area_classification, count(distinct code) as Total_Primary_Retailers from eggozdb.maplemonk.my_sql_retailer_retailer where distributor_id is null and category_id <> 3 and onboarding_status = \'Active\' group by area_classification )gt on xx.area= gt.area_classification left join ( select distinct mm.Date3, mm.area, sum(mm.retailers_Covered) over (partition by mm.Date3 , mm.area order by mm.Date3 , mm.area) as No_of_Visit_total_Primary ,sum(mm.total_onboarded) over (partition by mm.Date3 , mm.area order by mm.Date3 , mm.area) as Total_Onboarded_Primary from ( SELECT DATE_TRUNC(\'DAY\', date) AS date3, area AS area, beat_number_original AS beat_number_original, count(retailer_name)/(AVG(total_onboarded)*count(DISTINCT date)) AS Beat_Utilization_Per, COUNT(DISTINCT DATE) AS Beat_Visit_counts, count(DISTINCT retailer_name) AS Retailers_Covered, AVG(total_onboarded) AS Total_Onboarded, sum(replaced) AS Replaced, sum(returned) AS Returned, sum(sold) AS Eggs_Sold FROM maplemonk.beat_utilization_test GROUP BY DATE_TRUNC(\'DAY\', date), area, beat_number_original )mm )jj on xx.date1 = jj.Date3 and xx.area= jj.area left join ( select distinct nn.Date4, nn.area_classification , sum(nn.Retailers_Covered) over (partition by nn.Date4 , nn.area_classification order by nn.Date4 , nn.area_classification) as No_of_Visit_Total_Secondary ,sum(nn.Onboarded_Retailers) over (partition by nn.date4 , nn.area_classification order by nn.Date4 , nn.area_classification) as Total_Onboarded_Secondary from ( SELECT DATE_TRUNC(\'DAY\', date) AS date4, area_classification AS area_classification, beat_number AS beat_number, count(retailer_id)/(AVG(onboarded_retailers_in_beat)*count(DISTINCT date)) AS Beat_Utilization_Per, COUNT(DISTINCT DATE) AS Beat_Visit_counts, count(DISTINCT retailer_id) AS Retailers_covered, AVG(onboarded_retailers_in_beat) AS Onboarded_Retailers, sum(eggs_sold) AS Eggs_Sold, sum(eggs_rep) AS Eggs_Replaced FROM maplemonk.secondary_untouched_retailers WHERE ((order_status is not null or return_status is not null or replacement_status is not NULL or promo_status is not NULL)) GROUP BY DATE_TRUNC(\'DAY\', date), area_classification, beat_number )nn )kk on xx.date1=kk.date4 and xx.area = kk.area_classification full outer join ( select DISTINCT to_date(pp.date, \'dd/mm/yyyy\') as Date12, sum(pp.Cost) over (partition by pp.date,pp.area order by pp.date,pp.area) as Mid_Mile_Cost, sum(pp.Total_Eggs_Out) over (partition by pp.date,pp.area order by pp.date, pp.area) as Mid_Mile_Eggs_Out , pp.\"GT/MT\" , pp.Area , sum(pp.Unloading_Cost) over (partition by pp.date,pp.area order by pp.date, pp.area) as Mid_mile_Unloading_cost , case when Area = \'Delhi\' then \'Delhi-GT\' when Area = \'Noida\' then \'Noida-GT\' when Area = \'Gurgaon\' then \'Gurgaon-GT\' end as Area_classification from ( select Date , \"Cost/Egg_Out\" as Cost_Per_Egg, Area, Cost, Total_Eggs_Out , \"GT/MT\" ,Unloading_Cost from maplemonk.transport_costs_last___mid_mile where beat = \'Mid Mile\' order by date )pp )ma on xx.date1= ma.Date12 and xx.area = ma.Area_Classification full outer join ( select distinct yu.Date13, sum(yu.Last_Mile_cost) over (partition by yu.Date13,yu.area_classification order by yu.Date13,yu.area_classification) as Last_Mile_Cost , sum(yu.last_mile_eggs_out) over (partition by yu.Date13,yu.area_classification order by yu.Date13,yu.area_classification) as Last_Mile_Eggs_Out, sum(yu.last_mile_unloading_cost) over (partition by yu.Date13,yu.area_classification order by yu.Date13,yu.area_classification) as Last_mile_Unloading_cost , yu.Area_classification from ( select DISTINCT to_date(pp.date, \'dd/mm/yyyy\') as Date13, sum(pp.Cost) over (partition by pp.date,pp.\"GT/MT\" order by pp.date,pp.\"GT/MT\") as Last_Mile_Cost, sum(pp.Total_Eggs_Out) over (partition by pp.date,pp.\"GT/MT\" order by pp.date, pp.\"GT/MT\") as Last_Mile_Eggs_Out , pp.\"GT/MT\" , sum(pp.Unloading_Cost) over (partition by pp.date,pp.\"GT/MT\" order by pp.date, pp.\"GT/MT\") as Last_mile_Unloading_cost , case when \"GT/MT\" in (\'D-DST\',\'DGT\') then \'Delhi-GT\' when \"GT/MT\" in (\'G-DST\',\'GGT\') then \'Gurgaon-GT\' when \"GT/MT\" in (\'N-DST\', \'NGT\') then \'Noida-GT\' when \"GT/MT\" in (\'ON-MT\') then \'NCR-ON-MT\' when \"GT/MT\" in (\'OFF-MT\') then \'NCR-OF-MT\' when \"GT/MT\" in (\'UB\',\'DRZ\') then \'UB\' else \'Others\' end as Area_classification from ( select Date , \"Cost/Egg_Out\" as Cost_Per_Egg, Area, Cost, Total_Eggs_Out , \"GT/MT\" ,Unloading_Cost from maplemonk.transport_costs_last___mid_mile where beat<> \'Mid Mile\' order by date )pp where to_date(pp.date, \'dd/mm/yyyy\') >= \'2022-01-01\' ) yu )na on xx.date1= na.Date13 and xx.area = na.Area_Classification full outer join ( select distinct to_date(Date, \'dd/mm/yyyy\') as Date15, sum(\"Picked_up_Qty(In_Eggs)\") over (partition by date order by Date) as First_Mile_Eggs , sum(Total_Expense) over (partition by date order by Date) as First_Mile_Cost from maplemonk.transport_costs_FM_Vehicle_Details_after_1st_April order by to_date(Date, \'dd/mm/yyyy\') )df on xx.date1= df.date15 full outer join ( select DISTINCT date, datediff(\'day\',date_trunc(\'month\',date),date)+1 no_of_days , datediff(\'day\',date_trunc(\'month\',date), last_day(date,\'month\'))+1 days_in_month from eggozdb.maplemonk.summary_reporting_table_beat_retailer group by date, area, parent_retailer_name )gm on xx.date1= gm.date )bj left join ( select ui.*, bv.* from ( Select DISTINCT xx.GRN_DATE, xx.region, xx.Total_Procurred_Egg,xx.Amount, yy.Procurred_Egg_White, yy.Amount_White,zz.Procurred_Egg_Brown,zz.Amount_Brown, gg.Procurred_Egg_Nutra, gg.Amount_Nutra, xx.Amount/xx.Total_Procurred_Egg as Proc_Price , yy.Amount_White/yy.Procurred_Egg_White as Procured_Price_White , zz.Amount_Brown/zz.Procurred_Egg_Brown as Procured_Price_Brown , gg.Amount_Nutra/gg.Procurred_Egg_Nutra as Procured_Price_Nutra From ( SELECT GRN_DATE, region, sum(EGGS) as Total_Procurred_Egg, sum(amount) as Amount from eggozdb.maplemonk.region_wise_procurement_masterdata group by 1 ,2 ) xx full outer join (SELECT GRN_DATE, region, sum(EGGS) as Procurred_Egg_White , sum(amount) as Amount_White from eggozdb.maplemonk.region_wise_procurement_masterdata where type =\'White\' group by 1,2 )yy on xx.GRN_DATE= yy.GRN_DATE and xx.region = yy.region full outer join ( SELECT GRN_DATE, region, sum(EGGS) as Procurred_Egg_Brown , sum(amount) as Amount_Brown from eggozdb.maplemonk.region_wise_procurement_masterdata where type = \'Brown\' group by 1,2 ) zz on xx.GRN_DATE = zz.GRN_DATE and xx.region = zz.region full outer join ( SELECT GRN_DATE, region ,sum(EGGS) as Procurred_Egg_Nutra, sum(amount) as Amount_Nutra from eggozdb.maplemonk.region_wise_procurement_masterdata where type = \'Nutra+\' group by 1,2 ) gg on xx.GRN_DATE = gg.GRN_DATE and xx.region= gg.region )ui left join ( Select DISTINCT (ds.date), ds.area, case when ds.area in (\'Bangalore-GT\',\'Bangalore-OF-MT\', \'Bangalore-ON-MT\',\'Bangalore-UB\' ) then \'Bangalore\' when ds.area in (\'Delhi-GT\',\'Noida-GT\', \'NCR-OF-MT\', \'Gurgaon-GT\', \'NCR-ON-MT\') then \'NCR\' when ds.area in (\'East-GT\',\'East-OF-MT\', \'East-ON-MT\', \'East-UB\') then \'East\' when ds.area in (\'MP-ON-MT\', \'MP-OF-MT\', \'Indore-GT\', \'Bhopal-GT\') then \'M.P\' else \'NA\' end as Area_classification22, sum(ds.packaged_eggs) over (partition by ds.date , ds.area order by ds.date , ds.area) as Total_packaged_Eggs, sum(ds.packaging_cost) over (partition by ds.date , ds.area order by ds.date , ds.area) as Total_Packaging_Cost , sum(ds.Eggs_Sold_White) over (partition by ds.date , ds.area order by ds.date , ds.area) as Eggs_Sold_White, sum(ds.Eggs_Sold_Brown) over (partition by ds.date , ds.area order by ds.date , ds.area) as Eggs_Sold_Brown, sum(ds.Eggs_Sold_Nutra) over (partition by ds.date , ds.area order by ds.date , ds.area) as Eggs_Sold_Nutra, sum(ds.Eggs_Replaced_White) over (partition by ds.date , ds.area order by ds.date , ds.area) as Eggs_Replaced_White, sum(ds.Eggs_Replaced_Brown) over (partition by ds.date , ds.area order by ds.date , ds.area) as Eggs_Replaced_Brown, sum(ds.Eggs_Replaced_Nutra) over (partition by ds.date , ds.area order by ds.date , ds.area) as Eggs_Replaced_Nutra, sum(ds.Eggs_Return_White) over (partition by ds.date , ds.area order by ds.date , ds.area) as Eggs_Return_White, sum(ds.Eggs_Return_Brown) over (partition by ds.date , ds.area order by ds.date , ds.area) as Eggs_Return_Brown, sum(ds.Eggs_Return_Nutra) over (partition by ds.date , ds.area order by ds.date , ds.area) as Eggs_Return_Nutra from ( Select cc.* , dd.packaging_cost_per_egg , cc.packaged_eggs * dd.packaging_cost_per_egg as Packaging_Cost from ( SELECT DATE_TRUNC(\'DAY\', date) AS date, area AS area, case when area in (\'Bangalore-GT\',\'Bangalore-OF-MT\') then \'Bangalore-Others\' when area in (\'Delhi-GT\',\'Noida-GT\', \'NCR-OF-MT\', \'Gurgaon-GT\') then \'NCR-Others\' when area in (\'East-GT\',\'East-OF-MT\') then \'East-Others\' when area in (\'NCR-ON-MT\') then \'NCR-ON-MT\' when area in (\'Bangalore-ON-MT\') then \'Bangalore-ON-MT\' when area in (\'East-ON-MT\') then \'East-ON-MT\' else \'NA\' end as Area_classification1, sku AS sku, case when sum(eggs_sold) is null then 0 else sum(eggs_sold) end AS Eggs_Sold, sum(net_sales) AS Revenue, sum(amount_return) AS Amount_Returned, case when sum(eggs_sold)-sum(eggs_return) = 0 then 0 else (sum(net_sales)-sum(amount_return))/(sum(eggs_sold)-sum(eggs_return)) END AS Avg_SP, sum(eggs_replaced) AS Eggs_replaced, sum(eggs_return) AS Eggs_Returned, case when sum(eggs_sold)=0 then 0 else sum(eggs_promo)/sum(eggs_sold) end AS Promo_Per, sum(eggs_promo) AS Eggs_promo, case when sum(eggs_sold)-sum(eggs_return) = 0 then 0 ELSE sum(eggs_replaced)/(sum(eggs_sold)-sum(eggs_return)) END AS Replacement_Per, case when sum(eggs_sold)-sum(eggs_return) = 0 then 0 ELSE (sum(eggs_return)/(sum(eggs_sold)-sum(eggs_return))) END AS Returned_Per, sum(eggs_sold) + sum(eggs_promo)+ sum(eggs_replaced) as Packaged_Eggs, sum(eggs_sold_White) as Eggs_Sold_White, sum(eggs_sold_Brown) as Eggs_Sold_Brown, sum(eggs_sold_Nutra) as Eggs_Sold_Nutra, sum(eggs_return_white) as Eggs_Return_White, sum(eggs_return_brown) as Eggs_Return_Brown, sum(eggs_return_nutra) as Eggs_Return_Nutra, sum(eggs_replaced_white) as Eggs_Replaced_White, sum(eggs_replaced_brown) as Eggs_Replaced_Brown, sum(eggs_replaced_Nutra) as Eggs_Replaced_Nutra FROM maplemonk.summary_reporting_table_beat_retailer_sku GROUP BY DATE_TRUNC(\'DAY\', date), area, sku )cc left join ( select area, sku, packaging_cost_per_egg from eggozdb.maplemonk.pkg_cost_Packaging_cost )dd on cc.sku = dd.sku and cc.Area_classification1 = dd.area )ds )bv on ui.GRN_DATE = bv.date and ui.region = bv.Area_classification22 )jb on bj.date1 = jb.date and bj.area = jb.area ; create or replace table eggozdb.maplemonk.Overall_KPI_Unbranded as Select xx.*, ll.last_mile_cost , ll.Last_Mile_Unloading_Cost , mm.First_Mile_Cost, mm.First_Mile_Eggs from ( select distinct (aa.delivery_date), aa.category, aa.egg_type, aa.Total_Sales , aa.Total_Eggs_Sold, aa.Landing_SP, aa.Procured_Price,aa.Loss_Or_Profit_per_Egg, aa.Loss_Or_Profit, ifnull(bb.Sales_White_Chatki,0) as Sales_White_Chatki, ifnull(bb.Eggs_Sold_White_Chatki,0) as Eggs_Sold_White_Chatki , ifnull(Landing_SP_White_Chatki,0) as Landing_SP_White_Chatki, ifnull(bb.Procured_Price_White_Chatki,0) as Procurred_Price_White_Chatki, ifnull(bb.Loss_Or_Profit_per_Egg_White_Chatki,0) as Loss_Or_Profit_per_Egg_White_Chatki , ifnull(bb.Loss_Or_Profit_White_Chatki,0) as Loss_Or_Profit_White_Chatki, ifnull(cc.Sales_Brown_Chatki,0) as Sales_Brown_Chatki , ifnull(cc.Eggs_Sold_Brown_Chatki,0) as Eggs_Sold_Brown_Chatki , ifnull(cc.Landing_SP_Brown_Chatki,0) as Landing_SP_Brown_Chatki , ifnull(cc.Procured_Price_Brown_Chatki,0) as Procured_Price_Brown_Chatki , ifnull(cc.Loss_Or_Profit_per_Egg_Brown_Chatki,0) as Loss_Or_Profit_per_Egg_Brown_Chatki , ifnull(cc.Loss_Or_Profit_Brown_Chatki,0) as Loss_Or_Profit_Brown_Chatki , ifnull(dd.Sales_Brown_Hairline,0) as Sales_Brown_Hairline , ifnull(dd.Eggs_Sold_Brown_Hairline,0) as Eggs_Sold_Brown_Hairline , ifnull(dd.Landing_SP_Brown_Hairline,0) as Landing_SP_Brown_Hairline , ifnull(dd.Procured_Price_Brown_Hairline,0) as Procured_Price_Brown_Hairline , ifnull(dd.Loss_Or_Profit_per_Egg_Brown_Hairline,0) as Loss_Or_Profit_per_Egg_Brown_Hairline, ifnull(dd.Loss_Or_Profit_Brown_Hairline,0) as Loss_Or_Profit_Brown_Hairline , ifnull(ee.Sales_White_Hairline,0) as Sales_White_Hairline , ifnull(ee.Eggs_Sold_White_Hairline,0) as Eggs_Sold_White_Hairline , ifnull(ee.Landing_SP_White_Hairline,0) as Landing_SP_White_Hairline , ifnull(ee.Procured_Price_White_Hairline,0) as Procured_Price_White_Hairline , ifnull(ee.Loss_Or_Profit_per_Egg_White_Hairline,0) as Loss_Or_Profit_per_Egg_White_Hairline, ifnull(ee.Loss_Or_Profit_White_Hairline,0) as Loss_Or_Profit_White_Hairline, ifnull(ff.Sales_Brown_Melted,0) as Sales_Brown_Melted , ifnull(ff.Eggs_Sold_Brown_Melted,0) as Eggs_Sold_Brown_Melted, ifnull(ff.Landing_SP_Brown_Melted,0) as Landing_SP_Brown_Melted , ifnull(ff.Procured_Price_Brown_Melted,0) as Procured_Price_Brown_Melted , ifnull(ff.Loss_Or_Profit_per_Egg_Brown_Melted,0) as Loss_Or_Profit_per_Egg_Brown_Melted, ifnull(ff.Loss_Or_Profit_Brown_Melted,0) as Loss_Or_Profit_Brown_Melted, ifnull(gg.Sales_White_Melted,0) as Sales_White_Melted , ifnull(gg.Eggs_Sold_White_Melted,0)as Eggs_Sold_White_Melted , ifnull(gg.Landing_SP_White_Melted,0) as Landing_SP_White_Melted , ifnull(gg.Procured_Price_White_Melted,0) as Procured_Price_White_Melted, ifnull(gg.Loss_Or_Profit_per_Egg_White_Melted,0) as Loss_Or_Profit_per_Egg_White_Melted, ifnull(gg.Loss_Or_Profit_White_Melted,0) as Loss_Or_Profit_White_Melted, ifnull(hh.Sales_White_Normal,0) as Sales_White_Normal , ifnull(hh.Eggs_Sold_White_Normal,0) as Eggs_Sold_White_Normal, ifnull(hh.Landing_SP_White_Normal,0) as Landing_SP_White_Normal , ifnull(hh.Procured_Price_White_Normal,0) as Procured_Price_White_Normal, ifnull(hh.Loss_Or_Profit_per_Egg_White_Normal,0) as Loss_Or_Profit_per_Egg_White_Normal, ifnull(hh.Loss_Or_Profit_White_Normal,0) as Loss_Or_Profit_White_Normal, ifnull(ii.Sales_Brown_Normal,0) as Sales_Brown_Normal, ifnull(ii.Eggs_Sold_Brown_Normal,0) as Eggs_Sold_Brown_Normal, ifnull(ii.Landing_SP_Brown_Normal,0) as Landing_SP_Brown_Normal , ifnull(ii.Procured_Price_Brown_Normal,0) as Procured_Price_Brown_Normal, ifnull(ii.Loss_Or_Profit_per_Egg_Brown_Normal,0) as Loss_Or_Profit_per_Egg_Brown_Normal, ifnull(ii.Loss_Or_Profit_Brown_Normal,0) as Loss_Or_Profit_Brown_Normal, ifnull(jj.Sales_Brown_Replaced,0) as Sales_Brown_Replaced, ifnull(jj.Eggs_Sold_Brown_Replaced,0) as Eggs_Sold_Brown_Replaced, ifnull(jj.Landing_SP_Brown_Replaced,0) as Landing_SP_Brown_Replaced , ifnull(jj.Procured_Price_Brown_Replaced,0) as Procured_Price_Brown_Replaced , ifnull(jj.Loss_Or_Profit_per_Egg_Brown_Replaced,0) as Loss_Or_Profit_per_Egg_Brown_Replaced, ifnull(jj.Loss_Or_Profit_Brown_Replaced,0) as Loss_Or_Profit_Brown_Replaced, ifnull(kk.Sales_White_Replaced,0) as Sales_White_Replaced, ifnull(kk.Eggs_Sold_White_Replaced,0) as Eggs_Sold_White_Replaced, ifnull(kk.Landing_SP_White_Replaced,0) as Landing_SP_White_Replaced , ifnull(kk.Procured_Price_White_Replaced,0) as Procured_Price_White_Replaced, ifnull(kk.Loss_Or_Profit_per_Egg_White_Replaced,0) as Loss_Or_Profit_per_Egg_White_Replaced, ifnull(kk.Loss_Or_Profit_White_Replaced,0) as Loss_Or_Profit_White_Replaced , ifnull(zz.packaging_cost_per_egg,0), ifnull(ab.Sales_White_Darjan,0) as Sales_White_Darjan, ifnull(ab.Eggs_Sold_White_Darjan,0) as Eggs_Sold_White_Darjan, ifnull(ab.Landing_SP_White_Darjan,0) as Landing_SP_White_Darjan, ifnull(ab.Procured_Price_White_Darjan,0) as Procured_Price_White_Darjan, ifnull(ab.Loss_Or_Profit_per_Egg_White_Darjan,0) as Loss_Or_Profit_per_Egg_White_Darjan, ifnull(ab.Loss_Or_Profit_White_Darjan,0) as Loss_Or_Profit_White_Darjan from ( SELECT region AS region, category as category, egg_type as egg_type, DATE_TRUNC(\'DAY\', delivery_date) AS delivery_date, ifnull(round(SUM(sales_daily_typewise), 0),0) AS Total_Sales, ifnull(round(SUM(eggs_sold_daily_typewise), 0),0) AS Total_Eggs_Sold, case when sum(eggs_sold_daily_typewise) =0 then 0 else ifnull(round(sum(sales_daily_typewise)/sum(eggs_sold_daily_typewise), 2),0) end AS Landing_SP, case when sum(eggs_sold_daily_typewise) =0 then 0 else ifnull(sum(procured_price_daily*eggs_sold_daily_typewise)/sum(eggs_sold_daily_typewise),0) end AS Procured_Price, case when sum(eggs_sold_daily_typewise) =0 then 0 else ifnull(round((sum(sales_daily_typewise)-sum(eggs_sold_daily_typewise*procured_price_daily))/sum(eggs_sold_daily_typewise), 2),0) end AS Loss_Or_Profit_per_Egg, case when sum(eggs_sold_daily_typewise) =0 then 0 else ifnull(round(sum(sales_daily_typewise)-sum(eggs_sold_daily_typewise*procured_price_daily), 0),0) end AS Loss_Or_Profit FROM maplemonk.ub_log GROUP BY region, DATE_TRUNC(\'DAY\', delivery_date), category, egg_type )aa left join ( SELECT region AS region, DATE_TRUNC(\'DAY\', delivery_date) AS delivery_date, ifnull(round(SUM(sales_daily_typewise), 0),0) AS Sales_White_Chatki, ifnull(round(SUM(eggs_sold_daily_typewise), 0),0) AS Eggs_Sold_White_Chatki, ifnull(round(sum(sales_daily_typewise)/sum(eggs_sold_daily_typewise), 2),0) AS Landing_SP_White_Chatki, ifnull(sum(procured_price_daily*eggs_sold_daily_typewise)/sum(eggs_sold_daily_typewise),0) AS Procured_Price_White_Chatki, ifnull(round((sum(sales_daily_typewise)-sum(eggs_sold_daily_typewise*procured_price_daily))/sum(eggs_sold_daily_typewise), 2),0) AS Loss_Or_Profit_per_Egg_White_Chatki, ifnull(round(sum(sales_daily_typewise)-sum(eggs_sold_daily_typewise*procured_price_daily), 0),0) AS Loss_Or_Profit_White_Chatki FROM maplemonk.ub_log WHERE category = \'White\' AND egg_type = \'Chatki\' GROUP BY region, DATE_TRUNC(\'DAY\', delivery_date) )bb on aa.delivery_date= bb.delivery_date and aa.region = bb.region left join ( SELECT region AS region, DATE_TRUNC(\'DAY\', delivery_date) AS delivery_date, ifnull(round(SUM(sales_daily_typewise), 0),0) AS Sales_Brown_Chatki, ifnull(round(SUM(eggs_sold_daily_typewise), 0),0) AS Eggs_Sold_Brown_Chatki, ifnull(round(sum(sales_daily_typewise)/sum(eggs_sold_daily_typewise), 2),0) AS Landing_SP_Brown_Chatki, ifnull(sum(procured_price_daily*eggs_sold_daily_typewise)/sum(eggs_sold_daily_typewise),0) AS Procured_Price_Brown_Chatki, ifnull(round((sum(sales_daily_typewise)-sum(eggs_sold_daily_typewise*procured_price_daily))/sum(eggs_sold_daily_typewise), 2),0) AS Loss_Or_Profit_per_Egg_Brown_Chatki, ifnull(round(sum(sales_daily_typewise)-sum(eggs_sold_daily_typewise*procured_price_daily), 0),0) AS Loss_Or_Profit_Brown_Chatki FROM maplemonk.ub_log WHERE category = \'Brown\' AND egg_type = \'Chatki\' GROUP BY region, DATE_TRUNC(\'DAY\', delivery_date) )cc on aa.delivery_date = cc.delivery_date and aa.region = cc.region left join ( SELECT region AS region, DATE_TRUNC(\'DAY\', delivery_date) AS delivery_date, ifnull(round(SUM(sales_daily_typewise), 0),0) AS Sales_Brown_Hairline, ifnull(round(SUM(eggs_sold_daily_typewise), 0),0) AS Eggs_Sold_Brown_Hairline, ifnull(round(sum(sales_daily_typewise)/sum(eggs_sold_daily_typewise), 2),0) AS Landing_SP_Brown_Hairline, ifnull(sum(procured_price_daily*eggs_sold_daily_typewise)/sum(eggs_sold_daily_typewise),0) AS Procured_Price_Brown_Hairline, ifnull(round((sum(sales_daily_typewise)-sum(eggs_sold_daily_typewise*procured_price_daily))/sum(eggs_sold_daily_typewise), 2),0) AS Loss_Or_Profit_per_Egg_Brown_Hairline, ifnull(round(sum(sales_daily_typewise)-sum(eggs_sold_daily_typewise*procured_price_daily), 0),0) AS Loss_Or_Profit_Brown_Hairline FROM maplemonk.ub_log WHERE category = \'Brown\' AND egg_type = \'Hairline\' GROUP BY region, DATE_TRUNC(\'DAY\', delivery_date) )dd on aa.delivery_date = dd.delivery_date and aa.region =dd.region left join ( SELECT region AS region, DATE_TRUNC(\'DAY\', delivery_date) AS delivery_date, ifnull(round(SUM(sales_daily_typewise), 0),0) AS Sales_White_Hairline, ifnull(round(SUM(eggs_sold_daily_typewise), 0),0) AS Eggs_Sold_White_Hairline, ifnull(round(sum(sales_daily_typewise)/sum(eggs_sold_daily_typewise), 2),0) AS Landing_SP_White_Hairline, ifnull(sum(procured_price_daily*eggs_sold_daily_typewise)/sum(eggs_sold_daily_typewise),0) AS Procured_Price_White_Hairline, ifnull(round((sum(sales_daily_typewise)-sum(eggs_sold_daily_typewise*procured_price_daily))/sum(eggs_sold_daily_typewise), 2),0) AS Loss_Or_Profit_per_Egg_White_Hairline, ifnull(round(sum(sales_daily_typewise)-sum(eggs_sold_daily_typewise*procured_price_daily), 0),0) AS Loss_Or_Profit_White_Hairline FROM maplemonk.ub_log WHERE category = \'White\' AND egg_type = \'Hairline\' GROUP BY region, DATE_TRUNC(\'DAY\', delivery_date) )ee on aa.delivery_date = ee.delivery_date and aa.region = ee.region left join ( SELECT region AS region, DATE_TRUNC(\'DAY\', delivery_date) AS delivery_date, ifnull(round(SUM(sales_daily_typewise), 0),0) AS Sales_Brown_Melted, ifnull(round(SUM(eggs_sold_daily_typewise), 0),0) AS Eggs_Sold_Brown_Melted, ifnull(round(sum(sales_daily_typewise)/sum(eggs_sold_daily_typewise), 2),0) AS Landing_SP_Brown_Melted, ifnull(sum(procured_price_daily*eggs_sold_daily_typewise)/sum(eggs_sold_daily_typewise),0) AS Procured_Price_Brown_Melted, ifnull(round((sum(sales_daily_typewise)-sum(eggs_sold_daily_typewise*procured_price_daily))/sum(eggs_sold_daily_typewise), 2),0) AS Loss_Or_Profit_per_Egg_Brown_Melted, ifnull(round(sum(sales_daily_typewise)-sum(eggs_sold_daily_typewise*procured_price_daily), 0),0) AS Loss_Or_Profit_Brown_Melted FROM maplemonk.ub_log WHERE category = \'Brown\' AND egg_type = \'Melted\' GROUP BY region, DATE_TRUNC(\'DAY\', delivery_date) )ff on aa.delivery_date = ff.delivery_date and aa.region =ff.region left join ( SELECT region AS region, DATE_TRUNC(\'DAY\', delivery_date) AS delivery_date, ifnull(round(SUM(sales_daily_typewise), 0),0) AS Sales_White_Melted, ifnull(round(SUM(eggs_sold_daily_typewise), 0),0) AS Eggs_Sold_White_Melted, ifnull(round(sum(sales_daily_typewise)/sum(eggs_sold_daily_typewise), 2),0) AS Landing_SP_White_Melted, ifnull(sum(procured_price_daily*eggs_sold_daily_typewise)/sum(eggs_sold_daily_typewise),0) AS Procured_Price_White_Melted, ifnull(round((sum(sales_daily_typewise)-sum(eggs_sold_daily_typewise*procured_price_daily))/sum(eggs_sold_daily_typewise), 2),0) AS Loss_Or_Profit_per_Egg_White_Melted, ifnull(round(sum(sales_daily_typewise)-sum(eggs_sold_daily_typewise*procured_price_daily), 0),0) AS Loss_Or_Profit_White_Melted FROM maplemonk.ub_log WHERE category = \'White\' AND egg_type = \'Melted\' GROUP BY region, DATE_TRUNC(\'DAY\', delivery_date) )gg on aa.delivery_date = gg.delivery_date and aa.region = gg.region left join ( SELECT region AS region, DATE_TRUNC(\'DAY\', delivery_date) AS delivery_date, ifnull(round(SUM(sales_daily_typewise), 0),0) AS Sales_White_Normal, ifnull(round(SUM(eggs_sold_daily_typewise), 0),0) AS Eggs_Sold_White_Normal, ifnull(round(sum(sales_daily_typewise)/sum(eggs_sold_daily_typewise), 2),0) AS Landing_SP_White_Normal, ifnull(sum(procured_price_daily*eggs_sold_daily_typewise)/sum(eggs_sold_daily_typewise),0) AS Procured_Price_White_Normal, ifnull(round((sum(sales_daily_typewise)-sum(eggs_sold_daily_typewise*procured_price_daily))/sum(eggs_sold_daily_typewise), 2),0) AS Loss_Or_Profit_per_Egg_White_Normal, ifnull(round(sum(sales_daily_typewise)-sum(eggs_sold_daily_typewise*procured_price_daily), 0),0) AS Loss_Or_Profit_White_Normal FROM maplemonk.ub_log WHERE category = \'White\' AND egg_type = \'Normal\' GROUP BY region, DATE_TRUNC(\'DAY\', delivery_date) )hh on aa.delivery_date = hh.delivery_date and aa.region = hh.region left join ( SELECT region AS region, DATE_TRUNC(\'DAY\', delivery_date) AS delivery_date, ifnull(round(SUM(sales_daily_typewise), 0),0) AS Sales_Brown_Normal, ifnull(round(SUM(eggs_sold_daily_typewise), 0),0) AS Eggs_Sold_Brown_Normal, ifnull(round(sum(sales_daily_typewise)/sum(eggs_sold_daily_typewise), 2),0) AS Landing_SP_Brown_Normal, ifnull(sum(procured_price_daily*eggs_sold_daily_typewise)/sum(eggs_sold_daily_typewise),0) AS Procured_Price_Brown_Normal, ifnull(round((sum(sales_daily_typewise)-sum(eggs_sold_daily_typewise*procured_price_daily))/sum(eggs_sold_daily_typewise), 2),0) AS Loss_Or_Profit_per_Egg_Brown_Normal, ifnull(round(sum(sales_daily_typewise)-sum(eggs_sold_daily_typewise*procured_price_daily), 0),0) AS Loss_Or_Profit_Brown_Normal FROM maplemonk.ub_log WHERE category = \'Brown\' AND egg_type = \'Normal\' GROUP BY region, DATE_TRUNC(\'DAY\', delivery_date) )ii on aa.delivery_date = ii.delivery_date and aa.region = ii.region left join ( SELECT region AS region, DATE_TRUNC(\'DAY\', delivery_date) AS delivery_date, ifnull(round(SUM(sales_daily_typewise), 0),0) AS Sales_Brown_Replaced, ifnull(round(SUM(eggs_sold_daily_typewise), 0),0) AS Eggs_Sold_Brown_Replaced, ifnull(round(sum(sales_daily_typewise)/sum(eggs_sold_daily_typewise), 2),0) AS Landing_SP_Brown_Replaced, ifnull(sum(procured_price_daily*eggs_sold_daily_typewise)/sum(eggs_sold_daily_typewise),0) AS Procured_Price_Brown_Replaced, ifnull(round((sum(sales_daily_typewise)-sum(eggs_sold_daily_typewise*procured_price_daily))/sum(eggs_sold_daily_typewise), 2),0) AS Loss_Or_Profit_per_Egg_Brown_Replaced, ifnull(round(sum(sales_daily_typewise)-sum(eggs_sold_daily_typewise*procured_price_daily), 0),0) AS Loss_Or_Profit_Brown_Replaced FROM maplemonk.ub_log WHERE category = \'Brown\' AND egg_type = \'Replaced\' GROUP BY region, DATE_TRUNC(\'DAY\', delivery_date) )jj on aa.delivery_date = jj.delivery_date and aa.region = jj.region left join ( SELECT region AS region, DATE_TRUNC(\'DAY\', delivery_date) AS delivery_date, ifnull(round(SUM(sales_daily_typewise), 0),0) AS Sales_White_Replaced, ifnull(round(SUM(eggs_sold_daily_typewise), 0),0) AS Eggs_Sold_White_Replaced, ifnull(round(sum(sales_daily_typewise)/sum(eggs_sold_daily_typewise), 2),0) AS Landing_SP_White_Replaced, ifnull(sum(procured_price_daily*eggs_sold_daily_typewise)/sum(eggs_sold_daily_typewise),0) AS Procured_Price_White_Replaced, ifnull(round((sum(sales_daily_typewise)-sum(eggs_sold_daily_typewise*procured_price_daily))/sum(eggs_sold_daily_typewise), 2),0) AS Loss_Or_Profit_per_Egg_White_Replaced, ifnull(round(sum(sales_daily_typewise)-sum(eggs_sold_daily_typewise*procured_price_daily), 0),0) AS Loss_Or_Profit_White_Replaced FROM maplemonk.ub_log WHERE category = \'White\' AND egg_type = \'Replaced\' GROUP BY region, DATE_TRUNC(\'DAY\', delivery_date) )kk on aa.delivery_date = kk.delivery_date and aa.region = kk.region left join ( SELECT region AS region, DATE_TRUNC(\'DAY\', delivery_date) AS delivery_date, ifnull(round(SUM(sales_daily_typewise), 0),0) AS Sales_White_Darjan, ifnull(round(SUM(eggs_sold_daily_typewise), 0),0) AS Eggs_Sold_White_Darjan, ifnull(case when sum(eggs_sold_daily_typewise) = 0 then 0 else round(sum(sales_daily_typewise)/sum(eggs_sold_daily_typewise), 2) end,0) AS Landing_SP_White_Darjan, ifnull(case when sum(eggs_sold_daily_typewise) = 0 then 0 else sum(procured_price_daily*eggs_sold_daily_typewise)/sum(eggs_sold_daily_typewise) end ,0) AS Procured_Price_White_Darjan, ifnull(case when sum(eggs_sold_daily_typewise) = 0 then 0 else round((sum(sales_daily_typewise)-sum(eggs_sold_daily_typewise*procured_price_daily))/sum(eggs_sold_daily_typewise), 2) end,0) AS Loss_Or_Profit_per_Egg_White_Darjan, ifnull(round(sum(sales_daily_typewise)-sum(eggs_sold_daily_typewise*procured_price_daily), 0),0) AS Loss_Or_Profit_White_Darjan FROM maplemonk.ub_log WHERE category = \'White\' AND egg_type = \'Darjan\' GROUP BY region, DATE_TRUNC(\'DAY\', delivery_date) )ab on aa.delivery_date = ab.delivery_date and aa.region = ab.region left join ( select DISTINCT tt.area_classification, tt.sku, tt.packaging_cost_per_egg from ( select area, CASE when area in (\'NCR-ON-MT\' , \'NCR-Others\') then \'NCR\' when area in (\'Bangalore-ON-MT\', \'Bangalore-Others\') then \'Bangalore\' when area in (\'East-ON-MT\', \'East-Others\') then \'East\' else \'NA\' end as area_classification, sku, packaging_cost_per_egg from eggozdb.maplemonk.pkg_cost_Packaging_cost where sku = \'30WD\' ) tt )zz on aa.region = zz.area_classification where aa.region = \'NCR\' )xx full outer join ( Select distinct to_date(mm.Date, \'dd/mm/yyyy\' ) as Date , sum(mm.Cost) over (partition by mm.Date order by mm.Date) as Last_Mile_Cost , sum(mm.Unloading_Cost) over (partition by mm.Date order by mm.Date) as Last_Mile_Unloading_Cost, mm.Beat from ( select Date , \"Cost/Egg_Out\" as Cost_Per_Egg, Area, Cost, Total_Eggs_Out , \"GT/MT\" ,Unloading_Cost , beat from maplemonk.transport_costs_last___mid_mile where beat = \'Unbranded\' ) mm )ll on xx.delivery_date = ll.Date left join ( select distinct to_date(Date, \'dd/mm/yyyy\') as Date15, sum(\"Picked_up_Qty(In_Eggs)\") over (partition by date order by Date) as First_Mile_Eggs , sum(Total_Expense) over (partition by date order by Date) as First_Mile_Cost from maplemonk.transport_costs_FM_Vehicle_Details_after_1st_April order by to_date(Date, \'dd/mm/yyyy\' ) )mm on xx.delivery_date = mm.Date15 ;",
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
                        