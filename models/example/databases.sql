{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE eggozdb.maplemonk.PO_Expected_Target_Final as select lo.*, ve.eggs_replaced,ve.eggs_return,ve.no_of_days,ve.days_in_month,ve.eggs_sold from (Select fu.* , ck.revenue_target from ( Select xx.Date2 as PO_DATE, dayname(xx.Date2) as Day, yy.PO_AMOUNT as PO_AMOUNT, yy.Total_POs as Total_POs, yy.SUPPLY_AMOUNT as SUPPLY_AMOUNT, yy.Amount_Fillrate as Amount_Fillrate, yy.PO_Egg_Count as PO_Egg_Count,yy.Supply_Egg_Count as Supply_Egg_Count, yy.Egg_Fillrate as Egg_Fillrate,yy.TAT as TAT, xx.parent_name Parent_name,xx.PO_Expected as PO_Expected from ( select fd.Parent_Name, fd.Date2, fd.day2, coalesce(gd.PO_expected,fd.PO_expected) as PO_Expected from (Select bb.Date2 , bb.Day2 , bb.Parent_Name, aa.PO_Expected from (Select Day, Parent_name, area_classification, PO_Expected from maplemonk.target_PO_Expected_Maplemonk ) aa inner join ( Select DISTINCT qq.DATE as Date2 , pp.Parent_name as Parent_Name, qq.DayName as Day2 from (Select DISTINCT Parent_name , PO_Expected from maplemonk.target_PO_Expected_Maplemonk ) pp cross JOIN (SELECT distinct DECODE(EXTRACT (\'dayofweek_iso\',DATE), 1, \'Monday\', 2, \'Tuesday\', 3, \'Wednesday\', 4, \'Thursday\', 5, \'Friday\', 6, \'Saturday\', 7, \'Sunday\') AS DayName , DATE, area_classification from eggozdb.maplemonk.Date_area_dim WHERE year(date)>=2021 and date <=getdate() )qq )bb on aa.Day=bb.Day2 and aa.Parent_name= bb.Parent_Name where aa.area_classification = \'NCR-ON-MT\')gd right join (Select DISTINCT qq.DATE as Date2 , pp.Parent_name as Parent_Name, qq.DayName as Day2, 0 as PO_Expected from (Select DISTINCT Parent_name , PO_Expected from maplemonk.target_PO_Expected_Maplemonk ) pp cross JOIN (SELECT distinct DECODE(EXTRACT (\'dayofweek_iso\',DATE), 1, \'Monday\', 2, \'Tuesday\', 3, \'Wednesday\', 4, \'Thursday\', 5, \'Friday\', 6, \'Saturday\', 7, \'Sunday\') AS DayName , DATE, area_classification from eggozdb.maplemonk.Date_area_dim WHERE year(date)>=2021 and date <=getdate() )qq )fd on gd.Date2=fd.Date2 and gd.Parent_name=fd.Parent_name ) xx left join ( SELECT parent_name AS parent_name, DATE_TRUNC(\'DAY\', po_date) AS po_date, COUNT (DISTINCT po_no) AS Total_POs, sum(po_amount) AS PO_AMOUNT, sum(supply_amount) AS SUPPLY_AMOUNT, sum(supply_amount)/sum(po_amount) AS Amount_Fillrate, sum(po_egg_count) AS PO_Egg_Count, sum(supply_egg_count) AS Supply_Egg_Count, sum(supply_egg_count)/sum(po_egg_count) AS Egg_Fillrate, AVG(datediff(\'day\', po_date, delivery_date)) AS TAT FROM maplemonk.demand_supply_po_list WHERE lower(area_classification) LIKE lower(\'%ncr-on-mt%\') GROUP BY parent_name, DATE_TRUNC(\'DAY\', po_date) ) yy on xx.Date2 = yy.po_date and xx.Parent_Name = yy.parent_name )fu left join (select * from eggozdb.maplemonk.bi_parent_wise_target where month =\'4\' and area_classification = \'NCR-ON-MT\')ck on fu.parent_name=ck.parent )lo left join ( select date, area, parent_retailer_name, sum(net_sales) net_sales, sum(eggs_sold) eggs_sold, sum(eggs_replaced) eggs_replaced, sum(eggs_return) eggs_return, sum(amount_return) amount_return, datediff(\'day\',date_trunc(\'month\',date),date)+1 no_of_days , datediff(\'day\',date_trunc(\'month\',date), last_day(date,\'month\'))+1 days_in_month from eggozdb.maplemonk.summary_reporting_table_beat_retailer where area= \'NCR-ON-MT\' group by date, area, parent_retailer_name )ve on lo.PO_DATE = ve.date and lo.Parent_Name = ve.parent_retailer_name ; CREATE OR REPLACE TABLE eggozdb.maplemonk.PO_Expected_Target_Final_Bangalore as select lo.*, ve.eggs_replaced,ve.eggs_return,ve.no_of_days,ve.days_in_month,ve.eggs_sold from (Select fu.* , ck.revenue_target from ( Select xx.Date2 as PO_DATE, dayname(xx.Date2) as Day, yy.PO_AMOUNT as PO_AMOUNT, yy.Total_POs as Total_POs, yy.SUPPLY_AMOUNT as SUPPLY_AMOUNT, yy.Amount_Fillrate as Amount_Fillrate, yy.PO_Egg_Count as PO_Egg_Count,yy.Supply_Egg_Count as Supply_Egg_Count, yy.Egg_Fillrate as Egg_Fillrate,yy.TAT as TAT, xx.parent_name Parent_name,xx.PO_Expected as PO_Expected from ( select fd.Parent_Name, fd.Date2, fd.day2, coalesce(gd.PO_expected,fd.PO_expected) as PO_Expected from (Select bb.Date2 , bb.Day2 , bb.Parent_Name, aa.PO_Expected from (Select Day, Parent_name, area_classification, PO_Expected from maplemonk.target_PO_Expected_Maplemonk ) aa inner join ( Select DISTINCT qq.DATE as Date2 , pp.Parent_name as Parent_Name, qq.DayName as Day2 from (Select DISTINCT Parent_name , PO_Expected from maplemonk.target_PO_Expected_Maplemonk ) pp cross JOIN (SELECT distinct DECODE(EXTRACT (\'dayofweek_iso\',DATE), 1, \'Monday\', 2, \'Tuesday\', 3, \'Wednesday\', 4, \'Thursday\', 5, \'Friday\', 6, \'Saturday\', 7, \'Sunday\') AS DayName , DATE, area_classification from eggozdb.maplemonk.Date_area_dim WHERE year(date)>=2021 and date <=getdate() )qq )bb on aa.Day=bb.Day2 and aa.Parent_name= bb.Parent_Name where aa.area_classification = \'Bangalore-ON-MT\')gd right join (Select DISTINCT qq.DATE as Date2 , pp.Parent_name as Parent_Name, qq.DayName as Day2, 0 as PO_Expected from (Select DISTINCT Parent_name , PO_Expected from maplemonk.target_PO_Expected_Maplemonk ) pp cross JOIN (SELECT distinct DECODE(EXTRACT (\'dayofweek_iso\',DATE), 1, \'Monday\', 2, \'Tuesday\', 3, \'Wednesday\', 4, \'Thursday\', 5, \'Friday\', 6, \'Saturday\', 7, \'Sunday\') AS DayName , DATE, area_classification from eggozdb.maplemonk.Date_area_dim WHERE year(date)>=2021 and date <=getdate() )qq )fd on gd.Date2=fd.Date2 and gd.Parent_name=fd.Parent_name ) xx left join ( SELECT parent_name AS parent_name, DATE_TRUNC(\'DAY\', po_date) AS po_date, COUNT (DISTINCT po_no) AS Total_POs, sum(po_amount) AS PO_AMOUNT, sum(supply_amount) AS SUPPLY_AMOUNT, sum(supply_amount)/sum(po_amount) AS Amount_Fillrate, sum(po_egg_count) AS PO_Egg_Count, sum(supply_egg_count) AS Supply_Egg_Count, sum(supply_egg_count)/sum(po_egg_count) AS Egg_Fillrate, AVG(datediff(\'day\', po_date, delivery_date)) AS TAT FROM maplemonk.demand_supply_po_list WHERE lower(area_classification) LIKE lower(\'%bangalore-on-mt%\') GROUP BY parent_name, DATE_TRUNC(\'DAY\', po_date) ) yy on xx.Date2 = yy.po_date and xx.Parent_Name = yy.parent_name )fu left join (select * from eggozdb.maplemonk.bi_parent_wise_target where month =\'4\' and area_classification = \'Bangalore-ON-MT\')ck on fu.parent_name=ck.parent )lo left join ( select date, area, parent_retailer_name, sum(net_sales) net_sales, sum(eggs_sold) eggs_sold, sum(eggs_replaced) eggs_replaced, sum(eggs_return) eggs_return, sum(amount_return) amount_return, datediff(\'day\',date_trunc(\'month\',date),date)+1 no_of_days , datediff(\'day\',date_trunc(\'month\',date), last_day(date,\'month\'))+1 days_in_month from eggozdb.maplemonk.summary_reporting_table_beat_retailer where area= \'Bangalore-ON-MT\' group by date, area, parent_retailer_name )ve on lo.PO_DATE = ve.date and lo.Parent_Name = ve.parent_retailer_name ; CREATE OR REPLACE TABLE eggozdb.maplemonk.PO_Expected_Target_Final_East as select lo.*, ve.eggs_replaced,ve.eggs_return,ve.no_of_days,ve.days_in_month,ve.eggs_sold from (Select fu.* , ck.revenue_target from ( Select xx.Date2 as PO_DATE, dayname(xx.Date2) as Day, yy.PO_AMOUNT as PO_AMOUNT, yy.Total_POs as Total_POs, yy.SUPPLY_AMOUNT as SUPPLY_AMOUNT, yy.Amount_Fillrate as Amount_Fillrate, yy.PO_Egg_Count as PO_Egg_Count,yy.Supply_Egg_Count as Supply_Egg_Count, yy.Egg_Fillrate as Egg_Fillrate,yy.TAT as TAT, xx.parent_name Parent_name,xx.PO_Expected as PO_Expected from ( select fd.Parent_Name, fd.Date2, fd.day2, coalesce(gd.PO_expected,fd.PO_expected) as PO_Expected from (Select bb.Date2 , bb.Day2 , bb.Parent_Name, aa.PO_Expected from (Select Day, Parent_name, area_classification, PO_Expected from maplemonk.target_PO_Expected_Maplemonk ) aa inner join ( Select DISTINCT qq.DATE as Date2 , pp.Parent_name as Parent_Name, qq.DayName as Day2 from (Select DISTINCT Parent_name , PO_Expected from maplemonk.target_PO_Expected_Maplemonk ) pp cross JOIN (SELECT distinct DECODE(EXTRACT (\'dayofweek_iso\',DATE), 1, \'Monday\', 2, \'Tuesday\', 3, \'Wednesday\', 4, \'Thursday\', 5, \'Friday\', 6, \'Saturday\', 7, \'Sunday\') AS DayName , DATE, area_classification from eggozdb.maplemonk.Date_area_dim WHERE year(date)>=2021 and date <=getdate() )qq )bb on aa.Day=bb.Day2 and aa.Parent_name= bb.Parent_Name where aa.area_classification = \'East-ON-MT\')gd right join (Select DISTINCT qq.DATE as Date2 , pp.Parent_name as Parent_Name, qq.DayName as Day2, 0 as PO_Expected from (Select DISTINCT Parent_name , PO_Expected from maplemonk.target_PO_Expected_Maplemonk ) pp cross JOIN (SELECT distinct DECODE(EXTRACT (\'dayofweek_iso\',DATE), 1, \'Monday\', 2, \'Tuesday\', 3, \'Wednesday\', 4, \'Thursday\', 5, \'Friday\', 6, \'Saturday\', 7, \'Sunday\') AS DayName , DATE, area_classification from eggozdb.maplemonk.Date_area_dim WHERE year(date)>=2021 and date <=getdate() )qq )fd on gd.Date2=fd.Date2 and gd.Parent_name=fd.Parent_name ) xx left join ( SELECT parent_name AS parent_name, DATE_TRUNC(\'DAY\', po_date) AS po_date, COUNT (DISTINCT po_no) AS Total_POs, sum(po_amount) AS PO_AMOUNT, sum(supply_amount) AS SUPPLY_AMOUNT, sum(supply_amount)/sum(po_amount) AS Amount_Fillrate, sum(po_egg_count) AS PO_Egg_Count, sum(supply_egg_count) AS Supply_Egg_Count, sum(supply_egg_count)/sum(po_egg_count) AS Egg_Fillrate, AVG(datediff(\'day\', po_date, delivery_date)) AS TAT FROM maplemonk.demand_supply_po_list WHERE lower(area_classification) LIKE lower(\'%east-on-mt%\') GROUP BY parent_name, DATE_TRUNC(\'DAY\', po_date) ) yy on xx.Date2 = yy.po_date and xx.Parent_Name = yy.parent_name )fu left join (select * from eggozdb.maplemonk.bi_parent_wise_target where month =\'4\' and area_classification = \'East-ON-MT\')ck on fu.parent_name=ck.parent )lo left join ( select date, area, parent_retailer_name, sum(net_sales) net_sales, sum(eggs_sold) eggs_sold, sum(eggs_replaced) eggs_replaced, sum(eggs_return) eggs_return, sum(amount_return) amount_return, datediff(\'day\',date_trunc(\'month\',date),date)+1 no_of_days , datediff(\'day\',date_trunc(\'month\',date), last_day(date,\'month\'))+1 days_in_month from eggozdb.maplemonk.summary_reporting_table_beat_retailer where area= \'East-ON-MT\' group by date, area, parent_retailer_name )ve on lo.PO_DATE = ve.date and lo.Parent_Name = ve.parent_retailer_name ; CREATE OR REPLACE TABLE eggozdb.maplemonk.PO_Expected_Target_Final_MP as select lo.*, ve.eggs_replaced,ve.eggs_return,ve.no_of_days,ve.days_in_month,ve.eggs_sold from (Select fu.* , ck.revenue_target from ( Select xx.Date2 as PO_DATE, dayname(xx.Date2) as Day, yy.PO_AMOUNT as PO_AMOUNT, yy.Total_POs as Total_POs, yy.SUPPLY_AMOUNT as SUPPLY_AMOUNT, yy.Amount_Fillrate as Amount_Fillrate, yy.PO_Egg_Count as PO_Egg_Count,yy.Supply_Egg_Count as Supply_Egg_Count, yy.Egg_Fillrate as Egg_Fillrate,yy.TAT as TAT, xx.parent_name Parent_name,xx.PO_Expected as PO_Expected from ( select fd.Parent_Name, fd.Date2, fd.day2, coalesce(gd.PO_expected,fd.PO_expected) as PO_Expected from (Select bb.Date2 , bb.Day2 , bb.Parent_Name, aa.PO_Expected from (Select Day, Parent_name, area_classification, PO_Expected from maplemonk.target_PO_Expected_Maplemonk ) aa inner join ( Select DISTINCT qq.DATE as Date2 , pp.Parent_name as Parent_Name, qq.DayName as Day2 from (Select DISTINCT Parent_name , PO_Expected from maplemonk.target_PO_Expected_Maplemonk ) pp cross JOIN (SELECT distinct DECODE(EXTRACT (\'dayofweek_iso\',DATE), 1, \'Monday\', 2, \'Tuesday\', 3, \'Wednesday\', 4, \'Thursday\', 5, \'Friday\', 6, \'Saturday\', 7, \'Sunday\') AS DayName , DATE, area_classification from eggozdb.maplemonk.Date_area_dim WHERE year(date)>=2021 and date <=getdate() )qq )bb on aa.Day=bb.Day2 and aa.Parent_name= bb.Parent_Name where aa.area_classification = \'MP-ON-MT\')gd right join (Select DISTINCT qq.DATE as Date2 , pp.Parent_name as Parent_Name, qq.DayName as Day2, 0 as PO_Expected from (Select DISTINCT Parent_name , PO_Expected from maplemonk.target_PO_Expected_Maplemonk ) pp cross JOIN (SELECT distinct DECODE(EXTRACT (\'dayofweek_iso\',DATE), 1, \'Monday\', 2, \'Tuesday\', 3, \'Wednesday\', 4, \'Thursday\', 5, \'Friday\', 6, \'Saturday\', 7, \'Sunday\') AS DayName , DATE, area_classification from eggozdb.maplemonk.Date_area_dim WHERE year(date)>=2021 and date <=getdate() )qq )fd on gd.Date2=fd.Date2 and gd.Parent_name=fd.Parent_name ) xx left join ( SELECT parent_name AS parent_name, DATE_TRUNC(\'DAY\', po_date) AS po_date, COUNT (DISTINCT po_no) AS Total_POs, sum(po_amount) AS PO_AMOUNT, sum(supply_amount) AS SUPPLY_AMOUNT, sum(supply_amount)/sum(po_amount) AS Amount_Fillrate, sum(po_egg_count) AS PO_Egg_Count, sum(supply_egg_count) AS Supply_Egg_Count, sum(supply_egg_count)/sum(po_egg_count) AS Egg_Fillrate, AVG(datediff(\'day\', po_date, delivery_date)) AS TAT FROM maplemonk.demand_supply_po_list WHERE lower(area_classification) LIKE lower(\'%mp-on-mt%\') GROUP BY parent_name, DATE_TRUNC(\'DAY\', po_date) ) yy on xx.Date2 = yy.po_date and xx.Parent_Name = yy.parent_name )fu left join (select * from eggozdb.maplemonk.bi_parent_wise_target where month =\'4\' and area_classification = \'MP-ON-MT\')ck on fu.parent_name=ck.parent )lo left join ( select date, area, parent_retailer_name, sum(net_sales) net_sales, sum(eggs_sold) eggs_sold, sum(eggs_replaced) eggs_replaced, sum(eggs_return) eggs_return, sum(amount_return) amount_return, datediff(\'day\',date_trunc(\'month\',date),date)+1 no_of_days , datediff(\'day\',date_trunc(\'month\',date), last_day(date,\'month\'))+1 days_in_month from eggozdb.maplemonk.summary_reporting_table_beat_retailer where area= \'MP-ON-MT\' group by date, area, parent_retailer_name )ve on lo.PO_DATE = ve.date and lo.Parent_Name = ve.parent_retailer_name ; CREATE OR REPLACE TABLE eggozdb.maplemonk.PO_Expected_Target_Final_UP as select lo.*, ve.eggs_replaced,ve.eggs_return,ve.no_of_days,ve.days_in_month,ve.eggs_sold from (Select fu.* , ck.revenue_target from ( Select xx.Date2 as PO_DATE, dayname(xx.Date2) as Day, yy.PO_AMOUNT as PO_AMOUNT, yy.Total_POs as Total_POs, yy.SUPPLY_AMOUNT as SUPPLY_AMOUNT, yy.Amount_Fillrate as Amount_Fillrate, yy.PO_Egg_Count as PO_Egg_Count,yy.Supply_Egg_Count as Supply_Egg_Count, yy.Egg_Fillrate as Egg_Fillrate,yy.TAT as TAT, xx.parent_name Parent_name,xx.PO_Expected as PO_Expected from ( select fd.Parent_Name, fd.Date2, fd.day2, coalesce(gd.PO_expected,fd.PO_expected) as PO_Expected from (Select bb.Date2 , bb.Day2 , bb.Parent_Name, aa.PO_Expected from (Select Day, Parent_name, area_classification, PO_Expected from maplemonk.target_PO_Expected_Maplemonk ) aa inner join ( Select DISTINCT qq.DATE as Date2 , pp.Parent_name as Parent_Name, qq.DayName as Day2 from (Select DISTINCT Parent_name , PO_Expected from maplemonk.target_PO_Expected_Maplemonk ) pp cross JOIN (SELECT distinct DECODE(EXTRACT (\'dayofweek_iso\',DATE), 1, \'Monday\', 2, \'Tuesday\', 3, \'Wednesday\', 4, \'Thursday\', 5, \'Friday\', 6, \'Saturday\', 7, \'Sunday\') AS DayName , DATE, area_classification from eggozdb.maplemonk.Date_area_dim WHERE year(date)>=2021 and date <=getdate() )qq )bb on aa.Day=bb.Day2 and aa.Parent_name= bb.Parent_Name where aa.area_classification = \'UP-ON-MT\')gd right join (Select DISTINCT qq.DATE as Date2 , pp.Parent_name as Parent_Name, qq.DayName as Day2, 0 as PO_Expected from (Select DISTINCT Parent_name , PO_Expected from maplemonk.target_PO_Expected_Maplemonk ) pp cross JOIN (SELECT distinct DECODE(EXTRACT (\'dayofweek_iso\',DATE), 1, \'Monday\', 2, \'Tuesday\', 3, \'Wednesday\', 4, \'Thursday\', 5, \'Friday\', 6, \'Saturday\', 7, \'Sunday\') AS DayName , DATE, area_classification from eggozdb.maplemonk.Date_area_dim WHERE year(date)>=2021 and date <=getdate() )qq )fd on gd.Date2=fd.Date2 and gd.Parent_name=fd.Parent_name ) xx left join ( SELECT parent_name AS parent_name, DATE_TRUNC(\'DAY\', po_date) AS po_date, COUNT (DISTINCT po_no) AS Total_POs, sum(po_amount) AS PO_AMOUNT, sum(supply_amount) AS SUPPLY_AMOUNT, sum(supply_amount)/sum(po_amount) AS Amount_Fillrate, sum(po_egg_count) AS PO_Egg_Count, sum(supply_egg_count) AS Supply_Egg_Count, sum(supply_egg_count)/sum(po_egg_count) AS Egg_Fillrate, AVG(datediff(\'day\', po_date, delivery_date)) AS TAT FROM maplemonk.demand_supply_po_list WHERE lower(area_classification) LIKE lower(\'%up-on-mt%\') GROUP BY parent_name, DATE_TRUNC(\'DAY\', po_date) ) yy on xx.Date2 = yy.po_date and xx.Parent_Name = yy.parent_name )fu left join (select * from eggozdb.maplemonk.bi_parent_wise_target where month =\'4\' and area_classification = \'UP-ON-MT\')ck on fu.parent_name=ck.parent )lo left join ( select date, area, parent_retailer_name, sum(net_sales) net_sales, sum(eggs_sold) eggs_sold, sum(eggs_replaced) eggs_replaced, sum(eggs_return) eggs_return, sum(amount_return) amount_return, datediff(\'day\',date_trunc(\'month\',date),date)+1 no_of_days , datediff(\'day\',date_trunc(\'month\',date), last_day(date,\'month\'))+1 days_in_month from eggozdb.maplemonk.summary_reporting_table_beat_retailer where area= \'UP-ON-MT\' group by date, area, parent_retailer_name )ve on lo.PO_DATE = ve.date and lo.Parent_Name = ve.parent_retailer_name ; create or replace table eggozdb.maplemonk.Beatwise_Profitability_KPI_Final as Select pa.date, pa.area, pa.beat_number_original,pa.Eggs_Sold,pa.Eggs_Sold_White,pa.Net_Sales,pa.eggs_returned, pa.Eggs_Sold_Brown, pa.Eggs_Sold_Nutra, pa.Eggs_Sold_FR, pa.Revenue, pa.Collections, pa.Eggs_Replaced, pa.Eggs_Promo,pa.Eggs_Replaced_White, pa.Eggs_Replaced_Brown, pa.Eggs_Replaced_Nutra, pa.Eggs_Return_White, pa.Eggs_Return_Brown, pa.Eggs_Return_Nutra, pb.branded_shortfall,pb.NON_BRANDED_SHORTFALL, pb.LESS_SUPPLIED, pb.LESS_SUPPLIED_AFTER_COMMITMENT, pb.SOLD_VS_SUPPLY,pb.SOLD_AND_REPLACEMENT_VS_SUPPLY,pb.FRESH_RETURNED_TO_WAREHOUSE, pb.demand, pb.supply, pb.out, pb.sold, pb.Damage,pb.transfer, pb.promo, pb.Fresh_In, pb.Old_In ,pi.Last_Mile_cost, pc.No_of_Visits, pc.active_Retailer_visits,pc.other_retailer_vists, pc.Total_Onboarded, pc.Utilization, pc.Eggs_Sold_Visit, pc.Returned, pc.Replaced, pe.No_of_Visits_Bill, pe.active_Retailer_visits_Bill, pe.other_retailer_vists_Bill,pe.Total_Onboarded_Bill , pe.Utilization_Bill, pe.Eggs_Sold_Bill,pe.Returned_Bill , pe.Replaced_Bill,pg.No_of_Vists_BU, pg.active_Retailer_visits_BU, pg.other_retailer_vists_BU, pg.Total_Onboarded_BU, pg.Utilization_BU, pg.Eggs_Sold_BU, pg.Returned_BU , pg.Replaced_BU , pi.Last_Mile_Eggs_Out, pi.last_mile_unloading_cost, pj.Total_Procurred_Egg, pj.Amount, pj.Procurred_Egg_White,pj.Amount_White, pj.Procurred_Egg_Brown, pj.Amount_Brown, pj.Procurred_Egg_Nutra, pj.Amount_Nutra, pj.Proc_Price, pj.Procured_Price_White, pj.Procured_Price_Brown, pj.Procured_Price_Nutra, pj.Packaged_Eggs,pj.Packaging_Cost, pk.First_Mile_Eggs, pk.First_Mile_Cost, pl.No_of_Days, pl.Days_In_Month , kk.New_retailers_onboarded , jj.Total_Primary_Retailers_Beat, ik.Mid_Mile_Cost,ik.Mid_Mile_Eggs_Out from ( SELECT DATE_TRUNC(\'DAY\', date) AS date, area AS area, beat_number_original AS beat_number_original, case when sum(eggs_sold) is null then 0 else sum(eggs_sold) end AS Eggs_Sold, sum(eggs_sold_white) AS Eggs_Sold_White, sum(eggs_sold_brown) AS Eggs_Sold_Brown, sum(eggs_sold_nutra) AS Eggs_Sold_Nutra, sum(eggs_return_Brown) as Eggs_Return_Brown, sum(eggs_return_white) as Eggs_Return_White, sum(eggs_return_nutra) as Eggs_Return_Nutra, sum(eggs_replaced_White) as Eggs_Replaced_White, sum(eggs_replaced_Brown) as Eggs_Replaced_Brown, sum(eggs_replaced_Nutra) as Eggs_Replaced_Nutra, sum(eggs_sold_fr) AS Eggs_Sold_FR, sum(net_sales) AS Revenue, sum(net_sales)-sum(amount_return) as Net_Sales, sum(eggs_return) as eggs_returned, case when sum(net_sales)=0 then 0 else sum(collections)/sum(net_sales) end AS Collection_Per, sum(collections) AS Collections, sum(eggs_replaced) AS Eggs_Replaced, case when sum(eggs_sold)=0 then 0 else sum(eggs_replaced)/sum(eggs_sold) end AS Replacement_Per, case when sum(eggs_sold_white)=0 then 0 else sum(eggs_replaced_white)/sum(eggs_sold_white) end AS Eggs_Replacement_White_Per, case when sum(eggs_sold_brown)=0 then 0 else sum(eggs_replaced_brown)/sum(eggs_sold_brown) end AS Eggs_Replacement_Brown_Per, case when sum(eggs_sold_nutra)=0 then 0 else sum(eggs_replaced_nutra)/sum(eggs_sold_nutra) end AS Eggs_Replacement_Nutra_Per, case when sum(eggs_sold_fr)=0 then 0 else sum(eggs_replaced_fr)/sum(eggs_sold_fr) end AS Eggs_Replacement_FR_Per, case when sum(eggs_sold)=0 then 0 else sum(eggs_promo)/sum(eggs_sold) end AS Promo_Per, sum(eggs_promo) AS Eggs_Promo FROM maplemonk.summary_reporting_table_beat_retailer WHERE area IN (\'Delhi-GT\', \'Gurgaon-GT\', \'Noida-GT\', \'NCR-OF-MT\', \'NCR-ON-MT\') GROUP BY area, DATE_TRUNC(\'DAY\', date), beat_number_original )pa left join ( SELECT DATE_TRUNC(\'DAY\', date) AS date2, area AS area, beat_number as beat_number, sum(out)-sum(sold)-sum(replacement)+sum(transfer)-sum(promo)-sum(fresh_in) AS BRANDED_SHORTFALL, SUM(replacement)+SUM(return)-sum(damage)-SUM(old_in) AS NON_BRANDED_SHORTFALL, case when sum(demand)=0 then 0 ELSE -1*(sum(demand) - sum(out))/sum(demand) END AS LESS_SUPPLIED, case when sum(supply)=0 then 0 ELSE -1*(sum(supply) - sum(out))/sum(supply) END AS LESS_SUPPLIED_AFTER_COMMITMENT, case when sum(out)=0 then 0 ELSE sum(sold)/sum(out) END AS SOLD_VS_SUPPLY, case when sum(out)=0 then 0 ELSE (sum(sold)+sum(replacement))/sum(out) END AS SOLD_AND_REPLACEMENT_VS_SUPPLY, case when sum(out)=0 then 0 ELSE sum(fresh_in)/sum(out) END AS FRESH_RETURNED_TO_WAREHOUSE, sum(demand) AS Demand, sum(supply) AS Supply, sum(out) AS Out, sum(sold) AS Sold, sum(damage) AS Damage, sum(return) AS Return, sum(replacement) AS Replacement, sum(transfer) AS Transfer, sum(promo) AS Promo, sum(fresh_in) AS Fresh_In, sum(old_in) AS Old_In FROM maplemonk.beat_material_kpi GROUP BY DATE_TRUNC(\'DAY\', date), area, beat_number )pb on pa.date = pb.date2 and pa.area = pb.area and pa.beat_number_original = pb.beat_number left join ( SELECT DATE_TRUNC(\'DAY\', date) AS date, beat_number_original AS beat_number_original, area AS area, count(DISTINCT retailer_name) AS No_of_Visits, count(DISTINCT case when onboarding_status = \'Active\' then retailer_name end) AS active_Retailer_visits, count (distinct retailer_name) - count(DISTINCT case when onboarding_status = \'Active\' then retailer_name end) AS other_retailer_vists, AVG(total_onboarded) AS Total_Onboarded, count(DISTINCT(retailer_name))/avg(total_onboarded) AS Utilization, sum(sold) AS Eggs_Sold_Visit, sum(returned) AS Returned, sum(replaced) AS Replaced FROM maplemonk.beat_utilization_test GROUP BY DATE_TRUNC(\'DAY\', date), beat_number_original, area )pc on pa.date = pc.date and pa.area = pc.area and pa.beat_number_original = pc.beat_number_original left join ( SELECT DATE_TRUNC(\'DAY\', date) AS date, beat_number_original AS beat_number_original, area AS area, count(DISTINCT retailer_name) AS No_of_Visits_Bill, count(DISTINCT case when onboarding_status = \'Active\' then retailer_name end) AS active_Retailer_visits_Bill, count (distinct retailer_name) - count(DISTINCT case when onboarding_status = \'Active\' then retailer_name end) AS other_retailer_vists_Bill, AVG(total_onboarded) AS Total_Onboarded_Bill, count(DISTINCT(retailer_name))/avg(total_onboarded) AS Utilization_Bill, sum(sold) AS Eggs_Sold_Bill, sum(returned) AS Returned_Bill, sum(replaced) AS Replaced_Bill FROM maplemonk.beat_utilization_test where sold > 0 GROUP BY DATE_TRUNC(\'DAY\', date), beat_number_original, area )pe on pa.date = pe.date and pa.area = pe.area and pa.beat_number_original = pe.beat_number_original left join ( SELECT DATE_TRUNC(\'DAY\', date) AS date, beat_number_original AS beat_number_original, area AS area, count(DISTINCT retailer_name) AS No_of_Vists_BU, count(DISTINCT case when onboarding_status = \'Active\' then retailer_name end) AS active_Retailer_visits_BU, count (distinct retailer_name) - count(DISTINCT case when onboarding_status = \'Active\' then retailer_name end) AS other_retailer_vists_BU, AVG(total_onboarded) AS Total_Onboarded_BU, count(DISTINCT(retailer_name))/avg(total_onboarded) AS Utilization_BU, sum(sold) AS Eggs_Sold_BU, sum(returned) AS Returned_BU, sum(replaced) AS Replaced_BU FROM maplemonk.beat_utilization_test GROUP BY DATE_TRUNC(\'DAY\', date), beat_number_original, area HAVING ((COUNT(DISTINCT(retailer_name))>4)) )pg on pa.date = pg.date and pa.area = pg.area and pa.beat_number_original = pg.beat_number_original left join ( select distinct yu.Date13, yu.beat,sum(yu.Last_Mile_cost) over (partition by yu.Date13,yu.area_classification order by yu.Date13,yu.area_classification) as Last_Mile_Cost , sum(yu.last_mile_eggs_out) over (partition by yu.Date13,yu.area_classification order by yu.Date13,yu.area_classification) as Last_Mile_Eggs_Out, sum(yu.last_mile_unloading_cost) over (partition by yu.Date13,yu.area_classification order by yu.Date13,yu.area_classification) as Last_mile_Unloading_cost , yu.Area_classification from ( select DISTINCT to_date(pp.date, \'dd/mm/yyyy\') as Date13, pp.beat, sum(pp.Cost) over (partition by pp.date,pp.\"GT/MT\" order by pp.date,pp.\"GT/MT\") as Last_Mile_Cost, sum(pp.Total_Eggs_Out) over (partition by pp.date,pp.\"GT/MT\" order by pp.date, pp.\"GT/MT\") as Last_Mile_Eggs_Out , pp.\"GT/MT\" , sum(pp.Unloading_Cost) over (partition by pp.date,pp.\"GT/MT\" order by pp.date, pp.\"GT/MT\") as Last_mile_Unloading_cost , case when \"GT/MT\" in (\'D-DST\',\'DGT\') then \'Delhi-GT\' when \"GT/MT\" in (\'G-DST\',\'GGT\') then \'Gurgaon-GT\' when \"GT/MT\" in (\'N-DST\', \'NGT\') then \'Noida-GT\' when \"GT/MT\" in (\'ON-MT\') then \'NCR-ON-MT\' when \"GT/MT\" in (\'OFF-MT\') then \'NCR-OF-MT\' when \"GT/MT\" in (\'UB\',\'DRZ\') then \'UB\' else \'Others\' end as Area_classification from ( select Date , \"Cost/Egg_Out\" as Cost_Per_Egg, Area, Cost, Total_Eggs_Out , beat, \"GT/MT\" ,Unloading_Cost from maplemonk.transport_costs_last___mid_mile where beat not in (\'Mid Mile\' , \'Unbranded\' , \'D2C\' , \'Adhoc\' , \'Horeca\' , \'Darjan\' ,\'Material Transfer\', \'Sample\' , \'Unbranded \', \'Pending Deli\', \'Sampoorna\') and cost<> \'#N/A\' order by date )pp where to_date(pp.date, \'dd/mm/yyyy\') >= \'2023-01-01 \' ) yu where yu.area_classification <> \'NCR-ON-MT\' )pi on pa.date = pi.Date13 and pa.area = pi.area_classification and pa.beat_number_original = pi.beat left join ( select ui.*, bv.* from ( Select DISTINCT xx.GRN_DATE, xx.region, xx.Total_Procurred_Egg,xx.Amount, yy.Procurred_Egg_White, yy.Amount_White,zz.Procurred_Egg_Brown,zz.Amount_Brown, gg.Procurred_Egg_Nutra, gg.Amount_Nutra, case when xx.Total_Procurred_Egg =0 then 0 else xx.Amount/xx.Total_Procurred_Egg end as Proc_Price , case when yy.Procurred_Egg_White =0 then 0 else yy.Amount_White/yy.Procurred_Egg_White end as Procured_Price_White , case when zz.Procurred_Egg_Brown =0 then 0 else zz.Amount_Brown/zz.Procurred_Egg_Brown end as Procured_Price_Brown , case when gg.Procurred_Egg_Nutra =0 then 0 else gg.Amount_Nutra/gg.Procurred_Egg_Nutra end as Procured_Price_Nutra From ( SELECT GRN_DATE, region, sum(EGGS) as Total_Procurred_Egg, sum(amount) as Amount from eggozdb.maplemonk.region_wise_procurement_masterdata group by 1 ,2 ) xx left join (SELECT GRN_DATE, region, sum(EGGS) as Procurred_Egg_White , sum(amount) as Amount_White from eggozdb.maplemonk.region_wise_procurement_masterdata where type =\'White\' group by 1,2 )yy on xx.GRN_DATE= yy.GRN_DATE and xx.region = yy.region left join ( SELECT GRN_DATE, region, sum(EGGS) as Procurred_Egg_Brown , sum(amount) as Amount_Brown from eggozdb.maplemonk.region_wise_procurement_masterdata where type = \'Brown\' group by 1,2 ) zz on xx.GRN_DATE = zz.GRN_DATE and xx.region = zz.region left join ( SELECT GRN_DATE, region ,sum(EGGS) as Procurred_Egg_Nutra, sum(amount) as Amount_Nutra from eggozdb.maplemonk.region_wise_procurement_masterdata where type = \'Nutra+\' group by 1,2 ) gg on xx.GRN_DATE = gg.GRN_DATE and xx.region= gg.region )ui left join (SELECT DATE_TRUNC(\'DAY\', date) AS date, beat_number_original AS beat_number_original, area AS area, area_classification22 AS area_classification22, sum(packaged_eggs) AS Packaged_Eggs, sum(packaging_cost) AS Packaging_Cost FROM maplemonk.packaging_cost_final WHERE area_classification22 != \'N/A\' GROUP BY DATE_TRUNC(\'DAY\', date), beat_number_original, area, area_classification22 )bv on ui.GRN_DATE = bv.date and ui.region = bv.Area_classification22 )pj on pa.date = pj.date and pa.area = pj.area and pa.beat_number_original = pj.beat_number_original left join ( select cast(timestampadd(minute, 330, onboarding_date) as date) as Date, area_classification, beat_number, count(distinct code) New_retailers_onboarded from eggozdb.maplemonk.my_sql_retailer_retailer group by cast(timestampadd(minute, 330, onboarding_date) as date), area_classification, beat_number )kk on pa.date = kk.Date and pa.area = kk.area_classification and pa.beat_number_original = kk.beat_number left join ( select area_classification, beat_number, count(distinct code) as Total_Primary_Retailers_Beat from eggozdb.maplemonk.my_sql_retailer_retailer where distributor_id is null and category_id <> 3 and onboarding_status = \'Active\' group by area_classification, beat_number )jj on pa.area = jj.area_classification and pa.beat_number_original = jj.beat_number left join ( select distinct to_date(Date, \'dd/mm/yyyy\') as Date15, sum(\"Picked_up_Qty(In_Eggs)\") over (partition by date order by Date) as First_Mile_Eggs , sum(Total_Expense) over (partition by date order by Date) as First_Mile_Cost from maplemonk.transport_costs_FM_Vehicle_Details_after_1st_April order by to_date(Date, \'dd/mm/yyyy\') )pk on pa.date = pk.date15 left join ( select DISTINCT date, datediff(\'day\',date_trunc(\'month\',date),date)+1 no_of_days , datediff(\'day\',date_trunc(\'month\',date), last_day(date,\'month\'))+1 days_in_month from eggozdb.maplemonk.summary_reporting_table_beat_retailer group by date, area, parent_retailer_name )pl on pa.date = pl.date left join ( SELECT DISTINCT to_date(date, \'dd/mm/yyyy\') AS date, sum(cost) AS Mid_Mile_Cost, sum(total_eggs_out) AS Mid_Mile_Eggs_Out, AVG(\"Cost/Egg_Out\") AS Cost_Per_Egg_out_MM FROM maplemonk.transport_costs_last___mid_mile WHERE beat = \'Mid Mile\' and cost<> \'#N/A\' GROUP BY date )ik on pa.date = ik.date ;",
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
                        