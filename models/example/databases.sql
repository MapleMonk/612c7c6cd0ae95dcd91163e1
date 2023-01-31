{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE eggozdb.maplemonk.PO_Expected_Target_final as Select xx.Date2 as PO_DATE, dayname(xx.Date2), yy.PO_AMOUNT, yy.Total_POs, yy.SUPPLY_AMOUNT, yy.Amount_Fillrate, yy.PO_Egg_Count,yy.Supply_Egg_Count, yy.Egg_Fillrate,yy.TAT, xx.parent_name,xx.PO_Expected from (select fd.Parent_Name, fd.Date2, fd.day2, coalesce(gd.PO_expected,fd.PO_expected) as PO_Expected from (Select bb.Date2 , bb.Day2 , bb.Parent_Name, aa.PO_Expected from (Select Day, Parent_name, area_classification, PO_Expected from maplemonk.target_PO_Expected_Maplemonk ) aa inner join ( Select DISTINCT qq.DATE as Date2 , pp.Parent_name as Parent_Name, qq.DayName as Day2 from (Select DISTINCT Parent_name , PO_Expected from maplemonk.target_PO_Expected_Maplemonk ) pp cross JOIN (SELECT distinct DECODE(EXTRACT (\'dayofweek_iso\',DATE), 1, \'Monday\', 2, \'Tuesday\', 3, \'Wednesday\', 4, \'Thursday\', 5, \'Friday\', 6, \'Saturday\', 7, \'Sunday\') AS DayName , DATE, area_classification from eggozdb.maplemonk.Date_area_dim WHERE year(date)>=2021 and date <=getdate() )qq )bb on aa.Day=bb.Day2 and aa.Parent_name= bb.Parent_Name where aa.area_classification = \'NCR-ON-MT\')gd right join (Select DISTINCT qq.DATE as Date2 , pp.Parent_name as Parent_Name, qq.DayName as Day2, 0 as PO_Expected from (Select DISTINCT Parent_name , PO_Expected from maplemonk.target_PO_Expected_Maplemonk ) pp cross JOIN (SELECT distinct DECODE(EXTRACT (\'dayofweek_iso\',DATE), 1, \'Monday\', 2, \'Tuesday\', 3, \'Wednesday\', 4, \'Thursday\', 5, \'Friday\', 6, \'Saturday\', 7, \'Sunday\') AS DayName , DATE, area_classification from eggozdb.maplemonk.Date_area_dim WHERE year(date)>=2021 and date <=getdate() )qq )fd on gd.Date2=fd.Date2 and gd.Parent_name=fd.Parent_name ) xx left join ( SELECT parent_name AS parent_name, DATE_TRUNC(\'DAY\', po_date) AS po_date, COUNT (DISTINCT po_no) AS Total_POs, sum(po_amount) AS PO_AMOUNT, sum(supply_amount) AS SUPPLY_AMOUNT, sum(supply_amount)/sum(po_amount) AS Amount_Fillrate, sum(po_egg_count) AS PO_Egg_Count, sum(supply_egg_count) AS Supply_Egg_Count, sum(supply_egg_count)/sum(po_egg_count) AS Egg_Fillrate, AVG(datediff(\'day\', po_date, delivery_date)) AS TAT FROM maplemonk.demand_supply_po_list WHERE lower(area_classification) LIKE lower(\'%ncr-on-mt%\') GROUP BY parent_name, DATE_TRUNC(\'DAY\', po_date) ) yy on xx.Date2 = yy.po_date and xx.Parent_Name = yy.parent_name order by xx.Date2 DESC ;",
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
                        