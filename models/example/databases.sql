{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.transport_costs_last_mid_mile ADD (ddate Date); UPDATE eggozdb.maplemonk.transport_costs_last_mid_mile SET ddate = TRY_TO_DATE(Date,\'DD/MM/YYYY\'); ALTER TABLE eggozdb.maplemonk.transport_costs_fm_vehicle_details_after_1st_april ADD (ddate Date); UPDATE eggozdb.maplemonk.transport_costs_fm_vehicle_details_after_1st_april SET ddate = TRY_TO_DATE(Date,\'DD/MM/YYYY\'); create or replace table eggozdb.maplemonk.Logistic_Graphs as SELECT pp.* , qq.LM_Cost, qq.LM_Eggs_Out, qq.Vehicle_Capacity_LM, qq.LM_Cost_Per_Egg_Out, rr.MM_Cost, rr.MM_Eggs_Out, rr.MM_Cost_Per_Egg_Out , rr.Vehicle_Capacity_MM from ( SELECT DATE_TRUNC(\'DAY\', ddate) AS ddate, sum(total_expense) AS Total_Cost_FM, AVG(\"Cost/egg\") AS Cost_per_Egg, sum(total_eggs_filled)/sum(vehicle_capacity) AS Fill_Rate_Per_FM, sum(vehicle_capacity) AS Vehicle_capacity_FM, SUM(total_eggs_filled) AS Total_Eggs_Filled_FM, SUM(\"Picked_up_Qty(In_Eggs)\") AS Eggs_Out_FM FROM maplemonk.transport_costs_fm_vehicle_details_after_1st_april where vehicle_capacity not in (\'#N/A\') and \"Cost/egg\" not in (\'#DIV/0!\') and vehicle_capacity<>0 GROUP BY DATE_TRUNC(\'DAY\', ddate) )pp left join ( SELECT DATE_TRUNC(\'DAY\', ddate) AS ddate, AVG(cost) AS LM_Cost, sum(total_eggs_out) AS LM_Eggs_Out, AVG(\"Cost/Egg_Out\") AS LM_Cost_Per_Egg_Out, sum(\"vehicle capacity\") AS Vehicle_Capacity_LM FROM maplemonk.transport_costs_last_mid_mile WHERE beat NOT IN (\'Mid Mile\') and cost not in (\'#REF!\' , \'#N/A\', \'#VALUE!\', \'#VALUE! \') and DATE_TRUNC(\'DAY\', ddate) > \'2023-01-01\' and cost not like (\'%#%\') and \"Cost/Egg_Out\" not like (\'%#%\') and \"vehicle capacity\" not in (\'#VALUE!\') and total_eggs_out not like (\'%#%\') GROUP BY DATE_TRUNC(\'DAY\', ddate) )qq on pp.ddate = qq.ddate left join ( SELECT DATE_TRUNC(\'DAY\', ddate) AS ddate, AVG(cost) AS MM_Cost, sum(total_eggs_out) AS MM_Eggs_Out, AVG(\"Cost/Egg_Out\") AS MM_Cost_Per_Egg_Out, sum(\"vehicle capacity\") AS Vehicle_Capacity_MM FROM maplemonk.transport_costs_last_mid_mile WHERE beat IN (\'Mid Mile\') GROUP BY DATE_TRUNC(\'DAY\', ddate) )rr on pp.ddate = rr.ddate ;",
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
                        