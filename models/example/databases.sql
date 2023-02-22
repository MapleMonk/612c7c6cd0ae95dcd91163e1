{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.Overall_KPI as Select xx.*, yy.\"Demand\", yy.\"Fresh_In\" , yy.\"Supply\", yy.\"Old_In\" from ( SELECT area AS \"area\", DATE_TRUNC(\'DAY\', date) AS \"date\", sum(eggs_sold_white) AS \"Eggs_Sold_White\", sum(eggs_sold_brown) AS \"Eggs_Sold_Brown\", sum(eggs_sold_nutra) AS \"Eggs_Sold_Nutra\", sum(eggs_sold) AS \"Eggs_Sold\", sum(eggs_replaced) AS \"Eggs_Replaced\", case when sum(eggs_sold)=0 then 0 else sum(eggs_replaced)/sum(eggs_sold) end AS \"Replacement_Per\", case when sum(eggs_sold_white)=0 then 0 else sum(eggs_replaced_white)/sum(eggs_sold_white) end AS \"Eggs_Replacement_Per_White\", case when sum(eggs_sold_nutra)=0 then 0 else sum(eggs_replaced_nutra)/sum(eggs_sold_nutra) end AS \"Eggs_Replacement_Per_Nutra\", case when sum(eggs_sold_brown)=0 then 0 else sum(eggs_replaced_brown)/sum(eggs_sold_brown) end AS \"Eggs_Replacement_Per_Brown\", sum(eggs_returned) AS \"Eggs_Returned\", case when sum(eggs_sold)=0 then 0 else sum(eggs_returned)/sum(eggs_sold) end AS \"Returned_Per\", sum(amount_return) AS \"Amount_Return\", sum(net_sales) AS \"Revenue\", sum(net_sales)-sum(amount_return) AS \"NET_SALES\", case when sum(eggs_sold)=0 then 0 ELSE sum(net_sales)/sum(eggs_sold) END AS \"Landing_Price\", sum(collections) AS \"Collections\", case when sum(net_sales)=0 then 0 else sum(collections)/(sum(net_sales)-sum(amount_return)) end AS \"Collection_Per\", sum(daily_retailers_onboarded) AS \"Retailers_Onboarded\", sum(eggs_promo) AS \"Eggs_Promo\", case when sum(eggs_sold)=0 then 0 else sum(eggs_promo)/sum(eggs_sold) end AS \"Promo_Per\" FROM maplemonk.summary_reporting_table GROUP BY area, DATE_TRUNC(\'DAY\', date) ORDER BY \"Eggs_Sold\" DESC )xx left join ( SELECT DATE_TRUNC(\'DAY\', date) AS \"date\", area AS \"area\", sum(out)-sum(sold)-sum(replacement)+sum(transfer)-sum(promo)-sum(fresh_in) AS \"BRANDED_SHORTFALL\", SUM(replacement)+SUM(return)-sum(damage)-SUM(old_in) AS \"NON_BRANDED_SHORTFALL\", case when sum(demand)=0 then 0 ELSE -1*(sum(demand) - sum(out))/sum(demand) END AS \"LESS_SUPPLIED\", case when sum(supply)=0 then 0 ELSE -1*(sum(supply) - sum(out))/sum(supply) END AS \"LESS_SUPPLIED_AFTER_COMMITMENT\", case when sum(out)=0 then 0 ELSE sum(sold)/sum(out) END AS \"SOLD_VS_SUPPLY\", case when sum(out)=0 then 0 ELSE (sum(sold)+sum(replacement))/sum(out) END AS \"SOLD_AND_REPLACEMENT_VS_SUPPLY\", case when sum(out)=0 then 0 ELSE sum(fresh_in)/sum(out) END AS \"FRESH_RETURNED_TO_WAREHOUSE\", sum(demand) AS \"Demand\", sum(supply) AS \"Supply\", sum(out) AS \"Out\", sum(sold) AS \"Sold\", sum(damage) AS \"Damage\", sum(return) AS \"Return\", sum(replacement) AS \"Replacement\", sum(transfer) AS \"Transfer\", sum(promo) AS \"Promo\", sum(fresh_in) AS \"Fresh_In\", sum(old_in) AS \"Old_In\" FROM maplemonk.beat_material_kpi GROUP BY DATE_TRUNC(\'DAY\', date), area ORDER BY \"Sold\" DESC )yy on xx.\"date\" = yy.\"date\" and xx.\"area\" = yy.\"area\" ;",
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
                        