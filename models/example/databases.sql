{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Everpret_Overall_Target_vs_Achieved AS WITH Target AS ( SELECT SAFE.PARSE_DATE(\'%m-%d-%Y\', Date) AS Date, Type, CASE WHEN LOWER(Type) LIKE \'net\' THEN SAFE_CAST(REPLACE(Target,\",\",\"\") AS FLOAT64) ELSE 0 END AS Target, CASE WHEN LOWER(Type) LIKE \'cm\' THEN SAFE_CAST(REPLACE(Target,\",\",\"\") AS FLOAT64) ELSE 0 END AS CM_Target, CASE WHEN CURRENT_DATE(\'Asia/Kolkata\') > SAFE.PARSE_DATE(\'%Y-%m-%d\', Date) THEN 0 ELSE DATE_DIFF(LAST_DAY(SAFE.PARSE_DATE(\'%m-%d-%Y\', Date)), CURRENT_DATE(\'Asia/Kolkata\'), DAY) + 1 END AS days_Remaining, EXTRACT(DAY FROM LAST_DAY(SAFE.PARSE_DATE(\'%m-%d-%Y\', Date))) AS no_of_days FROM `MapleMonk.Everpret_Overall_Targets` t ), Sales AS ( SELECT Date, sum(ifnull(BAU_MRP_SALES,0)) - sum(ifnull(BAU_DISCOUNT,0)) - sum(ifnull(TRADE_MARGIN,0)) - sum(ifnull(Returns,0)) - sum(ifnull(gst,0)) AS Net_Revenue, sum(ifnull(BAU_MRP_SALES,0)) - sum(ifnull(BAU_DISCOUNT,0)) - sum(ifnull(TRADE_MARGIN,0)) - sum(ifnull(Returns,0)) - sum(ifnull(gst,0)) - sum(ifnull(cogs,0)) AS GM, sum(ifnull(BAU_MRP_SALES, 0)) - sum(ifnull(BAU_DISCOUNT, 0)) - sum(ifnull(TRADE_MARGIN, 0)) - sum(ifnull(Returns, 0)) - sum(ifnull(gst, 0)) - sum(ifnull(CHANNEL_MARGIN, 0)) - sum(ifnull(cogs, 0)) -(sum(ifnull(exhibition_cost,0)) + sum(ifnull(offline_store_cost,0)) + sum(((ifnull(BAU_MRP_SALES,0)) - (ifnull(BAU_DISCOUNT,0)) - (ifnull(Returns,0)) - (ifnull(gst,0)) - (ifnull(TRADE_MARGIN,0))) * ifnull(store_revenue_share,0))) - sum(ifnull(LOGISTICS_COST, 0)) - sum(ifnull(spend, 0)) + sum(ifnull(brand_spend,0)) AS CM_2, sum(ifnull(brand_spend,0)) - sum(ifnull(spend,0)) AS PM_Spends FROM `MapleMonk.STPL_pandl_intermediate` WHERE Date < Current_Date(\'Asia/Kolkata\') GROUP BY 1 ) SELECT COALESCE(s.Date,t.Date) AS Date, t.days_remaining, t.no_of_days, SUM(IFNULL(Net_Revenue,0)) AS Achievement, SUM(IFNULL(CM_2,0)) AS CM_2, SUM(IFNULL(GM,0)) AS GM, SUM(IFNULL(PM_Spends,0)) AS PM_Spends, SUM(IFNULL(t.Target,0)) AS Target, SUM(IFNULL(t.CM_Target,0)) AS Target_CM FROM Target t FULL OUTER JOIN Sales s ON t.Date = s.Date AND lower(t.Type) = \'%revenue%\' GROUP BY 1,2,3 Order by Date DESC ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            