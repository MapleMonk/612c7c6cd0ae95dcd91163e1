{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Zouk_DTC_Collection_Performance AS WITH Collection_Wise_Session AS ( SELECT SAFE.PARSE_DATE(\'%Y-%m-%d\',Date) AS Date, TRIM(LOWER(Collection)) AS Collection, CAST(REPLACE(ATC, \',\', \'\') AS FLOAT64) AS ATC, CAST(REPLACE(Session, \',\', \'\') AS FLOAT64) AS Session FROM MapleMonk.Zouk_Collection_Wise_Session ) , Targets AS ( SELECT DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY) AS Date, TRIM(LOWER(t.Collection)) AS Collection, CAST(REPLACE(t.Target, \',\', \'\') AS FLOAT64) / (DATE_DIFF(DATE(t.End_Date), DATE(t.Start_Date), DAY) + 1) AS Target_Revenue FROM MapleMonk.Zouk_Collection_Wise_Target t, UNNEST(GENERATE_ARRAY(0, DATE_DIFF(DATE(t.End_Date), DATE(t.Start_Date), DAY))) AS n ) , DTC_PandL AS ( SELECT Date, TRIM(LOWER(COLLECTION)) AS Collection, COUNT(DISTINCT REFERENCE_CODE) AS Orders, SUM(IFNULL(BAU_MRP_SALES, 0)) - SUM(IFNULL(BAU_DISCOUNT, 0)) - SUM(IFNULL(return_mrp_sales, 0)) AS MRP_Sales, SUM(IFNULL(BAU_MRP_SALES, 0)) - SUM(IFNULL(return_mrp_sales, 0)) - SUM(IFNULL(BAU_DISCOUNT, 0)) - SUM(IFNULL(TRADE_MARGIN, 0)) + SUM(IFNULL(return_trade_margin, 0)) - SUM(IFNULL(Returns, 0)) - SUM(IFNULL(gst, 0)) + SUM(IFNULL(Return_GST, 0)) AS Net_Revenue FROM MapleMonk.zouk_pandl_DTC GROUP BY 1,2 ) SELECT p.Date, p.Collection, t.Target_Revenue, s.ATC, s.Session, p.Orders, p.MRP_Sales, p.Net_Revenue FROM DTC_PandL p join Targets t ON p.Date = t.Date AND trim(lower(p.Collection)) = trim(lower(t.Collection)) join Collection_Wise_Session s ON t.Date = s.Date AND trim(lower(t.Collection)) = trim(lower(s.Collection)) ;",
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
            