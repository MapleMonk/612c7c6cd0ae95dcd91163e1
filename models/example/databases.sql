{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.DTC_Target_vs_Achievement AS WITH Collection_Customer_Type_Target AS ( SELECT TRIM(UPPER(t.Customer_Type)) AS Customer_Type, TRIM(UPPER(t.Collection)) AS Collection, DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY) AS Target_Date, CAST(t.Net_Sales_Target AS FLOAT64) / (DATE_DIFF(DATE(t.End_Date), DATE(t.Start_Date), DAY) + 1) AS Net_Sales_Target, CASE WHEN CURRENT_DATE() > DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY) THEN 0 ELSE DATE_DIFF(LAST_DAY(DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY)), CURRENT_DATE(), DAY) + 1 END AS days_Remaining, EXTRACT(DAY FROM LAST_DAY(DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY))) AS no_of_days FROM MapleMonk.Zouk_DTC_Targets t CROSS JOIN UNNEST(GENERATE_ARRAY(0, DATE_DIFF(DATE(t.End_Date), DATE(t.Start_Date), DAY))) AS n ), Sales_Achievement AS ( SELECT DATE(ps.Date) AS Target_Date, TRIM(UPPER(ps.new_customer_flag)) AS Customer_Type, TRIM(UPPER(ps.COLLECTION)) AS Collection, SUM(BAU_MRP_SALES) AS BAU_MRP_SALES, SUM(BAU_DISCOUNT) AS BAU_DISCOUNT, SUM(SALES) AS SALES, SUM(Spend) AS Spend, SUM(Brand_Spend) AS Brand_Spend, SUM(return_mrp_sales) AS return_mrp_sales, SUM(return_trade_margin) AS return_trade_margin, SUM(Returns) AS Returns, SUM(gst) AS gst, SUM(Return_GST) AS Return_GST, SUM(Trade_Margin) AS Trade_Margin FROM MapleMonk.Zouk_Product_Sales_Cost_Source_DTC ps GROUP BY 1,2,3 ) SELECT COALESCE(ct.Target_Date,sa.Target_Date) As Date, COALESCE(ct.Customer_Type,sa.Customer_Type) As Customer_Type, COALESCE(ct.Collection,sa.Collection) As Collection, ct.Net_Sales_Target, ct.days_Remaining, ct.no_of_days, IFNULL(sa.BAU_MRP_SALES, 0) AS BAU_MRP_SALES, IFNULL(sa.BAU_DISCOUNT, 0) AS BAU_DISCOUNT, IFNULL(sa.SALES, 0) AS SALES, IFNULL(sa.Spend, 0) AS Spend, IFNULL(sa.Brand_Spend, 0) AS Brand_Spend, IFNULL(sa.return_mrp_sales, 0) AS return_mrp_sales, IFNULL(sa.return_trade_margin, 0) AS return_trade_margin, IFNULL(sa.Returns, 0) AS Returns, IFNULL(sa.gst, 0) AS gst, IFNULL(sa.Return_GST, 0) AS Return_GST, IFNULL(sa.Trade_Margin, 0) AS Trade_Margin FROM Collection_Customer_Type_Target ct FULL OUTER JOIN Sales_Achievement sa ON ct.Target_Date = sa.Target_Date AND ct.Collection = sa.Collection AND ct.Customer_Type = sa.Customer_Type;",
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
            