{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.zouk_Customer_Calling_Report AS WITH Calling AS ( SELECT Date_Created, CAST(Date_Created AS DATE) AS Call_Date, LTRIM(Customer_Number, \'0\') AS Customer_Number, Agent_Name, Leg2_Status, 100.0 / COUNT(*) OVER (PARTITION BY CAST(Date_Created AS DATE), Agent_Name) AS Split_Target FROM `MapleMonk.Zouk_Exotel_Calling_Report` WHERE lower(Agent_Name) LIKE ANY (\'%madhuri%\', \'%shruthi%\') ), Orders AS ( SELECT CAST(order_timestamp AS DATE) AS Order_Date, order_name, Marketplace, REPLACE(phone, \'+91\', \'\') AS Shopify_Phone, SUM(Total_Sales) AS Total_Sales FROM `MapleMonk.zouk_shopify_fact_items` WHERE lower(order_status) NOT LIKE \'%cancel%\' GROUP BY 1, 2, 3, 4 ), First_Purchase_Logic AS ( SELECT c.Date_Created AS Call_Timestamp_ID, c.Customer_Number, o.Order_Date, o.Order_name, o.Marketplace, o.Total_Sales, DATE_DIFF(o.Order_Date, c.Call_Date, DAY) as Days_to_Convert FROM Calling c INNER JOIN Orders o ON c.Customer_Number = o.Shopify_Phone AND o.Order_Date >= c.Call_Date WHERE lower(c.Leg2_Status) like \'completed\' QUALIFY ROW_NUMBER() OVER( PARTITION BY c.Customer_Number, c.Date_Created ORDER BY o.Order_Date ASC ) = 1 ) SELECT c.Call_Date, fp.Order_Date, c.Agent_Name, c.Customer_Number, c.Leg2_Status AS Call_Status, fp.Order_name, fp.Marketplace, fp.Days_to_Convert, c.Split_Target, CASE WHEN fp.Order_name IS NOT NULL THEN 1 ELSE 0 END AS Order_Count, COALESCE(fp.Total_Sales, 0) AS Total_Sales FROM Calling c LEFT JOIN First_Purchase_Logic fp ON c.Customer_Number = fp.Customer_Number AND c.Date_Created = fp.Call_Timestamp_ID",
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
            