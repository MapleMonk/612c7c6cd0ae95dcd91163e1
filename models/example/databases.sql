{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Meta_And_Google_Detailed AS WITH Marketing_Granular AS ( SELECT DATE, SUM(CASE WHEN UPPER(CHANNEL) = \'FACEBOOK\' THEN IFNULL(SPEND, 0) ELSE 0 END) AS Meta_Spend, SUM(CASE WHEN UPPER(CHANNEL) = \'GOOGLE\' THEN IFNULL(SPEND, 0) ELSE 0 END) AS Google_Spend, SUM(link_clicks) AS Link_Clicks, SUM(Landing_Page_Views) AS Landing_Page_Views, SUM(Add_to_carts) AS Add_To_Carts, SUM(Initiate_Checkouts) AS Initiate_Checkouts, SUM(CASE WHEN LOWER(REPLACE(IFNULL(CAMPAIGN_NAME,\'\'),\'_\',\'$\')) LIKE \'%social$yt%\' THEN IFNULL(SPEND, 0) ELSE 0 END) AS G_SPEND, SUM(CASE WHEN LOWER(REPLACE(IFNULL(CAMPAIGN_NAME,\'\'),\'_\',\'$\')) LIKE ANY(\'%$top$%\',\'%instagram%\',\'%madison$zouk%\') OR LOWER(CAMPAIGN_NAME) LIKE \'demand gen- myntra\' THEN IFNULL(SPEND, 0) ELSE 0 END) AS M_SPEND FROM `MapleMonk.zouk_MARKETING_CONSOLIDATED` GROUP BY 1 ), Sales_Performance AS ( SELECT Date, SUM(Sales) AS Total_Sales, SUM(Orders) AS Total_Orders, SUM(IFNULL(Web_Sessions, 0) + IFNULL(App_Sessions, 0)) AS Total_Sessions FROM `MapleMonk.Zouk_D2C_Sessions_Report` GROUP BY 1 ) SELECT m.Date, (m.Meta_Spend - m.M_Spend)Meta_Spend, (m.Google_Spend - m.G_Spend)Google_Spend, (m.Meta_Spend - m.M_Spend + m.Google_Spend - m.G_Spend) AS Total_Spend, IFNULL(m.Link_Clicks, 0) AS Link_Clicks, IFNULL(m.Landing_Page_Views, 0) AS Landing_Page_Views, IFNULL(m.Add_To_Carts, 0) AS Add_To_Carts, IFNULL(m.Initiate_Checkouts, 0) AS Initiate_Checkouts, IFNULL(s.Total_Sales, 0) AS Sales, IFNULL(s.Total_Orders, 0) AS Orders, IFNULL(s.Total_Sessions, 0) AS Sessions FROM Marketing_Granular m LEFT JOIN Sales_Performance s ON m.Date = s.Date WHERE m.Meta_Spend > 0 OR m.Google_Spend > 0 OR m.M_Spend > 0 OR m.G_Spend > 0 ORDER BY m.Date DESC;",
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
            