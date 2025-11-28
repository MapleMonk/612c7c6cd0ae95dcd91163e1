{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE Maplemonk.Blinkit_HM_sales_Fact_Items AS SELECT CAST(S_No_ AS INT64) AS S_No, CAST(Item_ID AS STRING) AS Item_ID, CAST(HSN_Code AS STRING) AS HSN_Code, CAST(MRP__Rs_ AS FLOAT64) AS MRP, CAST(Order_ID AS STRING) AS Order_ID, CAST(Quantity AS INT64) AS Quantity, CAST(REPLACE(Total_Tax, \'-\', \'0\') AS FLOAT64) AS Total_Tax, CAST(Invoice_ID AS STRING) AS Invoice_ID, FORMAT_DATE(\'%Y-%m-%d\', PARSE_DATE(\'%d %b %Y\', Order_Date)) AS Order_Date, CAST(L0_Category AS STRING) AS L0_Category, CAST(L1_Category AS STRING) AS L1_Category, CAST(L2_Category AS STRING) AS L2_Category, CAST(Order_Status AS STRING) AS Order_Status, CAST(Product_Name AS STRING) AS Product_Name, CAST(Supply_City AS STRING) AS Supply_City, CAST(Customer_City AS STRING) AS Customer_City, CAST(Selling_Price__Rs_ AS FLOAT64) AS Selling_Price_Rs, CAST(Variant_Description AS STRING) AS Variant_Description, CAST(Total_Gross_Bill_Amount AS FLOAT64) AS Total_Gross_Bill_Amount, sm.Product_title as product_name_final, sm.category as Product_category, sm.Product_Type from `maplemonk.Blinkit_HM_sales` s left join (select Product_title, Product_Type, Category, Market_place_product_ID sku_code, Market_place_products_name from maplemonk.google_sheets_sku_master where lower(platform) like \'%blink%\' qualify row_number() over (partition by lower(replace(Market_place_product_ID,\' \',\'\')) order by 1)=1 ) sm on lower(replace(s.Item_ID,\' \',\'\')) = lower(replace(sm.sku_code,\' \',\'\')) ; Create Or Replace Table maplemonk.HM_DB_Blinkit_Ads_Fact_items as SELECT PARSE_DATE(\'%d-%m-%Y\', Date) AS Date, CAST(REPLACE(Estimated_Budget_Consumed, \',\', \'\') AS FLOAT64) AS Estimated_Budget_Consumed, SAFE_CAST(REPLACE(Total_RoAS, \',\', \'\') AS FLOAT64) AS Total_RoAS, SAFE_CAST(REPLACE(CPM, \',\', \'\') AS FLOAT64) AS CPM, SAFE_CAST(REPLACE(Direct_ATC, \',\', \'\') AS FLOAT64) AS Direct_ATC, SAFE_CAST(REPLACE(Direct_Quantities_Sold, \',\', \'\') AS FLOAT64) AS Direct_Quantities_Sold, SAFE_CAST(REPLACE(Direct_Sales, \',\', \'\') AS FLOAT64) AS Direct_Sales, SAFE_CAST(REPLACE(Impressions, \',\', \'\') AS FLOAT64) AS Impressions, ba.Campaign_Name, NULL AS CTR, NULL AS Reach, CAST(NULL AS STRING) AS Match_Type, CAST(NULL AS INT64) AS Unique_Clicks, CAST(NULL AS STRING) AS Targeting_Type, CAST(NULL AS STRING) AS Targeting_Value, \'regular\' AS Type FROM `MAPLEMONK.HM_DB_ads` ba;",
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
            