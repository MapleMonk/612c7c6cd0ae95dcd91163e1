{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.KA_Tally_B2B_SALES_FACT_ITEMS PARTITION BY Order_Date CLUSTER BY MARKTETPLACE, SKUCODE AS SELECT TD.Date AS Order_Date, TD.Voucher_Number AS REFERENCE_CODE, TD.Voucher_Number AS Order_ID, UPPER(TD.Party_Name) AS Name, UPPER(TD.Mapped_Voucher_Type) AS MARKETPLACE, TD.Item_Part_No AS SKUCODE, p.skucode AS commonsku, UPPER(COALESCE(p.name, TD.ITEM_NAME)) AS Product_Name_Final, UPPER(COALESCE(p.category, TD.Item_Group)) AS Category, UPPER(p.sub_category) AS Sub_Category, CASE WHEN TD.Date < \'2025-04-01\' THEN COALESCE( SAFE_DIVIDE(TD.MRP * (1 - TD.Discount / 100), TD.Rate) - 1, p.Tax_Rate ) ELSE COALESCE(TD.GST, 5) / 100 END AS Tax_Rate, CAST(NULL AS STRING) AS Pincode, CAST(NULL AS STRING) AS Tracking_Number, CAST(NULL AS STRING) AS Phone, CAST(NULL AS STRING) AS Email, UPPER(TD.Company_Name) AS warehouse, CAST(NULL AS STRING) AS state, SUM(TD.Qty) AS Quantity, SUM(TD.MRP * (TD.Discount / 100) * TD.Qty) AS Discount, SUM( TD.Amount * ( 1 + CASE WHEN TD.Date < \'2025-04-01\' THEN COALESCE( SAFE_DIVIDE(TD.MRP * (1 - TD.Discount / 100), TD.Rate) - 1, p.Tax_Rate ) ELSE COALESCE(TD.GST, 5) / 100 END ) ) AS Selling_Price FROM `kerala-ayurveda-wh.temp_analytics.ka_s3_tally_b2b_invoice` TD LEFT JOIN ( SELECT COMMONSKU AS skucode, Name, Category, Category_Code AS sub_category, MRP, TAX_RATE FROM `kerala-ayurveda-wh.MapleMonk.FINAL_SKU_MASTER` WHERE LOWER(MARKETPLACE) LIKE \'%unicommerce%\' QUALIFY ROW_NUMBER() OVER ( PARTITION BY LOWER(IFNULL(TRIM(COMMONSKU), \'\')) ORDER BY LOWER(IFNULL(TRIM(COMMONSKU), \'\')) DESC ) = 1 ) p ON LOWER(REPLACE(TD.Item_Part_No, \' \', \'\')) = LOWER(REPLACE(p.skucode, \' \', \'\')) GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17",
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
            