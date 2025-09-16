{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE Maplemonk.Blinkit_HM_sales_Fact_Items AS select S_No_ AS S_No, Item_ID AS Item_ID, CESS____ AS CESS, CGST____ AS CGST, HSN_Code AS HSN_Code, IGST____ AS IGST, MRP__Rs_ AS MRP_Rs, Order_ID AS Order_ID, Quantity AS Quantity, SGST____ AS SGST, Total_Tax AS Total_Tax, CESS_Value AS CESS_Value, CGST_Value AS CGST_Value, IGST_Value AS IGST_Value, Invoice_ID AS Invoice_ID, FORMAT_DATE(\'%Y-%m-%d\', PARSE_DATE(\'%d %b %Y\', Order_Date)) AS Order_Date, SGST_Value AS SGST_Value, L0_Category AS L0_Category, L1_Category AS L1_Category, L2_Category AS L2_Category, Order_Status AS Order_Status, Product_Name AS Product_Name, Supply_City AS Supply_City, Customer_City AS Customer_City, Selling_Price__Rs_ AS Selling_Price_Rs, Variant_Description AS Variant_Description, Total_Gross_Bill_Amount AS Total_Gross_Bill_Amount from `maplemonk.Blinkit_HM_sales`;",
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
            