{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE Maplemonk.4Thd_blinkit_Fact_Items AS SELECT CAST(S_No_ AS INT64) AS S_No, CAST(Item_ID AS STRING) AS Item_ID, CAST(HSN_Code AS STRING) AS HSN_Code, CAST(MRP__Rs_ AS FLOAT64) AS MRP, CAST(Order_Id AS STRING) AS Order_ID, CAST(Quantity AS INT64) AS Quantity, CAST(REPLACE(Total_Tax, \'-\', \'0\') AS FLOAT64) AS Total_Tax, cast(left(Order_Date,10) as date) AS Order_Date, CAST(Order_Status AS STRING) AS Order_Status, CAST(Product_Name AS STRING) AS Product_Name, CAST(Supply_City AS STRING) AS Supply_City, CAST(Customer_City AS STRING) AS Customer_City, CAST(Selling_Price__Rs_ AS FLOAT64) AS Selling_Price_Rs, CAST(Variant_Description AS STRING) AS Variant_Description, CAST(Total_Gross_Bill_Amount AS FLOAT64) AS Total_Gross_Bill_Amount, from maplemonk.4Thd_blinkit_sales s ;",
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
            