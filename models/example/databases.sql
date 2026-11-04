{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE RUBYSORGANICS_DB.MAPLEMONK.RUBYSORGANICS_blinkit_Fact_Items AS SELECT CAST(\"Item Id\" AS VARCHAR) AS Item_ID, CAST(\"HSN Code\" AS VARCHAR) AS HSN_Code, TRY_TO_DOUBLE(\"MRP (Rs)\") AS MRP, CAST(\"Order Id\" AS VARCHAR) AS Order_ID, TRY_TO_NUMBER(Quantity) AS Quantity, TRY_TO_DOUBLE(REPLACE(\"Total Tax\", \'-\', \'0\')) AS Total_Tax, TRY_TO_DATE(LEFT(\"Order Date\", 10)) AS Order_Date, CAST(\"Order Status\" AS VARCHAR) AS Order_Status, CAST(\"Product Name\" AS VARCHAR) AS Product_Name, CAST(\"Supply City\" AS VARCHAR) AS Supply_City, CAST(\"Customer City\" AS VARCHAR) AS Customer_City, TRY_TO_DOUBLE(\"Selling Price (Rs)\") AS Selling_Price_Rs, CAST(\"Variant Description\" AS VARCHAR) AS Variant_Description, TRY_TO_DOUBLE(\"Total Gross Bill Amount\") AS Total_Gross_Bill_Amount, \"Item Id\" AS PRODUCT_ID, \"Customer City\" AS CITY, NULL AS STATE FROM RUBYSORGANICS_DB.MAPLEMONK.Rubys_Organics_blinkit_sales s ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from RUBYSORGANICS_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            