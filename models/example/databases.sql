{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table maplemonk.ka_s3_distribution_sales_data as SELECT REGEXP_EXTRACT(_airbyte_Data, r\'\"Actual Qty\"\s*:\s*\"([^\"]*)\"\') AS Actual_Qty, REGEXP_EXTRACT(_airbyte_Data, r\'\"Qty\"\s*:\s*\"([^\"]*)\"\') AS Qty, REGEXP_EXTRACT(_airbyte_Data, r\'\"Category\"\s*:\s*\"([^\"]*)\"\') AS Category, REGEXP_EXTRACT(_airbyte_Data, r\'\"Discount\"\s*:\s*\"([^\"]*)\"\') AS Discount, REGEXP_EXTRACT(_airbyte_Data, r\'\"Item HSN\"\s*:\s*\"([^\"]*)\"\') AS Item_HSN, REGEXP_EXTRACT(_airbyte_Data, r\'\"MRP\"\s*:\s*\"([^\"]*)\"\') AS MRP, REGEXP_EXTRACT(_airbyte_Data, r\'\"Item GST\"\s*:\s*\"([^\"]*)\"\') AS Item_GST, REGEXP_EXTRACT(_airbyte_Data, r\'\"Voucher Type\"\s*:\s*\"([^\"]*)\"\') AS Voucher_Type, REGEXP_EXTRACT(_airbyte_Data, r\'\"RSM Name\"\s*:\s*\"([^\"]*)\"\') AS RSM_Name, REGEXP_EXTRACT(_airbyte_Data, r\'\"Sales Value\(With Tax\)\"\s*:\s*\"([^\"]*)\"\') AS Sales_Value_With_Tax, REGEXP_EXTRACT(_airbyte_Data, r\'\"ASM Name\"\s*:\s*\"([^\"]*)\"\') AS ASM_Name, REGEXP_EXTRACT(_airbyte_Data, r\'\"HQ/Location\"\s*:\s*\"([^\"]*)\"\') AS HQ_Location, REGEXP_EXTRACT(_airbyte_Data, r\'\"Party Name\"\s*:\s*\"([^\"]*)\"\') AS Party_Name, REGEXP_EXTRACT(_airbyte_Data, r\'\"Item Part No\"\s*:\s*\"([^\"]*)\"\') AS Item_Part_No, REGEXP_EXTRACT(_airbyte_Data, r\'\"Amount\"\s*:\s*\"([^\"]*)\"\') AS Amount, REGEXP_EXTRACT(_airbyte_Data, r\'\"GST Amount\"\s*:\s*\"([^\"]*)\"\') AS GST_Amount, REGEXP_EXTRACT(_airbyte_Data, r\'\"Date\"\s*:\s*\"([^\"]*)\"\') AS Date, REGEXP_EXTRACT(_airbyte_Data, r\'\"Voucher Number\"\s*:\s*\"([^\"]*)\"\') AS Voucher_Number, REGEXP_EXTRACT(_airbyte_Data, r\'\"Company Name\"\s*:\s*\"([^\"]*)\"\') AS Company_Name, REGEXP_EXTRACT(_airbyte_Data, r\'\"Item Name\"\s*:\s*\"([^\"]*)\"\') AS Item_Name, REGEXP_EXTRACT(_airbyte_Data, r\'\"BDE Name\"\s*:\s*\"([^\"]*)\"\') AS BDE_Name, REGEXP_EXTRACT(_airbyte_Data, r\'\"Item_Batch\"\s*:\s*\"([^\"]*)\"\') AS Item_Batch, REGEXP_EXTRACT(_airbyte_Data, r\'\"Item Group\"\s*:\s*\"([^\"]*)\"\') AS Item_Group, REGEXP_EXTRACT(_airbyte_Data, r\'\"Rate\(NRV\)\"\s*:\s*\"([^\"]*)\"\') AS Rate_NRV FROM `maplemonk._airbyte_raw_KA_S3_Distribution_Sales` ;",
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
            