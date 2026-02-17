{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.Saadaa_grn_serial_out_of_system_report AS SELECT CAST(PO AS INT64) AS PO, CAST(GRN AS INT64) AS GRN, CAST(MRP AS FLOAT64) AS MRP, SKU, Name, Size, IMEI1, IMEI2, IMEI3, IMEI4, IMEI5, IMEI6, Shelf, Colour, Status, CAST(Add_Time AS TIMESTAMP) AS Add_Time, Category, Vendor_Code, Company_Name, REPLACE(Serial_Number, \'`\', \'\') AS Serial_Number, CAST(Days_In_Warehouse AS INT64) AS Days_In_Warehouse FROM maplemonk.saadaa_serial_out_of_system_report;",
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
            