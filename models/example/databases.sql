{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; create or replace table snitch_db.maplemonk.ItemBarcode_Daily_Report AS WITH DateCheck AS ( SELECT 1 AS Exist FROM snitch_db.maplemonk.ItemBarcode_Daily_Report WHERE DATE = CURRENT_DATE() LIMIT 1 ) SELECT CONVERT_TIMEZONE(\'UTC\', \'Asia/Kolkata\', _airbyte_normalized_at::DATETIME)::date AS date, \"Item Status\", \"Inventory type\", \"Facility Id\", SUM(\"Unit price without tax\") AS VALUE, COUNT(DISTINCT \"Item Code\") AS Units FROM snitch_db.maplemonk.unicommerce_itembarcode_report WHEre not EXISTS (SELECT * FROM DateCheck) GROUP BY 1, 2, 3, 4 union all select * FROM snitch_db.maplemonk.ItemBarcode_Daily_Report;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from snitch_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        