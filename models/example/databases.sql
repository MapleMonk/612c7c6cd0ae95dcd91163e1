{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.gatepass_tracking AS ( SELECT gs.\"Order Placed Date\", gs.\"Gate-Pass Number\", gs.facility as warehouse_name, TO_TIMESTAMP(gp.\"Gatepass Updated At\") AS \"Updated Timestamp\", gs.\"Quantity Ordered\", SUM(gp.QUANTITY) AS QtyScanned, COALESCE(gs.\"Warehouse Status\", \'Not Started\') AS \"Warehouse Status\", gs.\"Store Destination\", gs.\"RTD Date\", FROM snitch_db.maplemonk.GS_STORE_GOODS_FINAL_SUMMARY gs LEFT OUTER JOIN snitch_db.maplemonk.unicommerce_gatepass gp ON gs.\"Gate-Pass Number\" = gp.\"Gatepass Code\" GROUP BY gs.\"Order Placed Date\", gs.\"Gate-Pass Number\", warehouse_name, \"Updated Timestamp\", gs.\"Quantity Ordered\", gs.\"Store Destination\", COALESCE(gs.\"Warehouse Status\", \'Not Started\'), gs.\"RTD Date\" )",
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
                        