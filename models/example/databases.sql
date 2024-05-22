{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.gatepass_tracking AS ( SELECT gs.\"Order Placed Date\", gs.\"Gate-Pass Number\", gs.facility, TO_TIMESTAMP(gp.\"Gatepass Updated At\") AS \"Updated Timestamp\", gs.\"Quantity Ordered\", SUM(gp.QUANTITY) AS QtyScanned, COALESCE(gs.\"Warehouse Status\", \'Not Started\') AS \"Warehouse Status\", gp.\"To Party\" FROM snitch_db.maplemonk.google_sheet_store_goods_tracker_summary gs LEFT OUTER JOIN snitch_db.maplemonk.unicommerce_gatepass gp ON gs.\"Gate-Pass Number\" = gp.\"Gatepass Code\" GROUP BY gs.\"Order Placed Date\", gs.\"Gate-Pass Number\", gs.facility, \"Updated Timestamp\", gs.\"Quantity Ordered\", gp.\"To Party\", COALESCE(gs.\"Warehouse Status\", \'Not Started\') )",
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
                        