{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table test.tcns.hourly_inventory_level as select PARENTCATEGORY, SUPERCATEGORY, SUBCATEGORY, sum(\"QTY SOLD\") Total_Qty_Sold from test.tcns.test_set group by PARENTCATEGORY, SUPERCATEGORY, SUBCATEGORY;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from TEST.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        