{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create table snitch_db.maplemonk.CategoryWiseProducts AS SELECT category AS \"category\", product_name, sum(quantity) AS \"QTY\" FROM snitch_db.maplemonk.fact_items_snitch WHERE order_timestamp >= \'2022-03-01 00:00:00.000000\' AND order_timestamp < \'2022-03-31 00:00:00.000000\' AND is_refund IN (0) GROUP BY category,product_name ORDER BY \"QTY\" DESC",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from SNITCH_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        