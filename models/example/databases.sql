{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.datewise_high_discount AS ( WITH high_discount AS ( SELECT order_name, order_timestamp::date AS date, div0(SUM(discount), SUM(discount + gross_sales)) * 100 AS discount_percentage FROM snitch_db.maplemonk.fact_items_snitch GROUP BY order_name, date ) SELECT date,discount_percentage, COUNT(order_name) AS order_count FROM high_discount GROUP BY date,discount_percentage ); CREATE OR REPLACE TABLE snitch_db.maplemonk.datewise_superlow_value AS ( WITH low_value AS ( SELECT order_name, order_timestamp::date AS date, SUM(gross_sales) AS gross_sales FROM snitch_db.maplemonk.fact_items_snitch GROUP BY order_name, date ) SELECT date,gross_sales, COUNT(order_name) AS order_count FROM low_value GROUP BY date,gross_sales );",
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
                        