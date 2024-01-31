{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table dunatura_db.maplemonk.b2b_sales as select concat(monthname(month::date),\' \',year(month::Date)) month_year, month::date month, sales::float sales, tagespack::float tagespack from dunatura_db.maplemonk.b2b_data ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from dunatura_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        