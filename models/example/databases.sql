{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table prd_db.beardo.dwh_ecommerce_summary as select to_date(date, \'mm/dd/yyyy\') ordeR_date, channel_group, Channel_name sku_code, quantity::float quantity, total_mrp::float total_mrp, \"BD Business\" product_category, \"BD Category\" Product_sub_category, product_name, \"NR (Single)\" NR_per_unit, \" NR (Total) \" NR from datalake_db.beardo.trn_ecomm_offtake",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from PRD_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        