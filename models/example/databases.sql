{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table redtape_db.maplemonk.new_products_inventory as select sku, location ,product_name, first_date from (select sku, location, \"Product Name\" product_name,\"Report Generated Date\"::date first_Date, row_number() over (partition by sku, location order by \"Report Generated Date\"::date) rn from redtape_db.maplemonk.easyecom_redtape_inventory_snapshot )where rn = 1",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from REDTAPE_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            