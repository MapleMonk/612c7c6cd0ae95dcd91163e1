{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table redtape_db.maplemonk.new_products_inventory as select sku, product_name, first_date, quantity from (select sku, product_name, first_date, quantity, row_number() over (partition by sku order by first_date) rn from (select sku , \"Product Name\" product_name , \"Report Generated Date\"::date first_Date , sum(\"Available Quantity\") quantity from redtape_db.maplemonk.easyecom_redtape_inventory_snapshot group by 1,2,3 ) )where rn = 1 ;",
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
            