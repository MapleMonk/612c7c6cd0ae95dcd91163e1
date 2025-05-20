{{ config(
            materialized='table',
                post_hook={
                    "sql": "UPDATE Snitch_db.maplemonk.FACT_ITEMS_SNITCH AS A SET A.acquisition_product=B.product_name FROM ( SELECT * FROM Snitch_db.maplemonk.temp_product WHERE rowid=1)B WHERE A.customer_id = B.customer_id;",
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
            