{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR replace temporary TABLE Snitch_db.maplemonk.temp_product AS SELECT DISTINCT customer_id, product_name, Row_number() OVER (partition BY customer_id ORDER BY LINE_ITEM_SALES DESC) rowid FROM ( SELECT DISTINCT customer_id, order_timestamp, product_name, LINE_ITEM_SALES , Min(order_timestamp) OVER (partition BY customer_id) firstOrderdate FROM Snitch_db.maplemonk.FACT_ITEMS_SNITCH where lower(order_Status) not in (\'cancelled\',\'returned\'))res WHERE order_timestamp=firstorderdate; UPDATE Snitch_db.maplemonk.FACT_ITEMS_SNITCH AS A SET A.acquisition_product=B.product_name FROM ( SELECT * FROM Snitch_db.maplemonk.temp_product WHERE rowid=1)B WHERE A.customer_id = B.customer_id; CREATE OR replace temporary TABLE Snitch_db.maplemonk.temp_material_colour AS SELECT DISTINCT customer_id, material_colour, Row_number() OVER (partition BY customer_id ORDER BY LINE_ITEM_SALES DESC) rowid FROM ( SELECT DISTINCT customer_id, order_timestamp, concat(material,\' \',colour) material_colour, LINE_ITEM_SALES , Min(order_timestamp) OVER (partition BY customer_id) firstOrderdate FROM Snitch_db.maplemonk.FACT_ITEMS_SNITCH where lower(order_Status) not in (\'cancelled\',\'returned\'))res WHERE order_timestamp=firstorderdate; UPDATE Snitch_db.maplemonk.FACT_ITEMS_SNITCH AS A SET A.acquisition_material_colour=B.material_colour FROM ( SELECT * FROM Snitch_db.maplemonk.temp_material_colour WHERE rowid=1)B WHERE A.customer_id = B.customer_id;",
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
            