{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.future_inwarding as WITH LatestProductDetails AS ( SELECT SKU_GROUP, PRODUCT_NAME, CATEGORY, ROW_NUMBER() OVER(PARTITION BY SKU_GROUP ORDER BY order_date DESC) as row_num FROM snitch_db.MAPLEMONK.UNICOMMERCE_FACT_ITEMS_SNITCH ), ExpectedDelivery AS ( SELECT SKU_group, CAST(expected_delivery_date AS date) as date_of_delivery, SUM(quantity) as expected_qty FROM snitch_db.MAPLEMONK.FACTORY_PRODUCTION_INVENTORY GROUP BY sku_group, expected_delivery_date ) SELECT ed.SKU_group, ed.date_of_delivery, ed.expected_qty, CASE WHEN lpd.PRODUCT_NAME is NULL THEN ed.SKU_GROUP ELSE lpd.PRODUCT_NAME END as product_name_updated, lpd.CATEGORY, lpd.row_num FROM ExpectedDelivery ed LEFT JOIN LatestProductDetails lpd ON ed.SKU_group = lpd.SKU_GROUP WHERE row_num = 1 ORDER BY ed.date_of_delivery",
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
                        