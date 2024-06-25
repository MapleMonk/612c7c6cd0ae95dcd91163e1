{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.Sku_Status_Demand AS WITH shopify_data AS ( SELECT REPLACE(A.value:position, \'\"\', \'\') AS pst, p.status AS status, REVERSE(SUBSTRING(REVERSE(B.value:sku), CHARINDEX(\'-\', REVERSE(B.value:sku)) + 1)) AS sku_group FROM snitch_db.MAPLEMONK.SHOPIFY_ALL_PRODUCTS p, LATERAL FLATTEN(INPUT => p.IMAGES) A, LATERAL FLATTEN(INPUT => p.variants) B WHERE pst = 1 ), availability_data AS ( SELECT sku_group, sku_class, SUM(available_units) AS total_available_units, category, product_name, natural_ros FROM snitch_db.maplemonk.AVAILABILITY_MASTER_V2 GROUP BY sku_group, sku_class, category, product_name, natural_ros ) SELECT Distinct(availability_data.sku_group), availability_data.sku_class, availability_data.total_available_units, availability_data.category, availability_data.product_name, availability_data.natural_ros, shopify_data.pst, shopify_data.status, CASE WHEN availability_data.total_available_units = 0 THEN \'No Stock\' WHEN availability_data.total_available_units > 0 AND availability_data.total_available_units <= 10 THEN \'Less Than 10\' WHEN availability_data.total_available_units > 10 AND availability_data.total_available_units <= 100 THEN \'Less Than 100\' WHEN availability_data.total_available_units > 100 AND availability_data.total_available_units <= 500 THEN \'Less Than 500\' WHEN availability_data.total_available_units > 500 AND availability_data.total_available_units <= 1000 THEN \'Less Than 500\' WHEN availability_data.total_available_units > 1000 THEN \'More Than 500\' END AS Inventory_slab FROM availability_data LEFT JOIN shopify_data ON availability_data.sku_group = shopify_data.sku_group",
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
                        