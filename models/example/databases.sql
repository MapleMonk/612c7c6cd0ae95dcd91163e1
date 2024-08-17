{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.category_visibility AS ( WITH cat_sales AS ( SELECT order_timestamp::date AS date, sku_group, sku_class, sum(gross_sales) as total_sales, SUM(gross_sales-rto_sales-dto_sales-cancel_sales) AS net_sales, sum(quantity) as total_quant, SUM(quantity-rto_quant-dto_quant-cancel_quant) AS net_quant FROM snitch_db.maplemonk.fact_items_snitch WHERE LOWER(IFNULL(discount_code, \'n\')) NOT LIKE \'%eco%\' AND LOWER(IFNULL(discount_code, \'n\')) NOT LIKE \'%influ%\' AND order_name NOT IN (\'2431093\',\'2422140\',\'2425364\',\'2430652\',\'2422237\',\'2420623\',\'2429832\',\'2422378\',\'2428311\',\'2429064\',\'2428204\',\'2421343\',\'2431206\',\'2430491\',\'2426682\',\'2426487\',\'2426458\',\'2423575\',\'2422431\',\'2423612\',\'2426625\',\'2428117\',\'2426894\',\'2425461\',\'2426570\',\'2423455\',\'2430777\',\'2426009\',\'2428245\',\'2427269\',\'2430946\',\'2425821\',\'2429986\',\'2429085\',\'2422047\',\'2430789\',\'2420219\',\'2428341\',\'2430444\',\'2426866\',\'2431230\',\'2425839\',\'2430980\',\'2427048\',\'2430597\',\'2420499\',\'2431050\',\'2420271\',\'2426684\',\'2428747\',\'2423523\',\'2431171\',\'2430830\',\'2425325\',\'2428414\',\'2429054\',\'2423596\') AND tags NOT IN (\'FLITS_LOGICERP\') GROUP BY 1, 2, 3 ), inventory AS ( SELECT date, REVERSE(SUBSTRING(REVERSE(\"Item SkuCode\"), CHARINDEX(\'-\', REVERSE(\"Item SkuCode\")) + 1, LEN(\"Item SkuCode\"))) AS sku_group, SUM(inventory) AS sellable_inventory, sum(inventory)*avg(mrp) as inventory_value FROM snitch_db.maplemonk.snitch_final_inventory_wh2 GROUP BY 1, 2 ), main_data AS ( SELECT a.*, b.sku_class,b.total_sales,b.total_quant,b.net_sales,b.net_quant FROM inventory a LEFT JOIN cat_sales b ON a.date = b.date AND a.sku_group = b.sku_group ), metafields AS ( SELECT *, ROW_NUMBER() OVER (PARTITION BY sku_group ORDER BY date_wise DESC) AS rn FROM snitch_db.maplemonk.Product_info ), recent_metafields AS ( SELECT sku_group, new_category, occassion, print_design, collar, material, sleeve_type, fit, color, designs, collar_new, material_new, occassion_new FROM metafields WHERE rn = 1 ), main_data2 as ( SELECT a.*, b.new_category, b.occassion, b.print_design, b.collar, b.material, b.sleeve_type, b.fit, b.color, b.designs, b.collar_new, b.material_new, b.occassion_new FROM main_data a left JOIN recent_metafields b ON a.sku_group = b.sku_group ), spends as ( select ga_date, sku_group, clicks, spends from snitch_db.maplemonk.sku_group_ad_inventory_check ) SELECT a.*,b.clicks as paid_clicks,b.spends FROM main_data2 a LEFT JOIN spends b ON a.sku_group = b.sku_group and a.date=b.ga_date );",
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
            