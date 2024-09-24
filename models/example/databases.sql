{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.category_visibility AS ( WITH cat_sales AS ( SELECT order_timestamp::date AS date, sku_group, sku_class, sum(gross_sales) as total_sales, sum(cancel_sales) as cancel_sales, sum(cancel_quant) as cancel_quant, sum(quantity) as total_quant FROM snitch_db.maplemonk.fact_items_snitch WHERE LOWER(IFNULL(discount_code, \'n\')) NOT LIKE \'%eco%\' AND LOWER(IFNULL(discount_code, \'n\')) NOT LIKE \'%influ%\' AND order_name NOT IN (\'2431093\',\'2422140\',\'2425364\',\'2430652\',\'2422237\',\'2420623\',\'2429832\',\'2422378\',\'2428311\',\'2429064\',\'2428204\',\'2421343\',\'2431206\',\'2430491\',\'2426682\',\'2426487\',\'2426458\',\'2423575\',\'2422431\',\'2423612\',\'2426625\',\'2428117\',\'2426894\',\'2425461\',\'2426570\',\'2423455\',\'2430777\',\'2426009\',\'2428245\',\'2427269\',\'2430946\',\'2425821\',\'2429986\',\'2429085\',\'2422047\',\'2430789\',\'2420219\',\'2428341\',\'2430444\',\'2426866\',\'2431230\',\'2425839\',\'2430980\',\'2427048\',\'2430597\',\'2420499\',\'2431050\',\'2420271\',\'2426684\',\'2428747\',\'2423523\',\'2431171\',\'2430830\',\'2425325\',\'2428414\',\'2429054\',\'2423596\') AND tags NOT IN (\'FLITS_LOGICERP\') GROUP BY 1,2,3 ), RTO_Data as ( select date(FI.RTO_DATE::date) Date ,sku_group ,sum(rto_sales) rto_sales ,sum(rto_quant) rto_quant from Snitch_db.maplemonk.FACT_ITEMS_SNITCH FI where lower(ifnull(discount_code,\'n\')) not like \'%eco%\' and lower(ifnull(discount_code,\'n\')) not like \'%influ%\' and order_name not in (\'2431093\',\'2422140\',\'2425364\',\'2430652\',\'2422237\',\'2420623\',\'2429832\',\'2422378\',\'2428311\',\'2429064\',\'2428204\',\'2421343\',\'2431206\',\'2430491\',\'2426682\',\'2426487\',\'2426458\',\'2423575\',\'2422431\',\'2423612\',\'2426625\',\'2428117\',\'2426894\',\'2425461\',\'2426570\',\'2423455\',\'2430777\',\'2426009\',\'2428245\',\'2427269\',\'2430946\',\'2425821\',\'2429986\',\'2429085\',\'2422047\',\'2430789\',\'2420219\',\'2428341\',\'2430444\',\'2426866\',\'2431230\',\'2425839\',\'2430980\',\'2427048\',\'2430597\',\'2420499\',\'2431050\',\'2420271\',\'2426684\',\'2428747\',\'2423523\',\'2431171\',\'2430830\',\'2425325\',\'2428414\',\'2429054\',\'2423596\') and tags not in (\'FLITS_LOGICERP\') and RTO_DATE is not null group by 1,2 ), DTO_Data as ( select date(FI.DTO_Date::date) Date ,sku_group ,sum(dto_sales) dto_sales ,sum(dto_quant) dto_quant from Snitch_db.maplemonk.FACT_ITEMS_SNITCH FI where lower(ifnull(discount_code,\'n\')) not like \'%eco%\' and lower(ifnull(discount_code,\'n\')) not like \'%influ%\' and order_name not in (\'2431093\',\'2422140\',\'2425364\',\'2430652\',\'2422237\',\'2420623\',\'2429832\',\'2422378\',\'2428311\',\'2429064\',\'2428204\',\'2421343\',\'2431206\',\'2430491\',\'2426682\',\'2426487\',\'2426458\',\'2423575\',\'2422431\',\'2423612\',\'2426625\',\'2428117\',\'2426894\',\'2425461\',\'2426570\',\'2423455\',\'2430777\',\'2426009\',\'2428245\',\'2427269\',\'2430946\',\'2425821\',\'2429986\',\'2429085\',\'2422047\',\'2430789\',\'2420219\',\'2428341\',\'2430444\',\'2426866\',\'2431230\',\'2425839\',\'2430980\',\'2427048\',\'2430597\',\'2420499\',\'2431050\',\'2420271\',\'2426684\',\'2428747\',\'2423523\',\'2431171\',\'2430830\',\'2425325\',\'2428414\',\'2429054\',\'2423596\') and tags not in (\'FLITS_LOGICERP\') and DTO_Date is not null group by 1,2 ), PreMain as ( SELECT COALESCE(a.date, b.Date, c.Date) AS date, coalesce(a.sku_group,b.sku_group,c.sku_group) as sku_group, a.sku_class, COALESCE(a.total_sales, 0) AS total_sales, COALESCE(a.total_quant, 0) AS total_quant, COALESCE(c.rto_sales, 0) AS rto_sales, COALESCE(c.rto_quant, 0) AS rto_quant, COALESCE(b.dto_sales, 0) AS dto_sales, COALESCE(b.dto_quant, 0) AS dto_quant, COALESCE(a.cancel_sales, 0) AS cancel_sales, COALESCE(a.total_sales, 0) - COALESCE(b.dto_sales, 0) - COALESCE(c.rto_sales, 0) - COALESCE(a.cancel_sales, 0) as net_sales, COALESCE(a.total_quant, 0) - COALESCE(b.dto_quant, 0) - COALESCE(c.rto_quant, 0) - COALESCE(a.cancel_quant, 0) as net_quant FROM cat_sales a full outer JOIN DTO_Data b ON a.date = b.Date AND a.sku_group = b.sku_group full outer JOIN RTO_Data c ON COALESCE(a.date, b.Date) = c.date AND COALESCE(a.sku_group, b.sku_group) = c.sku_group ), inventory AS ( SELECT date, REVERSE(SUBSTRING(REVERSE(\"Item SkuCode\"), CHARINDEX(\'-\', REVERSE(\"Item SkuCode\")) + 1, LEN(\"Item SkuCode\"))) AS sku_group, SUM(inventory) AS sellable_inventory, SUM(inventory) * AVG(mrp) as inventory_value FROM snitch_db.maplemonk.snitch_final_inventory_wh2 GROUP BY 1, 2 ), main_data AS ( SELECT a.date, a.sku_group, b.sku_class, b.total_sales, b.total_quant, b.net_sales, b.net_quant, a.sellable_inventory, a.inventory_value FROM inventory a LEFT JOIN PreMain b ON a.date = b.date AND a.sku_group = b.sku_group ), metafields AS ( SELECT *, ROW_NUMBER() OVER (PARTITION BY sku_group ORDER BY date_wise DESC) AS rn FROM snitch_db.maplemonk.Product_info ), recent_metafields AS ( SELECT sku_group, new_category, occassion, print_design, collar, material, sleeve_type, fit, color, designs, collar_new, material_new, occassion_new FROM metafields WHERE rn = 1 ), main_data2 as ( SELECT a.*, b.new_category, b.occassion, b.print_design, b.collar, b.material, b.sleeve_type, b.fit, b.color, b.designs, b.collar_new, b.material_new, b.occassion_new FROM main_data a LEFT JOIN recent_metafields b ON a.sku_group = b.sku_group ), spends as ( SELECT ga_date, sku_group, clicks, spends FROM snitch_db.maplemonk.sku_group_ad_inventory_check ), price as ( select sku_group,price from snitch_db.maplemonk.availability_master_v2 ) SELECT a.*, b.clicks as paid_clicks, b.spends, c.price FROM main_data2 a LEFT JOIN spends b ON a.sku_group = b.sku_group AND a.date = b.ga_date left join price c on a.sku_group = c.sku_group );",
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
            