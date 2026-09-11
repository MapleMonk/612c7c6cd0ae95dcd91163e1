{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.LUGGAGE_MASTER AS WITH offline AS ( SELECT o.ORDER_DATE, o.branch_code AS MARKETPLACE, o.MARKETPLACE_MAPPED, o.SOURCE AS Order_id, o.SKU, o.sku_group, CASE WHEN o.sku LIKE \'4MTL0001%\' THEN \'BLINK\' WHEN o.SKU LIKE \'4MTL0002%\' THEN \'VITO\' WHEN o.SKU LIKE \'4MTL0003%\' THEN \'RUBIK\' ELSE \'Other\' END AS Style, CASE WHEN RIGHT(o.SKU,1) LIKE \'3\' THEN \'SET-3\' WHEN RIGHT(o.sku,1) LIKE \'2\' THEN \'SET-2\' ELSE RIGHT(o.SKU,1) END AS SIZE, RIGHT(LEFT(o.SKU,11),2) AS COLOR, s.\"Shipping Address City\" AS CITY, s.state AS STATE, s.\"Shipping Address Pincode\" AS PINCODE, o.SUBORDER_QUANTITY AS QTY, o.MRP, ROUND(o.SELLING_PRICE,0) AS SELLING_PRICE, o.COGS_PRICE AS COGS, ROUND(o.SELLING_PRICE,0) - o.COGS_PRICE AS GROSS_PROFIT, \'\' AS shippingpackagecode, \'\' AS shippingpackagestatus, \'\' AS shipping_courier, CAST(NULL AS DATE) AS delivered_date, \'\' AS AWB, \'\' AS warehouse_name FROM snitch_db.maplemonk.store_fact_items_offline o LEFT JOIN GS_STORE_DETAILS s ON o.branch_code = s.branch_code WHERE o.sku_group LIKE \'4MTL0%\' AND o.doc_prefix NOT LIKE \'%QCOM%\' AND o.doc_prefix NOT LIKE \'%OMS%\' ), online AS ( SELECT order_date, marketplace, marketplace_mapped, order_name AS Order_id, sku, sku_group, CASE WHEN sku LIKE \'4MTL0001%\' THEN \'BLINK\' WHEN SKU LIKE \'4MTL0002%\' THEN \'VITO\' WHEN SKU LIKE \'4MTL0003%\' THEN \'RUBIK\' ELSE \'Other\' END AS Style, CASE WHEN RIGHT(SKU,1) LIKE \'3\' THEN \'SET-3\' WHEN RIGHT(sku,1) LIKE \'2\' THEN \'SET-2\' ELSE RIGHT(SKU,1) END AS SIZE, RIGHT(LEFT(SKU,11),2) AS COLOR, city, state, pincode, suborder_quantity AS QTY, mrp, selling_price AS SELLING_PRICE, cost AS COGS, selling_price - cost AS GROSS_PROFIT, shippingpackagecode, shippingpackagestatus, shipping_courier, delivered_date, AWB, warehouse_name FROM snitch_db.maplemonk.unicommerce_fact_items_intermediate WHERE sku_group LIKE \'4MTL0%\' AND sale_order_item_status NOT IN (\'CANCELLED\') AND marketplace_mapped LIKE \'SHOPIFY\' ), combined AS ( SELECT * FROM offline UNION ALL SELECT * FROM online ), cp AS ( SELECT ORDER_ID, awb_number, pre_current_status, status, quantity_in_box, courier_partner, return_reason, sku_list, journey FROM cp_1 WHERE SKU_LIST LIKE \'4MTL0%\' AND order_id NOT IN ( SELECT order_id FROM cp_1 WHERE journey IN (\'RTO\',\'Reverse\') AND status IN (\'CANCELLED\') AND sku_list LIKE \'4MTL0%\' ) ) SELECT c.*, cp.awb_number, cp.pre_current_status, cp.status, cp.quantity_in_box, cp.courier_partner, cp.return_reason, cp.sku_list, cp.journey FROM combined c LEFT JOIN cp ON c.Order_id = cp.ORDER_ID ORDER BY c.ORDER_DATE DESC ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from SNITCH_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            