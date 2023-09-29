{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.channel_wise_pricing as WITH products AS ( WITH latest_orders AS ( SELECT fact_items_snitch.sku_group, fact_items_snitch.product_name, fact_items_snitch.category, ROW_NUMBER() OVER(PARTITION BY sku_group ORDER BY order_timestamp DESC) as rn FROM snitch_db.maplemonk.fact_items_snitch ) SELECT sku_group, product_name, category FROM latest_orders WHERE rn = 1 ) SELECT main.sku_group, main.order_date, extract(MONTH from main.order_date) as month_order, extract(YEAR from main.order_date) as year_order, prod.product_name, prod.category, SUM(CASE WHEN upper(marketplace_mapped)=\'AJIO\' THEN selling_price*1.42 ELSE 0 END) AS ajio_sales, SUM(CASE WHEN upper(marketplace_mapped)=\'AJIO\' THEN suborder_quantity ELSE 0 END) AS ajio_units_sold, COALESCE(SUM(CASE WHEN upper(marketplace_mapped)=\'AJIO\' THEN selling_price*1.42 ELSE 0 END) / NULLIF(SUM(CASE WHEN upper(marketplace_mapped)=\'AJIO\' THEN suborder_quantity ELSE 0 END), 0), 0) AS ajio_asp, SUM(CASE WHEN upper(marketplace_mapped)=\'FLIPKART\' THEN selling_price ELSE 0 END) AS fk_sales, SUM(CASE WHEN upper(marketplace_mapped)=\'FLIPKART\' THEN suborder_quantity ELSE 0 END) AS fk_units_sold, COALESCE(SUM(CASE WHEN upper(marketplace_mapped)=\'FLIPKART\' THEN selling_price ELSE 0 END) / NULLIF(SUM(CASE WHEN upper(marketplace_mapped)=\'FLIPKART\' THEN suborder_quantity ELSE 0 END), 0), 0) AS fk_asp, SUM(CASE WHEN upper(marketplace_mapped)=\'SHOPIFY\' THEN selling_price ELSE 0 END) AS shopify_sales, SUM(CASE WHEN upper(marketplace_mapped)=\'SHOPIFY\' THEN suborder_quantity ELSE 0 END) AS shopify_units_sold, COALESCE(SUM(CASE WHEN upper(marketplace_mapped)=\'SHOPIFY\' THEN selling_price ELSE 0 END) / NULLIF(SUM(CASE WHEN upper(marketplace_mapped)=\'SHOPIFY\' THEN suborder_quantity ELSE 0 END), 0), 0) AS shopify_asp, SUM(CASE WHEN upper(marketplace_mapped)=\'SNITCH - JAYANAGAR\' THEN selling_price ELSE 0 END) AS jayanagar_sales, SUM(CASE WHEN upper(marketplace_mapped)=\'SNITCH - JAYANAGAR\' THEN CASE WHEN upper(marketplace_mapped)=\'MYNTRA\' THEN shipping_quantity ELSE suborder_quantity END ELSE 0 END) AS jayanagar_units_sold, COALESCE(SUM(CASE WHEN upper(marketplace_mapped)=\'SNITCH - JAYANAGAR\' THEN selling_price ELSE 0 END) / NULLIF(SUM(CASE WHEN upper(marketplace_mapped)=\'SNITCH - JAYANAGAR\' THEN CASE WHEN upper(marketplace_mapped)=\'MYNTRA\' THEN shipping_quantity ELSE suborder_quantity END ELSE 0 END), 0), 0) AS jayanagar_asp, SUM(CASE WHEN upper(marketplace_mapped)=\'BEWAKOOF\' THEN selling_price ELSE 0 END) AS bewakoof_sales, SUM(CASE WHEN upper(marketplace_mapped)=\'BEWAKOOF\' THEN CASE WHEN upper(marketplace_mapped)=\'MYNTRA\' THEN shipping_quantity ELSE suborder_quantity END ELSE 0 END) AS bewakoof_units_sold, COALESCE(SUM(CASE WHEN upper(marketplace_mapped)=\'BEWAKOOF\' THEN selling_price ELSE 0 END) / NULLIF(SUM(CASE WHEN upper(marketplace_mapped)=\'BEWAKOOF\' THEN CASE WHEN upper(marketplace_mapped)=\'MYNTRA\' THEN shipping_quantity ELSE suborder_quantity END ELSE 0 END), 0), 0) AS bewakoof_asp, SUM(CASE WHEN upper(marketplace_mapped)=\'NYKAA_FASHION\' THEN selling_price ELSE 0 END) AS nykaa_fashion_sales, SUM(CASE WHEN upper(marketplace_mapped)=\'NYKAA_FASHION\' THEN suborder_quantity ELSE 0 END) AS nykaa_fashion_units_sold, COALESCE(SUM(CASE WHEN upper(marketplace_mapped)=\'NYKAA_FASHION\' THEN selling_price ELSE 0 END) / NULLIF(SUM(CASE WHEN upper(marketplace_mapped)=\'NYKAA_FASHION\' THEN suborder_quantity ELSE 0 END), 0), 0) AS nykaa_fashion_asp, SUM(CASE WHEN upper(marketplace_mapped)=\'MENSXP\' THEN selling_price ELSE 0 END) AS mensxp_sales, SUM(CASE WHEN upper(marketplace_mapped)=\'MENSXP\' THEN suborder_quantity ELSE 0 END) AS mensxp_units_sold, COALESCE(SUM(CASE WHEN upper(marketplace_mapped)=\'MENSXP\' THEN selling_price ELSE 0 END) / NULLIF(SUM(CASE WHEN upper(marketplace_mapped)=\'MENSXP\' THEN suborder_quantity ELSE 0 END), 0), 0) AS mensxp_asp, SUM(CASE WHEN upper(marketplace_mapped)=\'FYND\' THEN selling_price ELSE 0 END) AS fynd_sales, SUM(CASE WHEN upper(marketplace_mapped)=\'FYND\' THEN suborder_quantity ELSE 0 END) AS fynd_units_sold, COALESCE(SUM(CASE WHEN upper(marketplace_mapped)=\'FYND\' THEN selling_price ELSE 0 END) / NULLIF(SUM(CASE WHEN upper(marketplace_mapped)=\'FYND\' THEN suborder_quantity ELSE 0 END), 0), 0) AS fynd_asp, SUM(CASE WHEN upper(marketplace_mapped)=\'DONOSHOP\' THEN selling_price ELSE 0 END) AS donoshop_sales, SUM(CASE WHEN upper(marketplace_mapped)=\'DONOSHOP\' THEN suborder_quantity ELSE 0 END) AS donoshop_units_sold, COALESCE(SUM(CASE WHEN upper(marketplace_mapped)=\'DONOSHOP\' THEN selling_price ELSE 0 END) / NULLIF(SUM(CASE WHEN upper(marketplace_mapped)=\'DONOSHOP\' THEN suborder_quantity ELSE 0 END), 0), 0) AS donoshop_asp, SUM(CASE WHEN upper(marketplace_mapped)=\'CUSTOM\' THEN selling_price ELSE 0 END) AS custom_sales, SUM(CASE WHEN upper(marketplace_mapped)=\'CUSTOM\' THEN suborder_quantity ELSE 0 END) AS custom_units_sold, COALESCE(SUM(CASE WHEN upper(marketplace_mapped)=\'CUSTOM\' THEN selling_price ELSE 0 END) / NULLIF(SUM(CASE WHEN upper(marketplace_mapped)=\'CUSTOM\' THEN suborder_quantity ELSE 0 END), 0), 0) AS custom_asp, SUM(CASE WHEN upper(marketplace_mapped)=\'MYNTRA\' THEN selling_price ELSE 0 END) AS myntra_sales, SUM(CASE WHEN upper(marketplace_mapped)=\'MYNTRA\' THEN shipping_quantity ELSE 0 END) AS myntra_units_sold, COALESCE(SUM(CASE WHEN upper(marketplace_mapped)=\'MYNTRA\' THEN selling_price ELSE 0 END) / NULLIF(SUM(CASE WHEN upper(marketplace_mapped)=\'MYNTRA\' THEN shipping_quantity ELSE 0 END), 0), 0) AS myntra_asp FROM snitch_db.maplemonk.unicommerce_fact_items_snitch main LEFT JOIN products prod ON main.sku_group = prod.sku_group where upper(order_status) not in (\'CANCELLED\') GROUP BY main.sku_group, prod.product_name, prod.category, main.order_date, month_order, year_order",
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
                        