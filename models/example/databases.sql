{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE snitch_db.snitch.snitch_db_discount_decile AS WITH CumulativeSales AS ( SELECT lf.order_id, lf.sales_order_item_id, lf.order_date, lf.marketplace_mapped, lf.market_place, lf.suborder_quantity, lf.sku, AM.sku_class, AM.available_units, AM.sales_last_7_days, AM.sales_last_15_days, AM.sales_last_30_days, Am.NUM_SIZE_AVAILABLE, case when upper(lf.marketplace_mapped) in (\'AJIO\',\'MYNTRA\',\'AMAZON\',\'FYND\',\'MENSXP\') then lf.selling_price * ifnull(lf.shipping_quantity ,lf.suborder_quantity) when upper(lf.marketplace_mapped) = upper(\'Shopify_India\') then lf.selling_price else lf.selling_price * ifnull(lf.suborder_quantity,lf.shipping_quantity) end as selling_price1, case when upper(lf.marketplace_mapped) in (\'AJIO\',\'MYNTRA\',\'AMAZON\',\'FYND\',\'MENSXP\') then Coalesce (pd.price,lf.mrp) * ifnull(lf.shipping_quantity ,lf.suborder_quantity) else Coalesce (pd.price,lf.mrp) * ifnull(lf.suborder_quantity,lf.shipping_quantity) end as mrp1, pd.product_category, CASE WHEN selling_price1 < 1049 THEN 1.05 ELSE 1.12 END AS SELLING_PRICE_DISCOUNT, CASE WHEN mrp1 < 1049 THEN 1.05 ELSE 1.12 END AS mrp_discount, sgt.*, fis.webshopney, fis.new_customer_flag FROM snitch_db.snitch.order_lineitems_fact lf LEFT JOIN snitch_db.snitch.product_dim pd ON LOWER(pd.sku) = LOWER(lf.sku) LEFT JOIN snitch_db.maplemonk.availability_master AM on lower(AM.sku_group) = lower(REVERSE(SUBSTRING(REVERSE(lf.sku), CHARINDEX(\'-\', REVERSE(lf.SKU)) + 1))) LEFT JOIN snitch_db.maplemonk.sku_group_tags sgt on lower(sgt.sku_group) = lower(REVERSE(SUBSTRING(REVERSE(lf.sku), CHARINDEX(\'-\', REVERSE(lf.SKU)) + 1))) LEFT JOIN snitch_db.maplemonk.fact_items_snitch fis on lf.SALES_ORDER_ITEM_ID = fis.line_item_id ), Percentage AS ( SELECT *, (MRP1 / mrp_discount - selling_price1 / SELLING_PRICE_DISCOUNT) AS dis, div0(dis , div0(MRP1 ,mrp_discount))*100 AS discount, case when discount=0 then \'d00\' when discount<=10 then \'d0\' when discount<=20 then \'d1\' when discount<=30 then \'d2\' when discount<=40 then \'d3\' when discount<=50 then \'d4\' when discount<=60 then \'d5\' when discount<=70 then \'d6\' when discount<=80 then \'d7\' when discount<=90 then \'d8\' when discount<=100 then \'d9\' when discount=100 then \'d100\' when discount>100 then \'d10\' end as decile FROM CumulativeSales ) SELECT * FROM Percentage;",
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
                        