{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE Snitch_db.maplemonk.Shopify_All_customers AS select *,\'Shopify_India\' AS Shop_Name from Snitch_db.maplemonk.shopifyindia_customers; CREATE OR REPLACE TABLE Snitch_db.maplemonk.Shopify_All_orders AS select o.*,\'Shopify_India\' AS Shop_Name, CASE when s.amount = 0 then \'Prepaid\' else \'COD\' end as payment_mode from Snitch_db.maplemonk.shopifyindia_orders o LEFT JOIN (select distinct _AIRBYTE_SHOPIFYINDIA_ORDERS_HASHID,presentment_money:amount amount from Snitch_db.maplemonk.SHOPIFYINDIA_ORDERS_TOTAL_SHIPPING_PRICE_SET) s on s._AIRBYTE_SHOPIFYINDIA_ORDERS_HASHID = o._AIRBYTE_shopifyindia_ORDERS_HASHID; ALTER TABLE Snitch_db.maplemonk.Shopify_All_orders RENAME COLUMN _AIRBYTE_SHOPIFYINDIA_ORDERS_HASHID to _AIRBYTE_ORDERS_HASHID; CREATE OR REPLACE TABLE Snitch_db.maplemonk.Shopify_All_products AS select *,\'Shopify_India\' AS Shop_Name from Snitch_db.maplemonk.shopifyindia_products ; ALTER TABLE Snitch_db.maplemonk.Shopify_All_products RENAME COLUMN _AIRBYTE_SHOPIFYINDIA_PRODUCTS_HASHID to _AIRBYTE_PRODUCTS_HASHID; CREATE OR REPLACE TABLE Snitch_db.maplemonk.Shopify_All_products_variants AS select *,\'Shopify_India\' AS Shop_Name from Snitch_db.maplemonk.shopifyindia_products_variants ; ALTER TABLE Snitch_db.maplemonk.Shopify_All_products_variants RENAME COLUMN _AIRBYTE_SHOPIFYINDIA_PRODUCTS_HASHID to _AIRBYTE_PRODUCTS_HASHID; CREATE OR REPLACE TABLE Snitch_db.maplemonk.Shopify_All_customers_addresses AS select *,\'Shopify_India\' AS Shop_Name from Snitch_db.maplemonk.shopifyindia_customers_addresses ; CREATE OR REPLACE TABLE Snitch_db.maplemonk.Shopify_All_orders_items_discount AS SELECT order_id, LINE_ITEM_ID, SUM(B.VALUE:amount::FLOAT) AS DISCOUNT FROM( SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:discount_allocations AS discount_allocations FROM Snitch_db.maplemonk.Shopify_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A)x,LATERAL FLATTEN (INPUT => x.discount_allocations)B GROUP BY ORDER_ID, LINE_ITEM_ID; CREATE OR REPLACE TABLE Snitch_db.maplemonk.Shopify_All_orders_items_tax AS SELECT order_id, LINE_ITEM_ID, SUM(B.VALUE:price::FLOAT) AS TAX FROM( SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:tax_lines AS tax_lines FROM Snitch_db.maplemonk.Shopify_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A)x,LATERAL FLATTEN (INPUT => x.tax_lines)B GROUP BY order_id, LINE_ITEM_ID; CREATE OR REPLACE TABLE Snitch_db.maplemonk.Shopify_All_Refunds AS SELECT Name, ID AS Order_ID, Shop_name, C.value:line_item_id AS line_item_id, SUM(C.VALUE:quantity) AS quantity, SUM(C.VALUE:subtotal) AS subtotal FROM Snitch_db.maplemonk.Shopify_All_orders, LATERAL FLATTEN(INPUT => refunds)A, LATERAL FLATTEN(INPUT=>A.value)B, LATERAL FLATTEN(INPUT => B.value) C WHERE C.value:line_item_id IS NOT null GROUP BY Name, ID, Shop_name, C.value:line_item_id; CREATE OR REPLACE TABLE Snitch_db.maplemonk.Shopify_All_orders_items AS WITH CTE AS (SELECT SHOP_NAME, ID::VARCHAR(16777216) AS ORDER_ID, NAME AS ORDER_NAME, CUSTOMER, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:sku::STRING AS SKU, A.VALUE:product_id::STRING AS PRODUCT_ID, A.VALUE:title::STRING AS PRODUCT_NAME, app_id, CURRENCY, LANDING_SITE, referring_site, CASE WHEN cancelled_at IS NOT NULL THEN \'CANCELLED\' ELSE \'Shopify_Processed\' END AS order_status, CREATED_AT::DATETIME AS order_timestamp, A.VALUE:price::FLOAT * A.VALUE:quantity::FLOAT AS LINE_ITEM_SALES, (TOTAL_SHIPPING_PRICE_SET:presentment_money:amount::FLOAT/ COUNT(ORDER_ID) OVER(PARTITION BY ORDER_ID ORDER BY ORDER_ID)) AS SHIPPING_PRICE, A.VALUE:quantity::FLOAT as QUANTITY, \'Shopify\' AS Source, payment_mode FROM Snitch_db.maplemonk.Shopify_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A) SELECT CTE.*, IFNULL(T.TAX,0) AS TAX, IFNULL(D.DISCOUNT,0) AS DISCOUNT, CTE.LINE_ITEM_SALES - IFNULL(D.DISCOUNT,0) + IFNULL(CTE.SHIPPING_PRICE,0) AS NET_SALES, CTE.LINE_ITEM_SALES - IFNULL(D.DISCOUNT,0) as Gross_Sales, CASE WHEN R.QUANTITY IS NOT NULL THEN 1 ELSE 0 END AS IS_REFUND FROM CTE LEFT JOIN Snitch_db.maplemonk.Shopify_All_orders_items_tax T ON CTE.ORDER_ID = T.ORDER_ID AND CTE.LINE_ITEM_ID = T.LINE_ITEM_ID LEFT JOIN Snitch_db.maplemonk.Shopify_All_orders_items_discount D ON CTE.ORDER_ID = D.ORDER_ID AND CTE.LINE_ITEM_ID = D.LINE_ITEM_ID LEFT JOIN Snitch_db.maplemonk.Shopify_All_Refunds R ON CTE.ORDER_ID = R.ORDER_ID AND CTE.LINE_ITEM_ID = R.LINE_ITEM_ID; CREATE OR REPLACE TABLE Snitch_db.maplemonk.FACT_ITEMS_SNITCH AS select a.*, case when cm.mapped_cuty is not null then cm.mapped_cuty else a.city end as city_mapped from ( SELECT O.SHOP_NAME, O.ORDER_ID, O.ORDER_NAME, O.CUSTOMER:id::int AS customer_id, O.LINE_ITEM_ID, o.app_id, O.SKU, O.PRODUCT_ID, CASE WHEN O.PRODUCT_NAME IS NULL THEN \'NA\' ELSE O.PRODUCT_NAME END AS PRODUCT_NAME, O.CURRENCY, O.IS_REFUND, CASE WHEN CD.city IS NULL OR CD.city = \'\' THEN \'NA\' ELSE Cd.city END AS city, CASE WHEN CD.province IS NULL OR CD.province = \'\' THEN \'NA\' ELSE CD.province END AS state, CASE WHEN P.product_type = \'\' THEN \'NA\' WHEN P.product_type = \'Jeans\' THEN \'Denim\' WHEN P.product_type = \'Pant\' then \'Pants\' ELSE P.product_type END AS category, CASE WHEN O.SKU LIKE \'%4MSQ0011%\' THEN \'LUXE\' WHEN O.SKU NOT LIKE \'%4MSQ0011%\' AND M.SKUS is NOT NULL THEN \'PLUS\' WHEN O.SKU NOT LIKE \'%4MSQ0011%\' AND N.SKUS is NOT NULL THEN \'INNERWEAR\' ELSE \'OTHER\' END AS Segment, CASE WHEN O.APP_ID = \'2653365\' THEN \'Shopney\' ELSE \'Web\' end as WebShopney, O.order_status, O.order_timestamp, O.LINE_ITEM_SALES, O.SHIPPING_PRICE, O.QUANTITY, O.TAX, O.DISCOUNT, O.NET_SALES, o.Gross_sales, O.Source, o.landing_site, o.referring_site, o.payment_mode FROM Snitch_db.maplemonk.Shopify_All_orders_items O LEFT JOIN Snitch_db.maplemonk.Shopify_All_products P ON O.PRODUCT_ID = P.id left join snitch_db.maplemonk.snitch_plus_sku_mapping m on o.sku = m.skus left join snitch_db.maplemonk.snitch_innerwear_sku_mapping n on o.sku = n.skus LEFT JOIN(SELECT customer_id, city, province, row_number() OVER ( partition BY customer_id ORDER BY id DESC) rowid FROM Snitch_db.maplemonk.Shopify_All_customers_addresses) AS CD ON O.CUSTOMER:id::int = CD.customer_id AND CD.rowid = 1 ) a left join (select distinct city, mapped_cuty from snitch_db.maplemonk.snitch_city_mapping where mapped_cuty is not null) cm on a.city = cm.city ; ALTER TABLE Snitch_db.maplemonk.FACT_ITEMS_SNITCH ADD COLUMN customer_flag varchar(50); ALTER TABLE Snitch_db.maplemonk.FACT_ITEMS_SNITCH ADD COLUMN new_customer_flag varchar(50); ALTER TABLE Snitch_db.maplemonk.FACT_ITEMS_SNITCH ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE Snitch_db.maplemonk.FACT_ITEMS_SNITCH ADD COLUMN acquisition_product varchar(16777216); UPDATE Snitch_db.maplemonk.FACT_ITEMS_SNITCH AS A SET A.customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id, order_timestamp, CASE WHEN order_timestamp <> Min(order_timestamp) OVER ( partition BY customer_id) THEN \'Repeated\' ELSE \'New\' END AS Flag FROM Snitch_db.maplemonk.FACT_ITEMS_SNITCH)AS B WHERE A.order_id = B.order_id AND A.customer_id = B.customer_id; UPDATE Snitch_db.maplemonk.FACT_ITEMS_SNITCH SET customer_flag = CASE WHEN customer_flag IS NULL THEN \'New\' ELSE customer_flag END; UPDATE Snitch_db.maplemonk.FACT_ITEMS_SNITCH AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id, order_timestamp, CASE WHEN Last_day(order_timestamp, \'month\') <> Last_day(Min(order_timestamp) OVER ( partition BY customer_id)) THEN \'Repeated\' ELSE \'New\' END AS Flag FROM Snitch_db.maplemonk.FACT_ITEMS_SNITCH)AS B WHERE A.order_id = B.order_id AND A.customer_id = B.customer_id; UPDATE Snitch_db.maplemonk.FACT_ITEMS_SNITCH SET new_customer_flag = CASE WHEN new_customer_flag IS NULL THEN \'New\' ELSE new_customer_flag END; CREATE OR replace temporary TABLE Snitch_db.maplemonk.temp_source AS SELECT DISTINCT customer_id, source FROM ( SELECT DISTINCT customer_id, order_timestamp, source, Min(order_timestamp) OVER ( partition BY customer_id) firstOrderdate FROM Snitch_db.maplemonk.FACT_ITEMS_SNITCH)res WHERE order_timestamp=firstorderdate; UPDATE Snitch_db.maplemonk.FACT_ITEMS_SNITCH AS a SET a.acquisition_channel=b.source FROM Snitch_db.maplemonk.temp_source b WHERE a.customer_id = b.customer_id; CREATE OR replace temporary TABLE Snitch_db.maplemonk.temp_product AS SELECT DISTINCT customer_id, product_name, Row_number() OVER (partition BY customer_id ORDER BY LINE_ITEM_SALES DESC) rowid FROM ( SELECT DISTINCT customer_id, order_timestamp, product_name, LINE_ITEM_SALES , Min(order_timestamp) OVER (partition BY customer_id) firstOrderdate FROM Snitch_db.maplemonk.FACT_ITEMS_SNITCH )res WHERE order_timestamp=firstorderdate; UPDATE Snitch_db.maplemonk.FACT_ITEMS_SNITCH AS A SET A.acquisition_product=B.product_name FROM ( SELECT * FROM Snitch_db.maplemonk.temp_product WHERE rowid=1)B WHERE A.customer_id = B.customer_id;",
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
                        