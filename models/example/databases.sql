{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE Snitch_db.maplemonk.Shopify_All_customers AS select *,\'Shopify_India\' AS Shop_Name from Snitch_db.maplemonk.shopifyindia_customers; CREATE OR REPLACE TABLE Snitch_db.MAPLEMONK.Shopify_All_orders AS SELECT *, iff(charindex(\'utm_medium=\', landing_site)=0,NULL,substring(landing_site, charindex(\'utm_medium=\', landing_site) + len(\'utm_medium=\'), ifnull(nullif(charindex(\'&\', insert(landing_site, 1, charindex(\'utm_medium=\', landing_site)-1, \'\')),0), len(insert(landing_site, 1, charindex(\'utm_medium=\', landing_site)-1, \'\'))+1) - len(\'utm_medium=\')-1)) as LANDING_UTM_MEDIUM, iff(charindex(\'utm_source=\', landing_site)=0,NULL,substring(landing_site, charindex(\'utm_source=\', landing_site) + len(\'utm_source=\'), ifnull(nullif(charindex(\'&\', insert(landing_site, 1, charindex(\'utm_source=\', landing_site)-1, \'\')),0), len(insert(landing_site, 1, charindex(\'utm_source=\', landing_site)-1, \'\'))+1) - len(\'utm_source=\')-1)) as LANDING_UTM_SOURCE, iff(charindex(\'utm_campaign=\', landing_site)=0,NULL,substring(landing_site, charindex(\'utm_campaign=\', landing_site) + len(\'utm_campaign=\'), ifnull(nullif(charindex(\'&\', insert(landing_site, 1, charindex(\'utm_campaign=\', landing_site)-1, \'\')),0), len(insert(landing_site, 1, charindex(\'utm_campaign=\', landing_site)-1, \'\'))+1) - len(\'utm_campaign=\')-1)) as LANDING_UTM_CAMPAIGN, iff(charindex(\'utm_medium=\', REFERRING_SITE)=0,NULL,substring(REFERRING_SITE, charindex(\'utm_medium=\', REFERRING_SITE) + len(\'utm_medium=\'), ifnull(nullif(charindex(\'&\', insert(REFERRING_SITE, 1, charindex(\'utm_medium=\', REFERRING_SITE)-1, \'\')),0), len(insert(REFERRING_SITE, 1, charindex(\'utm_medium=\', REFERRING_SITE)-1, \'\'))+1) - len(\'utm_medium=\')-1)) as REFERRING_UTM_MEDIUM, iff(charindex(\'utm_source=\', REFERRING_SITE)=0,NULL,substring(REFERRING_SITE, charindex(\'utm_source=\', REFERRING_SITE) + len(\'utm_source=\'), ifnull(nullif(charindex(\'&\', insert(REFERRING_SITE, 1, charindex(\'utm_source=\', REFERRING_SITE)-1, \'\')),0), len(insert(REFERRING_SITE, 1, charindex(\'utm_source=\', REFERRING_SITE)-1, \'\'))+1) - len(\'utm_source=\')-1)) as REFERRING_UTM_SOURCE, CASE WHEN LANDING_SITE LIKE \'%facebook.com%\' THEN \'Facebook\' WHEN LANDING_SITE LIKE \'%google.com%\' THEN \'Google\' WHEN LANDING_SITE LIKE \'%google.co%\' THEN \'Google\' WHEN LANDING_SITE LIKE \'%instagram.com%\' THEN \'Facebook\' WHEN LANDING_SITE LIKE \'%com.google%\' THEN \'Google\' ELSE NULL END AS LANDING_UTM_CHANNEL, CASE WHEN REFERRING_SITE LIKE \'%facebook.com%\' THEN \'Facebook\' WHEN REFERRING_SITE LIKE \'%google.com%\' THEN \'Google\' WHEN REFERRING_SITE LIKE \'%google.co%\' THEN \'Google\' WHEN REFERRING_SITE LIKE \'%instagram.com%\' THEN \'Instagram\' WHEN REFERRING_SITE LIKE \'%com.google%\' THEN \'Google\' ELSE NULL END AS REFERRING_UTM_CHANNEL FROM (select o.* ,\'Shopify_India\' AS Shop_Name, CASE when o.created_at::date < \'2022-06-16\' and s.amount = 0 then \'Prepaid\' when o.created_at::date < \'2022-06-16\' and s.amount <> 0 then \'COD\' end as payment_mode from Snitch_db.MAPLEMONK.SHOPIFYINDIA_ORDERS o LEFT JOIN (select distinct _AIRBYTE_SHOPIFYINDIA_ORDERS_HASHID,presentment_money:amount amount from Snitch_db.maplemonk.SHOPIFYINDIA_ORDERS_TOTAL_SHIPPING_PRICE_SET) s on s._AIRBYTE_SHOPIFYINDIA_ORDERS_HASHID = o._AIRBYTE_shopifyindia_ORDERS_HASHID )X; UPDATE Snitch_db.maplemonk.Shopify_All_orders AO SET AO.LANDING_UTM_CHANNEL = UTM.CHANNEL FROM Snitch_db.MAPLEMONK.UTM_MAPPING UTM WHERE AO.LANDING_UTM_CHANNEL IS NULL AND lower(AO.LANDING_UTM_MEDIUM) LIKE CONCAT(\'%\',lower(UTM.\"UTM Medium\"),\'%\') AND lower(AO.LANDING_UTM_SOURCE) LIKE CONCAT(\'%\',lower(UTM.\"UTM Source\"),\'%\'); UPDATE Snitch_db.maplemonk.Shopify_All_orders AO SET AO.REFERRING_UTM_CHANNEL = UTM.CHANNEL FROM Snitch_db.MAPLEMONK.UTM_MAPPING UTM WHERE AO.REFERRING_UTM_CHANNEL IS NULL AND lower(AO.REFERRING_UTM_MEDIUM) LIKE CONCAT(\'%\',lower(UTM.\"UTM Medium\"),\'%\') AND lower(AO.REFERRING_UTM_SOURCE) LIKE CONCAT(\'%\',lower(UTM.\"UTM Source\"),\'%\'); UPDATE Snitch_db.maplemonk.Shopify_All_orders SET LANDING_UTM_CHANNEL = \'Direct\' WHERE LANDING_UTM_CHANNEL IS NULL AND LANDING_UTM_MEDIUM IS NULL AND LANDING_UTM_SOURCE IS NULL AND REFERRING_UTM_MEDIUM IS NULL AND REFERRING_UTM_SOURCE IS NULL AND (LANDING_SITE LIKE \'%snitch.co.in%\'); UPDATE Snitch_db.maplemonk.Shopify_All_orders SET REFERRING_UTM_CHANNEL = \'Direct\' WHERE REFERRING_UTM_CHANNEL IS NULL AND LANDING_UTM_MEDIUM IS NULL AND LANDING_UTM_SOURCE IS NULL AND REFERRING_UTM_MEDIUM IS NULL AND REFERRING_UTM_SOURCE IS NULL AND (REFERRING_SITE LIKE \'%snitch.co.in%\'); ALTER TABLE Snitch_db.maplemonk.Shopify_All_orders ADD COLUMN FINAL_UTM_CHANNEL varchar(16777216); UPDATE Snitch_db.maplemonk.Shopify_All_orders SET FINAL_UTM_CHANNEL = COALESCE(LANDING_UTM_CHANNEL,REFERRING_UTM_CHANNEL,\'Others\') WHERE LANDING_UTM_CHANNEL IS NULL OR REFERRING_UTM_CHANNEL IS NULL; UPDATE Snitch_db.maplemonk.Shopify_All_orders SET FINAL_UTM_CHANNEL = LANDING_UTM_CHANNEL WHERE LANDING_UTM_CHANNEL IS NOT NULL AND REFERRING_UTM_CHANNEL IS NOT NULL AND lower(LANDING_UTM_CHANNEL) = lower(REFERRING_UTM_CHANNEL); UPDATE Snitch_db.maplemonk.Shopify_All_orders SET FINAL_UTM_CHANNEL = LANDING_UTM_CHANNEL WHERE LANDING_UTM_CHANNEL IS NOT NULL AND REFERRING_UTM_CHANNEL IS NOT NULL AND lower(LANDING_UTM_CHANNEL)<>lower(REFERRING_UTM_CHANNEL); UPDATE Snitch_db.maplemonk.Shopify_All_orders SET LANDING_UTM_CHANNEL = \'Others\' WHERE LANDING_UTM_CHANNEL IS NULL; UPDATE Snitch_db.maplemonk.Shopify_All_orders SET REFERRING_UTM_CHANNEL = \'Others\' WHERE REFERRING_UTM_CHANNEL IS NULL; ALTER TABLE Snitch_db.maplemonk.Shopify_All_orders RENAME COLUMN _AIRBYTE_SHOPIFYINDIA_ORDERS_HASHID to _AIRBYTE_ORDERS_HASHID; CREATE OR REPLACE TABLE Snitch_db.maplemonk.Shopify_All_products AS select *,\'Shopify_India\' AS Shop_Name from Snitch_db.maplemonk.shopifyindia_products ; ALTER TABLE Snitch_db.maplemonk.Shopify_All_products RENAME COLUMN _AIRBYTE_SHOPIFYINDIA_PRODUCTS_HASHID to _AIRBYTE_PRODUCTS_HASHID; CREATE OR REPLACE TABLE Snitch_db.maplemonk.Shopify_All_products_variants AS select *,\'Shopify_India\' AS Shop_Name from Snitch_db.maplemonk.shopifyindia_products_variants ; ALTER TABLE Snitch_db.maplemonk.Shopify_All_products_variants RENAME COLUMN _AIRBYTE_SHOPIFYINDIA_PRODUCTS_HASHID to _AIRBYTE_PRODUCTS_HASHID; CREATE OR REPLACE TABLE Snitch_db.maplemonk.Shopify_All_customers_addresses AS select *,\'Shopify_India\' AS Shop_Name from Snitch_db.maplemonk.shopifyindia_customers_addresses ; CREATE OR REPLACE TABLE Snitch_db.maplemonk.Shopify_All_orders_items_discount AS SELECT order_id, LINE_ITEM_ID, SUM(B.VALUE:amount::FLOAT) AS DISCOUNT FROM( SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:discount_allocations AS discount_allocations FROM Snitch_db.maplemonk.Shopify_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A)x,LATERAL FLATTEN (INPUT => x.discount_allocations)B GROUP BY ORDER_ID, LINE_ITEM_ID; CREATE OR REPLACE TABLE Snitch_db.maplemonk.Shopify_All_orders_items_tax AS SELECT order_id, LINE_ITEM_ID, SUM(B.VALUE:price::FLOAT) AS TAX FROM( SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:tax_lines AS tax_lines FROM Snitch_db.maplemonk.Shopify_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A)x,LATERAL FLATTEN (INPUT => x.tax_lines)B GROUP BY order_id, LINE_ITEM_ID; CREATE OR REPLACE TABLE Snitch_db.maplemonk.Shopify_All_Refunds AS SELECT Name, ID AS Order_ID, Shop_name, C.value:line_item_id AS line_item_id, SUM(C.VALUE:quantity) AS quantity, SUM(C.VALUE:subtotal) AS subtotal FROM Snitch_db.maplemonk.Shopify_All_orders, LATERAL FLATTEN(INPUT => refunds)A, LATERAL FLATTEN(INPUT=>A.value)B, LATERAL FLATTEN(INPUT => B.value) C WHERE C.value:line_item_id IS NOT null GROUP BY Name, ID, Shop_name, C.value:line_item_id; CREATE OR REPLACE TABLE Snitch_db.maplemonk.Shopify_All_orders_items AS WITH CTE AS (SELECT SHOP_NAME, ID::VARCHAR(16777216) AS ORDER_ID, NAME AS ORDER_NAME, CUSTOMER, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:sku::STRING AS SKU, A.VALUE:product_id::STRING AS PRODUCT_ID, A.VALUE:title::STRING AS PRODUCT_NAME, app_id, CURRENCY, tags, gateway, CASE WHEN cancelled_at IS NOT NULL THEN \'CANCELLED\' ELSE \'Shopify_Processed\' END AS order_status, CREATED_AT::DATETIME AS order_timestamp, A.VALUE:price::FLOAT * A.VALUE:quantity::FLOAT AS LINE_ITEM_SALES, (TOTAL_SHIPPING_PRICE_SET:presentment_money:amount::FLOAT/ COUNT(ORDER_ID) OVER(PARTITION BY ORDER_ID ORDER BY ORDER_ID)) AS SHIPPING_PRICE, A.VALUE:quantity::FLOAT as QUANTITY, \'Shopify\' AS Source, LANDING_UTM_MEDIUM, LANDING_UTM_SOURCE, LANDING_UTM_CAMPAIGN, REFERRING_UTM_MEDIUM, REFERRING_UTM_SOURCE, LANDING_UTM_CHANNEL, REFERRING_UTM_CHANNEL, FINAL_UTM_CHANNEL, payment_mode FROM Snitch_db.maplemonk.Shopify_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A) SELECT CTE.*, IFNULL(T.TAX,0) AS TAX, IFNULL(D.DISCOUNT,0) AS DISCOUNT, CTE.LINE_ITEM_SALES - IFNULL(D.DISCOUNT,0) + IFNULL(CTE.SHIPPING_PRICE,0) AS NET_SALES, CTE.LINE_ITEM_SALES - IFNULL(D.DISCOUNT,0) as Gross_Sales, CASE WHEN R.QUANTITY IS NOT NULL THEN 1 ELSE 0 END AS IS_REFUND FROM CTE LEFT JOIN Snitch_db.maplemonk.Shopify_All_orders_items_tax T ON CTE.ORDER_ID = T.ORDER_ID AND CTE.LINE_ITEM_ID = T.LINE_ITEM_ID LEFT JOIN Snitch_db.maplemonk.Shopify_All_orders_items_discount D ON CTE.ORDER_ID = D.ORDER_ID AND CTE.LINE_ITEM_ID = D.LINE_ITEM_ID LEFT JOIN Snitch_db.maplemonk.Shopify_All_Refunds R ON CTE.ORDER_ID = R.ORDER_ID AND CTE.LINE_ITEM_ID = R.LINE_ITEM_ID; CREATE OR REPLACE TABLE Snitch_db.maplemonk.FACT_ITEMS_SNITCH AS select a.*, case when cm.mapped_cuty is not null then cm.mapped_cuty else a.city end as city_mapped from ( SELECT O.SHOP_NAME, O.ORDER_ID, O.ORDER_NAME, O.CUSTOMER:id::int AS customer_id, O.LINE_ITEM_ID, o.app_id, gateway, o.tags, case when o.order_timestamp::date > \'2022-06-16\' and lower(o.gateway) = \'cashfree payments\' then \'Prepaid\' when o.order_timestamp::date > \'2022-06-16\' and lower(o.gateway) like \'%cash%\' and lower(o.gateway) <> \'cashfree payments%\' then \'COD\' when o.order_timestamp::date > \'2022-06-16\' and lower(o.gateway) not like \'%cash%\' and lower(o.gateway) <> \'\' then \'Prepaid\' when o.order_timestamp::date > \'2022-06-16\' and lower(o.gateway) not like \'%cash%\' and lower(o.gateway) = \'\' then \'Exchange\' else o.payment_mode end as payment_method, case when o.order_timestamp::date > \'2022-06-16\' and lower(o.gateway) like \'%cash%\' and lower(o.tags) like \'%gokwik%\' then \'GoKwik\' else gateway end as payment_gateway, O.SKU, pv.size, pv.colour, O.PRODUCT_ID, CASE WHEN O.PRODUCT_NAME IS NULL THEN \'NA\' ELSE O.PRODUCT_NAME END AS PRODUCT_NAME, replace(p.image:src,\'\"\"\',\'\') IMAGE_lINK, O.CURRENCY, O.IS_REFUND, CASE WHEN CD.city IS NULL OR CD.city = \'\' THEN \'NA\' ELSE Cd.city END AS city, CASE WHEN CD.province IS NULL OR CD.province = \'\' THEN \'NA\' ELSE CD.province END AS state, CASE WHEN P.product_type = \'\' THEN \'NA\' WHEN P.product_type = \'Jeans\' THEN \'Denim\' WHEN P.product_type = \'Pant\' then \'Pants\' ELSE P.product_type END AS category, CASE WHEN O.SKU LIKE \'%4MSQ0011%\' THEN \'LUXE\' WHEN O.SKU NOT LIKE \'%4MSQ0011%\' AND M.SKUS is NOT NULL THEN \'PLUS\' WHEN O.SKU NOT LIKE \'%4MSQ0011%\' AND N.SKUS is NOT NULL THEN \'INNERWEAR\' when P.product_type = \'Perfume & Cologne\' then \'PERFUMES\' ELSE \'OTHER\' END AS Segment, CASE WHEN O.APP_ID = \'2653365\' THEN \'Shopney\' ELSE \'Web\' end as WebShopney, O.order_status, O.order_timestamp, O.LINE_ITEM_SALES, O.SHIPPING_PRICE, O.QUANTITY, O.TAX, O.DISCOUNT, O.NET_SALES, o.Gross_sales, O.Source, O.LANDING_UTM_MEDIUM, O.LANDING_UTM_SOURCE, O.LANDING_UTM_CAMPAIGN, O.REFERRING_UTM_MEDIUM, O.REFERRING_UTM_SOURCE, O.LANDING_UTM_CHANNEL, O.REFERRING_UTM_CHANNEL, O.FINAL_UTM_CHANNEL, o.payment_mode FROM Snitch_db.maplemonk.Shopify_All_orders_items O LEFT JOIN Snitch_db.maplemonk.Shopify_All_products P ON O.PRODUCT_ID = P.id LEFT JOIN ( select sku, size, colour from ( select distinct sku, row_number() over (partition by sku order by title) row_num, case when option2 in (\'XL\',\'S\',\'3XL\',\'34\',\'44\',\'2XL\',\'5XL\',\'36\',\'L\',\'4XL\',\'46\',\'M\',\'XXL\',\'32\',\'30\',\'40\',\'28\',\'38\',\'42\') then option2 when option2 is null then option1 else option1 end as size, case when option2 in (\'XL\',\'S\',\'3XL\',\'34\',\'44\',\'2XL\',\'5XL\',\'36\',\'L\',\'4XL\',\'46\',\'M\',\'XXL\',\'32\',\'30\',\'40\',\'28\',\'38\',\'42\') then option1 when option2 is null then \'None\' else option2 end as colour from Snitch_db.maplemonk.SHOPIFY_ALL_PRODUCTS_VARIANTs ) where row_num=1) pv on pv.sku = o.sku left join snitch_db.maplemonk.snitch_plus_sku_mapping m on o.sku = m.skus left join snitch_db.maplemonk.snitch_innerwear_sku_mapping n on o.sku = n.skus LEFT JOIN(SELECT customer_id, city, province, row_number() OVER ( partition BY customer_id ORDER BY id DESC) rowid FROM Snitch_db.maplemonk.Shopify_All_customers_addresses) AS CD ON O.CUSTOMER:id::int = CD.customer_id AND CD.rowid = 1 ) a left join (select distinct city, mapped_cuty from snitch_db.maplemonk.snitch_city_mapping where mapped_cuty is not null) cm on a.city = cm.city ; ALTER TABLE Snitch_db.maplemonk.FACT_ITEMS_SNITCH ADD COLUMN customer_flag varchar(50); ALTER TABLE Snitch_db.maplemonk.FACT_ITEMS_SNITCH ADD COLUMN new_customer_flag varchar(50); ALTER TABLE Snitch_db.maplemonk.FACT_ITEMS_SNITCH ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE Snitch_db.maplemonk.FACT_ITEMS_SNITCH ADD COLUMN acquisition_product varchar(16777216); UPDATE Snitch_db.maplemonk.FACT_ITEMS_SNITCH AS A SET A.customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id, order_timestamp, CASE WHEN order_timestamp <> Min(order_timestamp) OVER ( partition BY customer_id) THEN \'Repeated\' ELSE \'New\' END AS Flag FROM Snitch_db.maplemonk.FACT_ITEMS_SNITCH)AS B WHERE A.order_id = B.order_id AND A.customer_id = B.customer_id; UPDATE Snitch_db.maplemonk.FACT_ITEMS_SNITCH SET customer_flag = CASE WHEN customer_flag IS NULL THEN \'New\' ELSE customer_flag END; UPDATE Snitch_db.maplemonk.FACT_ITEMS_SNITCH AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id, order_timestamp, CASE WHEN Last_day(order_timestamp, \'month\') <> Last_day(Min(order_timestamp) OVER ( partition BY customer_id)) THEN \'Repeated\' ELSE \'New\' END AS Flag FROM Snitch_db.maplemonk.FACT_ITEMS_SNITCH)AS B WHERE A.order_id = B.order_id AND A.customer_id = B.customer_id; UPDATE Snitch_db.maplemonk.FACT_ITEMS_SNITCH SET new_customer_flag = CASE WHEN new_customer_flag IS NULL THEN \'New\' ELSE new_customer_flag END; CREATE OR replace temporary TABLE Snitch_db.maplemonk.temp_source AS SELECT DISTINCT customer_id, source FROM ( SELECT DISTINCT customer_id, order_timestamp, source, Min(order_timestamp) OVER ( partition BY customer_id) firstOrderdate FROM Snitch_db.maplemonk.FACT_ITEMS_SNITCH)res WHERE order_timestamp=firstorderdate; UPDATE Snitch_db.maplemonk.FACT_ITEMS_SNITCH AS a SET a.acquisition_channel=b.source FROM Snitch_db.maplemonk.temp_source b WHERE a.customer_id = b.customer_id; CREATE OR replace temporary TABLE Snitch_db.maplemonk.temp_product AS SELECT DISTINCT customer_id, product_name, Row_number() OVER (partition BY customer_id ORDER BY LINE_ITEM_SALES DESC) rowid FROM ( SELECT DISTINCT customer_id, order_timestamp, product_name, LINE_ITEM_SALES , Min(order_timestamp) OVER (partition BY customer_id) firstOrderdate FROM Snitch_db.maplemonk.FACT_ITEMS_SNITCH )res WHERE order_timestamp=firstorderdate; UPDATE Snitch_db.maplemonk.FACT_ITEMS_SNITCH AS A SET A.acquisition_product=B.product_name FROM ( SELECT * FROM Snitch_db.maplemonk.temp_product WHERE rowid=1)B WHERE A.customer_id = B.customer_id;",
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
                        