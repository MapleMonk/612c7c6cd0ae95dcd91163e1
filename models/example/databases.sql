{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE TABLE IF NOT EXISTS MM_TEST.NaveenSept7.UTM_MAPPING ( UTM_SOURCE VARCHAR(16777216), UTM_MEDIUM VARCHAR(16777216), CHANNEL VARCHAR(16777216)); CREATE OR REPLACE TABLE MM_TEST.NaveenSept7.Shopify_All_customers AS select *,\'Shopify_lil_goodness\' AS Shop_Name from MM_TEST.NaveenSept7.Shopify_lil_goodness_CUSTOMERS ; CREATE OR REPLACE TABLE MM_TEST.NaveenSept7.Shopify_All_orders AS SELECT *, iff(charindex(\'utm_medium=\', landing_site)=0,NULL,substring(landing_site, charindex(\'utm_medium=\', landing_site) + len(\'utm_medium=\'), ifnull(nullif(charindex(\'&\', insert(landing_site, 1, charindex(\'utm_medium=\', landing_site)-1, \'\')),0), len(insert(landing_site, 1, charindex(\'utm_medium=\', landing_site)-1, \'\'))+1) - len(\'utm_medium=\')-1)) as LANDING_UTM_MEDIUM, iff(charindex(\'utm_source=\', landing_site)=0,NULL,substring(landing_site, charindex(\'utm_source=\', landing_site) + len(\'utm_source=\'), ifnull(nullif(charindex(\'&\', insert(landing_site, 1, charindex(\'utm_source=\', landing_site)-1, \'\')),0), len(insert(landing_site, 1, charindex(\'utm_source=\', landing_site)-1, \'\'))+1) - len(\'utm_source=\')-1)) as LANDING_UTM_SOURCE, iff(charindex(\'utm_campaign=\', landing_site)=0,NULL,substring(landing_site, charindex(\'utm_campaign=\', landing_site) + len(\'utm_campaign=\'), ifnull(nullif(charindex(\'&\', insert(landing_site, 1, charindex(\'utm_campaign=\', landing_site)-1, \'\')),0), len(insert(landing_site, 1, charindex(\'utm_campaign=\', landing_site)-1, \'\'))+1) - len(\'utm_campaign=\')-1)) as LANDING_UTM_CAMPAIGN, iff(charindex(\'utm_medium=\', REFERRING_SITE)=0,NULL,substring(REFERRING_SITE, charindex(\'utm_medium=\', REFERRING_SITE) + len(\'utm_medium=\'), ifnull(nullif(charindex(\'&\', insert(REFERRING_SITE, 1, charindex(\'utm_medium=\', REFERRING_SITE)-1, \'\')),0), len(insert(REFERRING_SITE, 1, charindex(\'utm_medium=\', REFERRING_SITE)-1, \'\'))+1) - len(\'utm_medium=\')-1)) as REFERRING_UTM_MEDIUM, iff(charindex(\'utm_source=\', REFERRING_SITE)=0,NULL,substring(REFERRING_SITE, charindex(\'utm_source=\', REFERRING_SITE) + len(\'utm_source=\'), ifnull(nullif(charindex(\'&\', insert(REFERRING_SITE, 1, charindex(\'utm_source=\', REFERRING_SITE)-1, \'\')),0), len(insert(REFERRING_SITE, 1, charindex(\'utm_source=\', REFERRING_SITE)-1, \'\'))+1) - len(\'utm_source=\')-1)) as REFERRING_UTM_SOURCE, CASE WHEN LANDING_SITE LIKE \'%facebook.com%\' THEN \'Facebook\' WHEN LANDING_SITE LIKE \'%google.com%\' THEN \'Google\' WHEN LANDING_SITE LIKE \'%google.co%\' THEN \'Google\' WHEN LANDING_SITE LIKE \'%instagram.com%\' THEN \'Facebook\' WHEN LANDING_SITE LIKE \'%com.google%\' THEN \'Google\' ELSE NULL END AS LANDING_UTM_CHANNEL, CASE WHEN REFERRING_SITE LIKE \'%facebook.com%\' THEN \'Facebook\' WHEN REFERRING_SITE LIKE \'%google.com%\' THEN \'Google\' WHEN REFERRING_SITE LIKE \'%google.co%\' THEN \'Google\' WHEN REFERRING_SITE LIKE \'%instagram.com%\' THEN \'Instagram\' WHEN REFERRING_SITE LIKE \'%com.google%\' THEN \'Google\' ELSE NULL END AS REFERRING_UTM_CHANNEL FROM ( select *,\'Shopify_lil_goodness\' AS Shop_Name from MM_TEST.NaveenSept7.Shopify_lil_goodness_ORDERS )X; UPDATE MM_TEST.NaveenSept7.Shopify_All_orders AO SET AO.LANDING_UTM_CHANNEL = UTM.CHANNEL FROM MM_TEST.NaveenSept7.UTM_MAPPING UTM WHERE AO.LANDING_UTM_CHANNEL IS NULL AND lower(AO.LANDING_UTM_SOURCE) LIKE CONCAT(\'%\',lower(UTM.UTM_SOURCE),\'%\'); UPDATE MM_TEST.NaveenSept7.Shopify_All_orders AO SET AO.REFERRING_UTM_CHANNEL = UTM.CHANNEL FROM MM_TEST.NaveenSept7.UTM_MAPPING UTM WHERE AO.REFERRING_UTM_CHANNEL IS NULL AND lower(AO.REFERRING_UTM_SOURCE) LIKE CONCAT(\'%\',lower(UTM.UTM_SOURCE),\'%\'); UPDATE MM_TEST.NaveenSept7.Shopify_All_orders SET LANDING_UTM_CHANNEL = \'Direct\' WHERE LANDING_UTM_CHANNEL IS NULL AND LANDING_UTM_SOURCE IS NULL AND REFERRING_UTM_SOURCE IS NULL AND (LANDING_SITE LIKE \'%MM_TEST.com%\' or LANDING_SITE LIKE \'%MM_TEST.com%\'); UPDATE MM_TEST.NaveenSept7.Shopify_All_orders SET REFERRING_UTM_CHANNEL = \'Direct\' WHERE REFERRING_UTM_CHANNEL IS NULL AND LANDING_UTM_SOURCE IS NULL AND REFERRING_UTM_SOURCE IS NULL AND LANDING_SITE LIKE \'%MM_TEST.com%\' or LANDING_SITE LIKE \'%MM_TEST.com%\'; ALTER TABLE MM_TEST.NaveenSept7.Shopify_All_orders ADD COLUMN FINAL_UTM_CHANNEL varchar(16777216); UPDATE MM_TEST.NaveenSept7.Shopify_All_orders SET FINAL_UTM_CHANNEL = COALESCE(LANDING_UTM_CHANNEL,REFERRING_UTM_CHANNEL,\'Others\') WHERE LANDING_UTM_CHANNEL IS NULL OR REFERRING_UTM_CHANNEL IS NULL; UPDATE MM_TEST.NaveenSept7.Shopify_All_orders SET FINAL_UTM_CHANNEL = LANDING_UTM_CHANNEL WHERE LANDING_UTM_CHANNEL IS NOT NULL AND REFERRING_UTM_CHANNEL IS NOT NULL AND lower(LANDING_UTM_CHANNEL) = lower(REFERRING_UTM_CHANNEL); UPDATE MM_TEST.NaveenSept7.Shopify_All_orders SET FINAL_UTM_CHANNEL = LANDING_UTM_CHANNEL WHERE LANDING_UTM_CHANNEL IS NOT NULL AND REFERRING_UTM_CHANNEL IS NOT NULL AND lower(LANDING_UTM_CHANNEL)<>lower(REFERRING_UTM_CHANNEL); UPDATE MM_TEST.NaveenSept7.Shopify_All_orders SET LANDING_UTM_CHANNEL = \'Others\' WHERE LANDING_UTM_CHANNEL IS NULL; UPDATE MM_TEST.NaveenSept7.Shopify_All_orders SET REFERRING_UTM_CHANNEL = \'Others\' WHERE REFERRING_UTM_CHANNEL IS NULL; ALTER TABLE MM_TEST.NaveenSept7.Shopify_All_orders RENAME COLUMN _AIRBYTE_Shopify_lil_goodness_ORDERS_HASHID to _AIRBYTE_ORDERS_HASHID; CREATE OR REPLACE TABLE MM_TEST.NaveenSept7.Shopify_All_products AS select *,\'Shopify_lil_goodness\' AS Shop_Name from MM_TEST.NaveenSept7.Shopify_lil_goodness_PRODUCTS ; ALTER TABLE MM_TEST.NaveenSept7.Shopify_All_products RENAME COLUMN _AIRBYTE_Shopify_lil_goodness_PRODUCTS_HASHID to _AIRBYTE_PRODUCTS_HASHID; CREATE OR REPLACE TABLE MM_TEST.NaveenSept7.Shopify_All_products_variants AS select *,\'Shopify_lil_goodness\' AS Shop_Name from MM_TEST.NaveenSept7.Shopify_lil_goodness_PRODUCTS_VARIANTS ; ALTER TABLE MM_TEST.NaveenSept7.SHOPIFY_ALL_PRODUCTS_VARIANTS RENAME COLUMN _AIRBYTE_Shopify_lil_goodness_PRODUCTS_HASHID to _AIRBYTE_PRODUCTS_HASHID; CREATE OR REPLACE TABLE MM_TEST.NaveenSept7.Shopify_All_customers_addresses AS select *,\'Shopify_lil_goodness\' AS Shop_Name from MM_TEST.NaveenSept7.Shopify_lil_goodness_CUSTOMERS_ADDRESSES ; CREATE OR REPLACE TABLE MM_TEST.NaveenSept7.Shopify_All_orders_items_discount AS SELECT order_id, LINE_ITEM_ID, SUM(B.VALUE:amount::FLOAT) AS DISCOUNT FROM( SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:discount_allocations AS discount_allocations FROM MM_TEST.NaveenSept7.Shopify_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A)x,LATERAL FLATTEN (INPUT => x.discount_allocations)B GROUP BY ORDER_ID, LINE_ITEM_ID; CREATE OR REPLACE TABLE MM_TEST.NaveenSept7.Shopify_All_orders_items_tax AS SELECT order_id, LINE_ITEM_ID, SUM(B.VALUE:price::FLOAT) AS TAX, sum(B.VALUE:rate::float) as Tax_Rate FROM( SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:tax_lines AS tax_lines FROM MM_TEST.NaveenSept7.Shopify_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A)x,LATERAL FLATTEN (INPUT => x.tax_lines)B GROUP BY order_id, LINE_ITEM_ID; CREATE OR REPLACE TABLE MM_TEST.NaveenSept7.Shopify_All_Refunds AS SELECT Name, ID AS Order_ID, Shop_name, C.value:line_item_id AS line_item_id, SUM(C.VALUE:quantity) AS quantity, SUM(C.VALUE:subtotal) AS subtotal FROM MM_TEST.NaveenSept7.Shopify_All_orders, LATERAL FLATTEN(INPUT => refunds)A, LATERAL FLATTEN(INPUT=>A.value)B, LATERAL FLATTEN(INPUT => B.value) C WHERE C.value:line_item_id IS NOT null GROUP BY Name, ID, Shop_name, C.value:line_item_id; CREATE OR REPLACE TABLE MM_TEST.NaveenSept7.Shopify_All_orders_items AS WITH CTE AS (SELECT SHOP_NAME, ID::VARCHAR(16777216) AS ORDER_ID, NAME AS ORDER_NAME, CUSTOMER, replace(customer:default_address:name,\'\"\',\'\') NAME, PHONE, EMAIL, tags, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:sku::STRING AS SKU, A.VALUE:product_id::STRING AS PRODUCT_ID, A.VALUE:title::STRING AS PRODUCT_NAME, CURRENCY, CASE WHEN cancelled_at IS NOT NULL THEN \'CANCELLED\' ELSE \'Shopify_Processed\' END AS order_status, CREATED_AT::DATETIME AS order_timestamp, A.VALUE:price::FLOAT * A.VALUE:quantity::FLOAT AS LINE_ITEM_SALES, (TOTAL_SHIPPING_PRICE_SET:presentment_money:amount::FLOAT/ COUNT(ORDER_ID) OVER(PARTITION BY ORDER_ID ORDER BY ORDER_ID)) AS SHIPPING_PRICE, A.value:price/(1+A.value:tax_lines:rate), A.VALUE:quantity::FLOAT as QUANTITY, \'Shopify\' AS Source, LANDING_UTM_MEDIUM, LANDING_UTM_SOURCE, LANDING_UTM_CAMPAIGN, REFERRING_UTM_MEDIUM, REFERRING_UTM_SOURCE, LANDING_UTM_CHANNEL, REFERRING_UTM_CHANNEL, FINAL_UTM_CHANNEL, NULL as product_sub_category FROM MM_TEST.NaveenSept7.Shopify_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A) SELECT CTE.*, IFNULL(T.TAX_RATE,0) AS TAX_RATE, IFNULL(D.DISCOUNT,0) AS DISCOUNT, CTE.LINE_ITEM_SALES AS GROSS_SALES_AFTER_TAX, CTE.LINE_ITEM_SALES/(1+IFNULL(T.TAX_RATE,0)) AS GROSS_SALES_BEFORE_TAX, IFNULL(D.DISCOUNT,0)/(1+IFNULL(T.TAX_RATE,0)) AS DISCOUNT_BEFORE_TAX, CTE.LINE_ITEM_SALES/(1+IFNULL(T.TAX_RATE,0)) - IFNULL(D.DISCOUNT,0)/(1+IFNULL(T.TAX_RATE,0)) AS NET_SALES_BEFORE_TAX, IFNULL(T.TAX,0) AS TAX, (CTE.LINE_ITEM_SALES/(1+IFNULL(T.TAX_RATE,0))) - (IFNULL(D.DISCOUNT,0)/(1+IFNULL(T.TAX_RATE,0))) + IFNULL(T.TAX,0) + CTE.SHIPPING_PRICE AS TOTAL_SALES, CASE WHEN R.QUANTITY IS NOT NULL THEN 1 ELSE 0 END AS IS_REFUND FROM CTE LEFT JOIN MM_TEST.NaveenSept7.Shopify_All_orders_items_tax T ON CTE.ORDER_ID = T.ORDER_ID AND CTE.LINE_ITEM_ID = T.LINE_ITEM_ID LEFT JOIN MM_TEST.NaveenSept7.Shopify_All_orders_items_discount D ON CTE.ORDER_ID = D.ORDER_ID AND CTE.LINE_ITEM_ID = D.LINE_ITEM_ID LEFT JOIN MM_TEST.NaveenSept7.Shopify_All_Refunds R ON CTE.ORDER_ID = R.ORDER_ID AND CTE.LINE_ITEM_ID = R.LINE_ITEM_ID ; CREATE OR REPLACE TABLE MM_TEST.NaveenSept7.MM_TEST_SHOPIFY_FACT_ITEMS AS SELECT O.SHOP_NAME, O.ORDER_ID, O.ORDER_NAME, O.CUSTOMER:id::int AS customer_id, O.Name, O.EMAIL, O.PHONE, O.Tags, O.LINE_ITEM_ID, O.SKU, O.PRODUCT_ID, O.CURRENCY, O.IS_REFUND, CASE WHEN CD.city IS NULL OR CD.city = \'\' THEN \'NA\' ELSE upper(CD.city) END AS city, CASE WHEN CD.province IS NULL OR CD.province = \'\' THEN \'NA\' ELSE upper(CD.province) END AS state, CASE WHEN P.title = \'\' THEN \'NA\' ELSE P.title END AS product_name, CASE WHEN P.product_type = \'\' THEN \'NA\' ELSE P.product_type END AS category, O.order_status, O.order_timestamp, O.LINE_ITEM_SALES, O.SHIPPING_PRICE, O.QUANTITY, O.TAX, O.TAX_RATE, O.DISCOUNT, O.DISCOUNT_BEFORE_TAX, O.GROSS_SALES_AFTER_TAX, O.GROSS_SALES_BEFORE_TAX, O.NET_SALES_BEFORE_TAX, O.TOTAL_SALES, O.Source, O.LANDING_UTM_MEDIUM, O.LANDING_UTM_SOURCE, O.LANDING_UTM_CAMPAIGN, O.REFERRING_UTM_MEDIUM, O.REFERRING_UTM_SOURCE, O.LANDING_UTM_CHANNEL, O.REFERRING_UTM_CHANNEL, O.FINAL_UTM_CHANNEL, o.product_sub_category FROM MM_TEST.NaveenSept7.Shopify_All_orders_items O LEFT JOIN MM_TEST.NaveenSept7.Shopify_All_products P ON O.PRODUCT_ID = P.id LEFT JOIN(SELECT customer_id, city, province, row_number() OVER ( partition BY customer_id ORDER BY id DESC) rowid FROM MM_TEST.NaveenSept7.Shopify_All_customers_addresses) AS CD ON O.CUSTOMER:id::int = CD.customer_id AND CD.rowid = 1; ALTER TABLE MM_TEST.NaveenSept7.MM_TEST_SHOPIFY_FACT_ITEMS ADD COLUMN new_customer_flag varchar(50); ALTER TABLE MM_TEST.NaveenSept7.MM_TEST_SHOPIFY_FACT_ITEMS ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE MM_TEST.NaveenSept7.MM_TEST_SHOPIFY_FACT_ITEMS ADD COLUMN acquisition_product varchar(16777216); UPDATE MM_TEST.NaveenSept7.MM_TEST_SHOPIFY_FACT_ITEMS AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id, order_timestamp, CASE WHEN order_timestamp <> Min(order_timestamp) OVER ( partition BY customer_id) THEN \'Repeat\' ELSE \'New\' END AS Flag FROM MM_TEST.NaveenSept7.MM_TEST_SHOPIFY_FACT_ITEMS)AS B WHERE A.order_id = B.order_id AND A.customer_id = B.customer_id; UPDATE MM_TEST.NaveenSept7.MM_TEST_SHOPIFY_FACT_ITEMS SET new_customer_flag = CASE WHEN new_customer_flag IS NULL THEN \'New\' ELSE new_customer_flag END; CREATE OR replace temporary TABLE MM_TEST.NaveenSept7.temp_source AS SELECT DISTINCT customer_id, source FROM ( SELECT DISTINCT customer_id, order_timestamp, source, Min(order_timestamp) OVER ( partition BY customer_id) firstOrderdate FROM MM_TEST.NaveenSept7.MM_TEST_SHOPIFY_FACT_ITEMS)res WHERE order_timestamp=firstorderdate; UPDATE MM_TEST.NaveenSept7.MM_TEST_SHOPIFY_FACT_ITEMS AS a SET a.acquisition_channel=b.source FROM MM_TEST.NaveenSept7.temp_source b WHERE a.customer_id = b.customer_id; ALTER TABLE MM_TEST.NaveenSept7.MM_TEST_SHOPIFY_FACT_ITEMS ADD COLUMN SHIPPING_TAX FLOAT; ALTER TABLE MM_TEST.NaveenSept7.MM_TEST_SHOPIFY_FACT_ITEMS ADD COLUMN SHIP_PROMOTION_DISCOUNT FLOAT; ALTER TABLE MM_TEST.NaveenSept7.MM_TEST_SHOPIFY_FACT_ITEMS ADD COLUMN GIFT_WRAP_PRICE FLOAT; ALTER TABLE MM_TEST.NaveenSept7.MM_TEST_SHOPIFY_FACT_ITEMS ADD COLUMN GIFT_WRAP_TAX FLOAT; ALTER TABLE MM_TEST.NaveenSept7.MM_TEST_SHOPIFY_FACT_ITEMS MODIFY COLUMN ORDER_STATUS VARCHAR(100); CREATE OR REPLACE TABLE MM_TEST.NaveenSept7.MM_TEST_SHOPIFY_FACT_ITEMS_TEMP_Category as select fi.* ,fi.SKU AS SKU_CODE ,p.name as PRODUCT_NAME_Final ,coalesce(Upper(p.CATEGORY),upper(fi.category)) AS Product_Category ,Upper(p.sub_category) as Product_Super_Category from MM_TEST.NaveenSept7.MM_TEST_SHOPIFY_FACT_ITEMS fi left join (select distinct skucode, name, category, sub_category from MM_TEST.NaveenSept7.sku_master) p on fi.sku = p.skucode; CREATE OR REPLACE TABLE MM_TEST.NaveenSept7.MM_TEST_SHOPIFY_FACT_ITEMS AS SELECT * FROM MM_TEST.NaveenSept7.MM_TEST_SHOPIFY_FACT_ITEMS_TEMP_Category; CREATE OR replace temporary TABLE MM_TEST.NaveenSept7.temp_product AS SELECT DISTINCT customer_id, product_name, Row_number() OVER (partition BY customer_id ORDER BY LINE_ITEM_SALES DESC) rowid FROM ( SELECT DISTINCT customer_id, order_timestamp, product_name, LINE_ITEM_SALES , Min(order_timestamp) OVER (partition BY customer_id) firstOrderdate FROM MM_TEST.NaveenSept7.MM_TEST_SHOPIFY_FACT_ITEMS )res WHERE order_timestamp=firstorderdate; UPDATE MM_TEST.NaveenSept7.MM_TEST_SHOPIFY_FACT_ITEMS AS A SET A.acquisition_product=B.product_name FROM ( SELECT * FROM MM_TEST.NaveenSept7.temp_product WHERE rowid=1)B WHERE A.customer_id = B.customer_id;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from MM_TEST.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        