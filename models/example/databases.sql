{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE RPSG_DB.MAPLEMONK.Shopify_AyurvedicSource_All_customers AS select *,\'Shopify_AyurvedicSource\' AS Shop_Name from RPSG_DB.MAPLEMONK.SHOPIFY_AyurvedicSource_CUSTOMERS; CREATE OR REPLACE TABLE RPSG_DB.MAPLEMONK.Shopify_AyurvedicSource_All_orders AS SELECT *, iff(charindex(\'utm_medium=\', landing_site)=0,NULL,substring(landing_site, charindex(\'utm_medium=\', landing_site) + len(\'utm_medium=\'), ifnull(nullif(charindex(\'&\', insert(landing_site, 1, charindex(\'utm_medium=\', landing_site)-1, \'\')),0), len(insert(landing_site, 1, charindex(\'utm_medium=\', landing_site)-1, \'\'))+1) - len(\'utm_medium=\')-1)) as LANDING_UTM_MEDIUM, iff(charindex(\'utm_source=\', landing_site)=0,NULL,substring(landing_site, charindex(\'utm_source=\', landing_site) + len(\'utm_source=\'), ifnull(nullif(charindex(\'&\', insert(landing_site, 1, charindex(\'utm_source=\', landing_site)-1, \'\')),0), len(insert(landing_site, 1, charindex(\'utm_source=\', landing_site)-1, \'\'))+1) - len(\'utm_source=\')-1)) as LANDING_UTM_SOURCE, iff(charindex(\'utm_campaign=\', landing_site)=0,NULL,substring(landing_site, charindex(\'utm_campaign=\', landing_site) + len(\'utm_campaign=\'), ifnull(nullif(charindex(\'&\', insert(landing_site, 1, charindex(\'utm_campaign=\', landing_site)-1, \'\')),0), len(insert(landing_site, 1, charindex(\'utm_campaign=\', landing_site)-1, \'\'))+1) - len(\'utm_campaign=\')-1)) as LANDING_UTM_CAMPAIGN, iff(charindex(\'utm_medium=\', REFERRING_SITE)=0,NULL,substring(REFERRING_SITE, charindex(\'utm_medium=\', REFERRING_SITE) + len(\'utm_medium=\'), ifnull(nullif(charindex(\'&\', insert(REFERRING_SITE, 1, charindex(\'utm_medium=\', REFERRING_SITE)-1, \'\')),0), len(insert(REFERRING_SITE, 1, charindex(\'utm_medium=\', REFERRING_SITE)-1, \'\'))+1) - len(\'utm_medium=\')-1)) as REFERRING_UTM_MEDIUM, iff(charindex(\'utm_source=\', REFERRING_SITE)=0,NULL,substring(REFERRING_SITE, charindex(\'utm_source=\', REFERRING_SITE) + len(\'utm_source=\'), ifnull(nullif(charindex(\'&\', insert(REFERRING_SITE, 1, charindex(\'utm_source=\', REFERRING_SITE)-1, \'\')),0), len(insert(REFERRING_SITE, 1, charindex(\'utm_source=\', REFERRING_SITE)-1, \'\'))+1) - len(\'utm_source=\')-1)) as REFERRING_UTM_SOURCE, CASE WHEN LANDING_SITE LIKE \'%facebook.com%\' THEN \'Facebook\' WHEN LANDING_SITE LIKE \'%google.com%\' THEN \'Google\' WHEN LANDING_SITE LIKE \'%google.co%\' THEN \'Google\' WHEN LANDING_SITE LIKE \'%instagram.com%\' THEN \'Facebook\' WHEN LANDING_SITE LIKE \'%com.google%\' THEN \'Google\' ELSE NULL END AS LANDING_UTM_CHANNEL, CASE WHEN REFERRING_SITE LIKE \'%facebook.com%\' THEN \'Facebook\' WHEN REFERRING_SITE LIKE \'%google.com%\' THEN \'Google\' WHEN REFERRING_SITE LIKE \'%google.co%\' THEN \'Google\' WHEN REFERRING_SITE LIKE \'%instagram.com%\' THEN \'Facebook\' WHEN REFERRING_SITE LIKE \'%com.google%\' THEN \'Google\' ELSE NULL END AS REFERRING_UTM_CHANNEL FROM (select *,case when lower(tags) like \'%cred%\' then \'CRED\' else \'Shopify_AyurvedicSource\' end AS Shop_Name from RPSG_DB.MAPLEMONK.Shopify_AyurvedicSource_ORDERS)X; UPDATE RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_orders AO SET AO.LANDING_UTM_CHANNEL = UTM.CHANNEL FROM RPSG_DB.MAPLEMONK.UTM_MAPPING UTM WHERE AO.LANDING_UTM_CHANNEL IS NULL AND lower(AO.LANDING_UTM_SOURCE) LIKE CONCAT(\'%\',lower(UTM.SOURCE),\'%\') AND lower(AO.LANDING_UTM_MEDIUM) LIKE CONCAT(\'%\',lower(UTM.MEDIUM),\'%\') ; UPDATE RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_orders AO SET AO.REFERRING_UTM_CHANNEL = UTM.CHANNEL FROM RPSG_DB.MAPLEMONK.UTM_MAPPING UTM WHERE AO.REFERRING_UTM_CHANNEL IS NULL AND lower(AO.REFERRING_UTM_SOURCE) LIKE CONCAT(\'%\',lower(UTM.SOURCE),\'%\') AND lower(AO.REFERRING_UTM_MEDIUM) LIKE CONCAT(\'%\',lower(UTM.MEDIUM),\'%\') ; UPDATE RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_orders SET LANDING_UTM_CHANNEL = \'Direct\' WHERE LANDING_UTM_CHANNEL IS NULL AND LANDING_UTM_SOURCE IS NULL AND LANDING_UTM_MEDIUM IS NULL AND REFERRING_UTM_SOURCE IS NULL AND REFERRING_UTM_MEDIUM IS NULL AND (LANDING_SITE LIKE \'%ayurvedic-source.com%\'); UPDATE RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_orders SET REFERRING_UTM_CHANNEL = \'Direct\' WHERE REFERRING_UTM_CHANNEL IS NULL AND LANDING_UTM_SOURCE IS NULL AND LANDING_UTM_MEDIUM IS NULL AND REFERRING_UTM_SOURCE IS NULL AND REFERRING_UTM_MEDIUM IS NULL AND (REFERRING_SITE LIKE \'%%ayurvedic-source.com%\'); ALTER TABLE RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_orders ADD COLUMN FINAL_UTM_CHANNEL varchar(16777216); UPDATE RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_orders SET FINAL_UTM_CHANNEL = COALESCE(LANDING_UTM_CHANNEL,REFERRING_UTM_CHANNEL,\'Others\') WHERE LANDING_UTM_CHANNEL IS NULL OR REFERRING_UTM_CHANNEL IS NULL; UPDATE RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_orders SET FINAL_UTM_CHANNEL = LANDING_UTM_CHANNEL WHERE LANDING_UTM_CHANNEL IS NOT NULL AND REFERRING_UTM_CHANNEL IS NOT NULL AND lower(LANDING_UTM_CHANNEL) = lower(REFERRING_UTM_CHANNEL); UPDATE RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_orders SET FINAL_UTM_CHANNEL = LANDING_UTM_CHANNEL WHERE LANDING_UTM_CHANNEL IS NOT NULL AND REFERRING_UTM_CHANNEL IS NOT NULL AND lower(LANDING_UTM_CHANNEL)<>lower(REFERRING_UTM_CHANNEL); UPDATE RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_orders SET LANDING_UTM_CHANNEL = \'Others\' WHERE LANDING_UTM_CHANNEL IS NULL; UPDATE RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_orders SET REFERRING_UTM_CHANNEL = \'Others\' WHERE REFERRING_UTM_CHANNEL IS NULL; ALTER TABLE RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_orders RENAME COLUMN _AIRBYTE_Shopify_AyurvedicSource_ORDERS_HASHID to _AIRBYTE_ORDERS_HASHID; CREATE OR REPLACE TABLE RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_products AS select *,\'Shopify_AyurvedicSource\' AS Shop_Name from RPSG_DB.MAPLEMONK.Shopify_AyurvedicSource_PRODUCTS; ALTER TABLE RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_products RENAME COLUMN _AIRBYTE_Shopify_AyurvedicSource_PRODUCTS_HASHID to _AIRBYTE_PRODUCTS_HASHID; CREATE OR REPLACE TABLE RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_products_variants AS select *,\'Shopify_AyurvedicSource\' AS Shop_Name from RPSG_DB.MAPLEMONK.Shopify_AyurvedicSource_PRODUCTS_VARIANTS; ALTER TABLE RPSG_DB.MAPLEMONK.Shopify_AyurvedicSource_ALL_PRODUCTS_VARIANTS RENAME COLUMN _AIRBYTE_Shopify_AyurvedicSource_PRODUCTS_HASHID to _AIRBYTE_PRODUCTS_HASHID; CREATE OR REPLACE TABLE RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_customers_addresses AS select *,\'Shopify_AyurvedicSource\' AS Shop_Name from RPSG_DB.MAPLEMONK.Shopify_AyurvedicSource_CUSTOMERS_ADDRESSES; CREATE OR REPLACE TABLE RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_orders_items_discount AS SELECT order_id, LINE_ITEM_ID, SUM(B.VALUE:amount::FLOAT) AS DISCOUNT FROM( SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:discount_allocations AS discount_allocations FROM RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A)x,LATERAL FLATTEN (INPUT => x.discount_allocations)B GROUP BY ORDER_ID, LINE_ITEM_ID; CREATE OR REPLACE TABLE RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_orders_items_tax AS SELECT order_id, LINE_ITEM_ID, SUM(B.VALUE:price::FLOAT) AS TAX, sum(B.VALUE:rate::float) as Tax_Rate FROM( SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:tax_lines AS tax_lines FROM RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A)x,LATERAL FLATTEN (INPUT => x.tax_lines)B GROUP BY order_id, LINE_ITEM_ID; CREATE OR REPLACE TABLE RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_Refunds AS SELECT Name, ID AS Order_ID, Shop_name, C.value:line_item_id AS line_item_id, SUM(C.VALUE:quantity) AS quantity, SUM(C.VALUE:subtotal) AS subtotal FROM RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_orders, LATERAL FLATTEN(INPUT => refunds)A, LATERAL FLATTEN(INPUT=>A.value)B, LATERAL FLATTEN(INPUT => B.value) C WHERE C.value:line_item_id IS NOT null GROUP BY Name, ID, Shop_name, C.value:line_item_id; CREATE OR REPLACE TABLE RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_orders_items AS WITH CTE AS (SELECT SHOP_NAME, ID::VARCHAR(16777216) AS ORDER_ID, NAME AS ORDER_NAME, CUSTOMER, replace(customer:default_address:name,\'\"\',\'\') NAME, PHONE, EMAIL, tags, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:sku::STRING AS SKU, A.VALUE:product_id::STRING AS PRODUCT_ID, A.VALUE:title::STRING AS PRODUCT_NAME, CURRENCY, CASE WHEN cancelled_at IS NOT NULL THEN \'CANCELLED\' ELSE \'Shopify_Processed\' END AS order_status, CREATED_AT::DATETIME AS order_timestamp, A.VALUE:price::FLOAT * A.VALUE:quantity::FLOAT AS LINE_ITEM_SALES, (TOTAL_SHIPPING_PRICE_SET:presentment_money:amount::FLOAT/ COUNT(ORDER_ID) OVER(PARTITION BY ORDER_ID ORDER BY ORDER_ID)) AS SHIPPING_PRICE, A.value:price/(1+A.value:tax_lines:rate), A.VALUE:quantity::FLOAT as QUANTITY, \'Shopify\' AS Source, LANDING_UTM_MEDIUM, LANDING_UTM_SOURCE, LANDING_UTM_CAMPAIGN, REFERRING_UTM_MEDIUM, REFERRING_UTM_SOURCE, LANDING_UTM_CHANNEL, REFERRING_UTM_CHANNEL, FINAL_UTM_CHANNEL, SHIPPING_ADDRESS FROM RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A) SELECT CTE.*, IFNULL(T.TAX_RATE,0) AS TAX_RATE, IFNULL(D.DISCOUNT,0) AS DISCOUNT, CTE.LINE_ITEM_SALES AS GROSS_SALES_AFTER_TAX, CTE.LINE_ITEM_SALES/(1+IFNULL(T.TAX_RATE,0)) AS GROSS_SALES_BEFORE_TAX, IFNULL(D.DISCOUNT,0)/(1+IFNULL(T.TAX_RATE,0)) AS DISCOUNT_BEFORE_TAX, CTE.LINE_ITEM_SALES/(1+IFNULL(T.TAX_RATE,0)) - IFNULL(D.DISCOUNT,0)/(1+IFNULL(T.TAX_RATE,0)) AS NET_SALES_BEFORE_TAX, IFNULL(T.TAX,0) AS TAX, (CTE.LINE_ITEM_SALES/(1+IFNULL(T.TAX_RATE,0))) - (IFNULL(D.DISCOUNT,0)/(1+IFNULL(T.TAX_RATE,0))) + IFNULL(T.TAX,0) + CTE.SHIPPING_PRICE AS TOTAL_SALES, CASE WHEN R.QUANTITY IS NOT NULL THEN 1 ELSE 0 END AS IS_REFUND FROM CTE LEFT JOIN RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_orders_items_tax T ON CTE.ORDER_ID = T.ORDER_ID AND CTE.LINE_ITEM_ID = T.LINE_ITEM_ID LEFT JOIN RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_orders_items_discount D ON CTE.ORDER_ID = D.ORDER_ID AND CTE.LINE_ITEM_ID = D.LINE_ITEM_ID LEFT JOIN RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_Refunds R ON CTE.ORDER_ID = R.ORDER_ID AND CTE.LINE_ITEM_ID = R.LINE_ITEM_ID; CREATE OR REPLACE TABLE RPSG_DB.maplemonk.FACT_ITEMS_Shopify_AyurvedicSource AS SELECT O.SHOP_NAME, O.ORDER_ID, O.ORDER_NAME, O.CUSTOMER:id::int AS customer_id, O.Name, O.EMAIL, O.PHONE, O.Tags, O.LINE_ITEM_ID, O.SKU, O.PRODUCT_ID, O.CURRENCY, O.IS_REFUND, upper(replace(O.shipping_address:city,\'\"\',\'\')) as city, upper(replace(O.shipping_address:province,\'\"\',\'\')) as State, CASE WHEN P.title = \'\' THEN \'NA\' ELSE P.title END AS product_name, CASE WHEN P.product_type = \'\' THEN \'NA\' ELSE P.product_type END AS category, O.order_status, O.order_timestamp, O.LINE_ITEM_SALES, O.SHIPPING_PRICE, O.QUANTITY, O.TAX, O.TAX_RATE, O.DISCOUNT, O.DISCOUNT_BEFORE_TAX, O.GROSS_SALES_AFTER_TAX, O.GROSS_SALES_BEFORE_TAX, O.NET_SALES_BEFORE_TAX, O.TOTAL_SALES, O.Source, O.LANDING_UTM_MEDIUM, O.LANDING_UTM_SOURCE, O.LANDING_UTM_CAMPAIGN, O.REFERRING_UTM_MEDIUM, O.REFERRING_UTM_SOURCE, O.LANDING_UTM_CHANNEL, O.REFERRING_UTM_CHANNEL, O.FINAL_UTM_CHANNEL FROM RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_orders_items O LEFT JOIN RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_products P ON O.PRODUCT_ID = P.id LEFT JOIN(SELECT customer_id, city, province, row_number() OVER ( partition BY customer_id ORDER BY id DESC) rowid FROM RPSG_DB.maplemonk.Shopify_AyurvedicSource_All_customers_addresses) AS CD ON O.CUSTOMER:id::int = CD.customer_id AND CD.rowid = 1; ALTER TABLE RPSG_DB.maplemonk.FACT_ITEMS_Shopify_AyurvedicSource ADD COLUMN new_customer_flag varchar(50); ALTER TABLE RPSG_DB.maplemonk.FACT_ITEMS_Shopify_AyurvedicSource ADD COLUMN new_customer_flag_month varchar(50); ALTER TABLE RPSG_DB.maplemonk.FACT_ITEMS_Shopify_AyurvedicSource ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE RPSG_DB.maplemonk.FACT_ITEMS_Shopify_AyurvedicSource ADD COLUMN acquisition_product varchar(16777216); UPDATE RPSG_DB.maplemonk.FACT_ITEMS_Shopify_AyurvedicSource AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id, order_timestamp, CASE WHEN order_timestamp <> Min(order_timestamp) OVER ( partition BY customer_id) THEN \'Repeat\' ELSE \'New\' END AS Flag FROM RPSG_DB.maplemonk.FACT_ITEMS_Shopify_AyurvedicSource)AS B WHERE A.order_id = B.order_id AND A.customer_id = B.customer_id; UPDATE RPSG_DB.maplemonk.FACT_ITEMS_Shopify_AyurvedicSource SET new_customer_flag = CASE WHEN new_customer_flag IS NULL THEN \'New\' ELSE new_customer_flag END; CREATE OR replace temporary TABLE RPSG_DB.maplemonk.temp_source AS SELECT DISTINCT customer_id, source FROM ( SELECT DISTINCT customer_id, order_timestamp, source, Min(order_timestamp) OVER ( partition BY customer_id) firstOrderdate FROM RPSG_DB.maplemonk.FACT_ITEMS_Shopify_AyurvedicSource)res WHERE order_timestamp=firstorderdate; UPDATE RPSG_DB.maplemonk.FACT_ITEMS_Shopify_AyurvedicSource AS a SET a.acquisition_channel=b.source FROM RPSG_DB.maplemonk.temp_source b WHERE a.customer_id = b.customer_id; UPDATE RPSG_DB.maplemonk.FACT_ITEMS_Shopify_AyurvedicSource AS A SET A.new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, customer_id, order_timestamp, CASE WHEN Last_day(order_timestamp, \'month\') <> Last_day(Min(order_timestamp) OVER ( partition BY customer_id)) THEN \'Repeated\' ELSE \'New\' END AS Flag FROM RPSG_DB.maplemonk.FACT_ITEMS_Shopify_AyurvedicSource)AS B WHERE A.order_id = B.order_id AND A.customer_id = B.customer_id; CREATE OR replace temporary TABLE RPSG_DB.maplemonk.temp_product AS SELECT DISTINCT customer_id, product_name, Row_number() OVER (partition BY customer_id ORDER BY LINE_ITEM_SALES DESC) rowid FROM ( SELECT DISTINCT customer_id, order_timestamp, product_name, LINE_ITEM_SALES , Min(order_timestamp) OVER (partition BY customer_id) firstOrderdate FROM RPSG_DB.maplemonk.FACT_ITEMS_Shopify_AyurvedicSource )res WHERE order_timestamp=firstorderdate; UPDATE RPSG_DB.maplemonk.FACT_ITEMS_Shopify_AyurvedicSource AS A SET A.acquisition_product=B.product_name FROM ( SELECT * FROM RPSG_DB.maplemonk.temp_product WHERE rowid=1)B WHERE A.customer_id = B.customer_id; CREATE OR REPLACE TABLE RPSG_DB.maplemonk.FACT_ITEMS_Shopify_AyurvedicSource AS SELECT *, RI.ISO_3166_2_CODE AS ISO_Region_Code FROM RPSG_DB.maplemonk.FACT_ITEMS_Shopify_AyurvedicSource FI left join RPSG_DB.maplemonk.region_iso_3166_codes RI on Upper(FI.state) = Upper(RI.Subdivision_name);",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from RPSG_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        