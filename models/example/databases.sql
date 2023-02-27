{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE XYXX_DB.MAPLEMONK.Shopify_All_customers AS select *,\'Shopify_India\' AS Shop_Name from XYXX_DB.MAPLEMONK.SHOPIFY_IN_CUSTOMERS; CREATE OR REPLACE TABLE XYXX_DB.MAPLEMONK.Shopify_All_orders AS SELECT *, iff(charindex(\'utm_medium=\', landing_site)=0,NULL,substring(landing_site, charindex(\'utm_medium=\', landing_site) + len(\'utm_medium=\'), ifnull(nullif(charindex(\'&\', insert(landing_site, 1, charindex(\'utm_medium=\', landing_site)-1, \'\')),0), len(insert(landing_site, 1, charindex(\'utm_medium=\', landing_site)-1, \'\'))+1) - len(\'utm_medium=\')-1)) as LANDING_UTM_MEDIUM, iff(charindex(\'utm_source=\', landing_site)=0,NULL,substring(landing_site, charindex(\'utm_source=\', landing_site) + len(\'utm_source=\'), ifnull(nullif(charindex(\'&\', insert(landing_site, 1, charindex(\'utm_source=\', landing_site)-1, \'\')),0), len(insert(landing_site, 1, charindex(\'utm_source=\', landing_site)-1, \'\'))+1) - len(\'utm_source=\')-1)) as LANDING_UTM_SOURCE, iff(charindex(\'utm_campaign=\', landing_site)=0,NULL,substring(landing_site, charindex(\'utm_campaign=\', landing_site) + len(\'utm_campaign=\'), ifnull(nullif(charindex(\'&\', insert(landing_site, 1, charindex(\'utm_campaign=\', landing_site)-1, \'\')),0), len(insert(landing_site, 1, charindex(\'utm_campaign=\', landing_site)-1, \'\'))+1) - len(\'utm_campaign=\')-1)) as LANDING_UTM_CAMPAIGN, iff(charindex(\'utm_medium=\', REFERRING_SITE)=0,NULL,substring(REFERRING_SITE, charindex(\'utm_medium=\', REFERRING_SITE) + len(\'utm_medium=\'), ifnull(nullif(charindex(\'&\', insert(REFERRING_SITE, 1, charindex(\'utm_medium=\', REFERRING_SITE)-1, \'\')),0), len(insert(REFERRING_SITE, 1, charindex(\'utm_medium=\', REFERRING_SITE)-1, \'\'))+1) - len(\'utm_medium=\')-1)) as REFERRING_UTM_MEDIUM, iff(charindex(\'utm_source=\', REFERRING_SITE)=0,NULL,substring(REFERRING_SITE, charindex(\'utm_source=\', REFERRING_SITE) + len(\'utm_source=\'), ifnull(nullif(charindex(\'&\', insert(REFERRING_SITE, 1, charindex(\'utm_source=\', REFERRING_SITE)-1, \'\')),0), len(insert(REFERRING_SITE, 1, charindex(\'utm_source=\', REFERRING_SITE)-1, \'\'))+1) - len(\'utm_source=\')-1)) as REFERRING_UTM_SOURCE, CASE WHEN LANDING_SITE LIKE \'%facebook.com%\' THEN \'Performance marketing & SM\' WHEN LANDING_SITE LIKE \'%google.com%\' THEN \'Performance marketing & SM\' WHEN LANDING_SITE LIKE \'%google.co%\' THEN \'Performance marketing & SM\' WHEN LANDING_SITE LIKE \'%instagram.com%\' THEN \'Performance marketing & SM\' WHEN LANDING_SITE LIKE \'%com.google%\' THEN \'Performance marketing & SM\' ELSE NULL END AS LANDING_UTM_CHANNEL, CASE WHEN REFERRING_SITE LIKE \'%facebook.com%\' THEN \'Performance marketing & SM\' WHEN REFERRING_SITE LIKE \'%google.com%\' THEN \'Performance marketing & SM\' WHEN REFERRING_SITE LIKE \'%google.co%\' THEN \'Performance marketing & SM\' WHEN REFERRING_SITE LIKE \'%instagram.com%\' THEN \'Performance marketing & SM\' WHEN REFERRING_SITE LIKE \'%com.google%\' THEN \'Performance marketing & SM\' ELSE NULL END AS REFERRING_UTM_CHANNEL FROM (select *,case when lower(tags) like \'%cred%\' then \'CRED\' else \'Shopify_India\' end AS Shop_Name from xyxx_db.MAPLEMONK.SHOPIFY_IN_ORDERS)X; UPDATE xyxx_db.maplemonk.Shopify_All_orders AO SET AO.LANDING_UTM_CHANNEL = UTM.CHANNEL FROM XYXX_DB.MAPLEMONK.XYXX_UTM_CAMPAIGN_MAPPING UTM WHERE AO.LANDING_UTM_CHANNEL IS NULL AND lower(AO.LANDING_UTM_SOURCE) LIKE CONCAT(\'%\',lower(UTM.utm_campaign_source),\'%\'); UPDATE XYXX_db.maplemonk.Shopify_All_orders AO SET AO.REFERRING_UTM_CHANNEL = UTM.CHANNEL FROM XYXX_DB.MAPLEMONK.XYXX_UTM_CAMPAIGN_MAPPING UTM WHERE AO.REFERRING_UTM_CHANNEL IS NULL AND lower(AO.REFERRING_UTM_SOURCE) LIKE CONCAT(\'%\',lower(UTM.utm_campaign_source),\'%\'); UPDATE XYXX_db.maplemonk.Shopify_All_orders SET LANDING_UTM_CHANNEL = \'Direct\' WHERE LANDING_UTM_CHANNEL IS NULL AND LANDING_UTM_SOURCE IS NULL AND REFERRING_UTM_SOURCE IS NULL AND (LANDING_SITE LIKE \'%xyxxcrew.com%\'); UPDATE xyxx_db.maplemonk.Shopify_All_orders SET REFERRING_UTM_CHANNEL = \'Direct\' WHERE REFERRING_UTM_CHANNEL IS NULL AND LANDING_UTM_SOURCE IS NULL AND REFERRING_UTM_SOURCE IS NULL AND (REFERRING_SITE LIKE \'%xyxxcrew.com%\'); create or replace table xyxx_db.maplemonk.GOKWIK_SOURCE as With GO_KWIK as ( select id ,note_attributes ,A.value:\"name\" ,case when lower(note_attributes) like \'%gokwik%\' and lower(replace(A.value:\"name\",\'\"\',\'\')) like \'%utm_source%\' then UPPER(replace(A.value:\"value\",\'\"\',\'\')) end as GOKWIK_UTM_SOURCE from shopify_in_orders, LATERAL flatten (INPUT => note_attributes) A where lower(note_attributes) like \'%gokwik%\' and GOKWIK_UTM_SOURCE is not null ) Select GO_KWIK.* ,ifnull(UTM_MAPPING.CHANNEL, \'Others\') as GOKWIK_MAPPED_CHANNEL ,ifnull(UTM_MAPPING.SOURCE,\'Others\') as GOKWIK_MAPPED_SOURCE from GO_KWIK left join (select * from (select * , row_number() over (partition by utm_campaign_source order by 1) rw from xyxx_db.maplemonk.xyxx_utm_campaign_mapping) where rw=1 and utm_campaign_source is not null ) UTM_MAPPING on lower(GO_KWIK.GOKWIK_UTM_SOURCE) = lower(UTM_MAPPING.utm_campaign_source) ; ALTER TABLE xyxx_db.maplemonk.Shopify_All_orders ADD COLUMN GOKWIK_MAPPED_CHANNEL varchar(16777216); UPDATE xyxx_db.maplemonk.Shopify_All_orders AO SET AO.GOKWIK_MAPPED_CHANNEL = GOKWIK_SOURCE.GOKWIK_MAPPED_CHANNEL from xyxx_db.maplemonk.GOKWIK_SOURCE GOKWIK_SOURCE WHERE AO.ID=GOKWIK_SOURCE.ID; ALTER TABLE xyxx_db.maplemonk.Shopify_All_orders ADD COLUMN GOKWIK_MAPPED_SOURCE varchar(16777216); UPDATE xyxx_db.maplemonk.Shopify_All_orders AO SET AO.GOKWIK_MAPPED_SOURCE = GOKWIK_SOURCE.GOKWIK_MAPPED_SOURCE from xyxx_db.maplemonk.GOKWIK_SOURCE GOKWIK_SOURCE WHERE AO.ID=GOKWIK_SOURCE.ID; ALTER TABLE xyxx_db.maplemonk.Shopify_All_orders ADD COLUMN GOKWIK_UTM_SOURCE varchar(16777216); UPDATE xyxx_db.maplemonk.Shopify_All_orders AO SET AO.GOKWIK_UTM_SOURCE = GOKWIK_SOURCE.GOKWIK_UTM_SOURCE from xyxx_db.maplemonk.GOKWIK_SOURCE GOKWIK_SOURCE WHERE AO.ID=GOKWIK_SOURCE.ID; ALTER TABLE xyxx_db.maplemonk.Shopify_All_orders ADD COLUMN FINAL_UTM_CHANNEL varchar(16777216); UPDATE xyxx_db.maplemonk.Shopify_All_orders SET FINAL_UTM_CHANNEL = COALESCE(GOKWIK_MAPPED_CHANNEL,LANDING_UTM_CHANNEL,REFERRING_UTM_CHANNEL,\'Others\') WHERE LANDING_UTM_CHANNEL IS NULL OR REFERRING_UTM_CHANNEL IS NULL; UPDATE xyxx_db.maplemonk.Shopify_All_orders SET FINAL_UTM_CHANNEL = COALESCE(GOKWIK_MAPPED_CHANNEL,LANDING_UTM_CHANNEL) WHERE LANDING_UTM_CHANNEL IS NOT NULL AND REFERRING_UTM_CHANNEL IS NOT NULL AND lower(LANDING_UTM_CHANNEL) = lower(REFERRING_UTM_CHANNEL); UPDATE xyxx_db.maplemonk.Shopify_All_orders SET FINAL_UTM_CHANNEL = COALESCE(GOKWIK_MAPPED_CHANNEL,LANDING_UTM_CHANNEL) WHERE LANDING_UTM_CHANNEL IS NOT NULL AND REFERRING_UTM_CHANNEL IS NOT NULL AND lower(LANDING_UTM_CHANNEL)<>lower(REFERRING_UTM_CHANNEL); UPDATE xyxx_db.maplemonk.Shopify_All_orders SET LANDING_UTM_CHANNEL = \'Others\' WHERE LANDING_UTM_CHANNEL IS NULL; UPDATE xyxx_db.maplemonk.Shopify_All_orders SET REFERRING_UTM_CHANNEL = \'Others\' WHERE REFERRING_UTM_CHANNEL IS NULL; ALTER TABLE xyxx_db.maplemonk.Shopify_All_orders ADD COLUMN FINAL_UTM_SOURCE varchar(16777216); UPDATE xyxx_db.maplemonk.Shopify_All_orders SET FINAL_UTM_SOURCE = COALESCE(GOKWIK_MAPPED_SOURCE,LANDING_UTM_SOURCE,REFERRING_UTM_SOURCE,\'Others\'); ALTER TABLE xyxx_db.maplemonk.Shopify_All_orders RENAME COLUMN _AIRBYTE_SHOPIFY_IN_ORDERS_HASHID to _AIRBYTE_ORDERS_HASHID; CREATE OR REPLACE TABLE xyxx_db.maplemonk.Shopify_All_products AS select *,\'Shopify_India\' AS Shop_Name from XYXX_DB.MAPLEMONK.SHOPIFY_IN_PRODUCTS; ALTER TABLE xyxx_db.maplemonk.Shopify_All_products RENAME COLUMN _AIRBYTE_SHOPIFY_IN_PRODUCTS_HASHID to _AIRBYTE_PRODUCTS_HASHID; CREATE OR REPLACE TABLE xyxx_db.maplemonk.Shopify_All_products_variants AS select *,\'Shopify_India\' AS Shop_Name from XYXX_DB.MAPLEMONK.SHOPIFY_IN_PRODUCTS_VARIANTS; ALTER TABLE XYXX_DB.MAPLEMONK.SHOPIFY_ALL_PRODUCTS_VARIANTS RENAME COLUMN _AIRBYTE_SHOPIFY_IN_PRODUCTS_HASHID to _AIRBYTE_PRODUCTS_HASHID; CREATE OR REPLACE TABLE XYXX_DB.maplemonk.Shopify_All_customers_addresses AS select *,\'Shopify_India\' AS Shop_Name from XYXX_DB.MAPLEMONK.SHOPIFY_IN_CUSTOMERS_ADDRESSES; CREATE OR REPLACE TABLE XYXX_DB.maplemonk.Shopify_All_orders_items_discount AS SELECT order_id, LINE_ITEM_ID, SUM(B.VALUE:amount::FLOAT) AS DISCOUNT FROM( SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:discount_allocations AS discount_allocations FROM xyxx_db.maplemonk.Shopify_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A)x,LATERAL FLATTEN (INPUT => x.discount_allocations)B GROUP BY ORDER_ID, LINE_ITEM_ID; CREATE OR REPLACE TABLE XYXX_db.maplemonk.Shopify_All_orders_items_tax AS SELECT order_id, LINE_ITEM_ID, SUM(B.VALUE:price::FLOAT) AS TAX, sum(B.VALUE:rate::float) as Tax_Rate FROM( SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:tax_lines AS tax_lines FROM xyxx_db.maplemonk.Shopify_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A)x,LATERAL FLATTEN (INPUT => x.tax_lines)B GROUP BY order_id, LINE_ITEM_ID; CREATE OR REPLACE TABLE xyxx_db.maplemonk.Shopify_All_Refunds AS SELECT Name, ID AS Order_ID, Shop_name, C.value:line_item_id AS line_item_id, SUM(C.VALUE:quantity) AS quantity, SUM(C.VALUE:subtotal) AS subtotal FROM xyxx_db.maplemonk.Shopify_All_orders, LATERAL FLATTEN(INPUT => refunds)A, LATERAL FLATTEN(INPUT=>A.value)B, LATERAL FLATTEN(INPUT => B.value) C WHERE C.value:line_item_id IS NOT null GROUP BY Name, ID, Shop_name, C.value:line_item_id; CREATE OR REPLACE TABLE xyxx_db.maplemonk.Shopify_All_orders_items AS WITH CTE AS (SELECT SHOP_NAME, ID::VARCHAR(16777216) AS ORDER_ID, NAME AS ORDER_NAME, CUSTOMER, replace(customer:default_address:name,\'\"\',\'\') NAME, PHONE, EMAIL, tags, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:sku::STRING AS SKU, A.VALUE:product_id::STRING AS PRODUCT_ID, A.VALUE:title::STRING AS PRODUCT_NAME, CURRENCY, CASE WHEN cancelled_at IS NOT NULL THEN \'CANCELLED\' ELSE \'Shopify_Processed\' END AS order_status, CREATED_AT::DATETIME AS order_timestamp, A.VALUE:price::FLOAT * A.VALUE:quantity::FLOAT AS LINE_ITEM_SALES, (TOTAL_SHIPPING_PRICE_SET:presentment_money:amount::FLOAT/ COUNT(ORDER_ID) OVER(PARTITION BY ORDER_ID ORDER BY ORDER_ID)) AS SHIPPING_PRICE, A.value:price/(1+A.value:tax_lines:rate), A.VALUE:quantity::FLOAT as QUANTITY, \'Shopify\' AS Source, LANDING_UTM_MEDIUM, LANDING_UTM_SOURCE, LANDING_UTM_CAMPAIGN, REFERRING_UTM_MEDIUM, REFERRING_UTM_SOURCE, LANDING_UTM_CHANNEL, REFERRING_UTM_CHANNEL, FINAL_UTM_CHANNEL FROM xyxx_db.maplemonk.Shopify_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A) SELECT CTE.*, IFNULL(T.TAX_RATE,0) AS TAX_RATE, IFNULL(D.DISCOUNT,0) AS DISCOUNT, CTE.LINE_ITEM_SALES AS GROSS_SALES_AFTER_TAX, CTE.LINE_ITEM_SALES/(1+IFNULL(T.TAX_RATE,0)) AS GROSS_SALES_BEFORE_TAX, CASE when T.TAX=0 then IFNULL(D.DISCOUNT,0) else IFNULL(D.DISCOUNT,0)/(1+IFNULL(T.TAX_RATE,0)) end AS DISCOUNT_BEFORE_TAX, CASE when T.TAX=0 then CTE.LINE_ITEM_SALES - IFNULL(D.DISCOUNT,0) else CTE.LINE_ITEM_SALES/(1+IFNULL(T.TAX_RATE,0)) - IFNULL(D.DISCOUNT,0)/(1+IFNULL(T.TAX_RATE,0)) end AS NET_SALES_BEFORE_TAX, IFNULL(T.TAX,0) AS TAX, case when T.TAX=0 then (CTE.LINE_ITEM_SALES) - IFNULL(D.DISCOUNT,0) + IFNULL(T.TAX,0) + CTE.SHIPPING_PRICE else (CTE.LINE_ITEM_SALES/(1+IFNULL(T.TAX_RATE,0))) - (IFNULL(D.DISCOUNT,0)/(1+IFNULL(T.TAX_RATE,0))) + IFNULL(T.TAX,0) + CTE.SHIPPING_PRICE end AS TOTAL_SALES, CASE WHEN R.QUANTITY IS NOT NULL THEN 1 ELSE 0 END AS IS_REFUND FROM CTE LEFT JOIN xyxx_db.maplemonk.Shopify_All_orders_items_tax T ON CTE.ORDER_ID = T.ORDER_ID AND CTE.LINE_ITEM_ID = T.LINE_ITEM_ID LEFT JOIN xyxx_db.maplemonk.Shopify_All_orders_items_discount D ON CTE.ORDER_ID = D.ORDER_ID AND CTE.LINE_ITEM_ID = D.LINE_ITEM_ID LEFT JOIN xyxx_db.maplemonk.Shopify_All_Refunds R ON CTE.ORDER_ID = R.ORDER_ID AND CTE.LINE_ITEM_ID = R.LINE_ITEM_ID; CREATE OR REPLACE TABLE xyxx_db.maplemonk.FACT_ITEMS_XYXX AS SELECT O.SHOP_NAME, O.ORDER_ID, O.ORDER_NAME, O.CUSTOMER:id::int AS customer_id, O.Name, O.EMAIL, O.PHONE, O.Tags, O.LINE_ITEM_ID, O.SKU, O.PRODUCT_ID, O.CURRENCY, O.IS_REFUND, CASE WHEN CD.city IS NULL OR CD.city = \'\' THEN \'NA\' ELSE upper(CD.city) END AS city, CASE WHEN CD.province IS NULL OR CD.province = \'\' THEN \'NA\' ELSE upper(CD.province) END AS state, CASE WHEN P.title = \'\' THEN \'NA\' ELSE P.title END AS product_name, CASE WHEN P.product_type = \'\' THEN \'NA\' ELSE P.product_type END AS category, O.order_status, O.order_timestamp, O.LINE_ITEM_SALES, O.SHIPPING_PRICE, O.QUANTITY, O.TAX, O.TAX_RATE, O.DISCOUNT, O.DISCOUNT_BEFORE_TAX, O.GROSS_SALES_AFTER_TAX, O.GROSS_SALES_BEFORE_TAX, O.NET_SALES_BEFORE_TAX, O.TOTAL_SALES, O.Source, O.LANDING_UTM_MEDIUM, O.LANDING_UTM_SOURCE, O.LANDING_UTM_CAMPAIGN, O.REFERRING_UTM_MEDIUM, O.REFERRING_UTM_SOURCE, O.LANDING_UTM_CHANNEL, O.REFERRING_UTM_CHANNEL, O.FINAL_UTM_CHANNEL FROM xyxx_db.maplemonk.Shopify_All_orders_items O LEFT JOIN xyxx_db.maplemonk.Shopify_All_products P ON O.PRODUCT_ID = P.id LEFT JOIN(SELECT customer_id, city, province, row_number() OVER ( partition BY customer_id ORDER BY id DESC) rowid FROM xyxx_db.maplemonk.Shopify_All_customers_addresses) AS CD ON O.CUSTOMER:id::int = CD.customer_id AND CD.rowid = 1; ALTER TABLE xyxx_db.maplemonk.FACT_ITEMS_XYXX ADD COLUMN new_customer_flag varchar(50); ALTER TABLE xyxx_db.maplemonk.FACT_ITEMS_XYXX ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE xyxx_db.maplemonk.FACT_ITEMS_XYXX ADD COLUMN acquisition_product varchar(16777216); UPDATE XYXX_DB.maplemonk.FACT_ITEMS_XYXX AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id, order_timestamp, CASE WHEN order_timestamp <> Min(order_timestamp) OVER ( partition BY customer_id) THEN \'Repeat\' ELSE \'New\' END AS Flag FROM XYXX_db.maplemonk.FACT_ITEMS_XYXX)AS B WHERE A.order_id = B.order_id AND A.customer_id = B.customer_id; UPDATE XYXX_db.maplemonk.FACT_ITEMS_XYXX SET new_customer_flag = CASE WHEN new_customer_flag IS NULL THEN \'New\' ELSE new_customer_flag END; CREATE OR replace temporary TABLE xyxx_db.maplemonk.temp_source AS SELECT DISTINCT customer_id, source FROM ( SELECT DISTINCT customer_id, order_timestamp, source, Min(order_timestamp) OVER ( partition BY customer_id) firstOrderdate FROM xyxx_db.maplemonk.FACT_ITEMS_XYXX)res WHERE order_timestamp=firstorderdate; UPDATE xyxx_db.maplemonk.FACT_ITEMS_XYXX AS a SET a.acquisition_channel=b.source FROM xyxx_db.maplemonk.temp_source b WHERE a.customer_id = b.customer_id; ALTER TABLE xyxx_db.maplemonk.FACT_ITEMS_XYXX ADD COLUMN SHIPPING_TAX FLOAT; ALTER TABLE xyxx_db.maplemonk.FACT_ITEMS_XYXX ADD COLUMN SHIP_PROMOTION_DISCOUNT FLOAT; ALTER TABLE xyxx_db.maplemonk.FACT_ITEMS_XYXX ADD COLUMN GIFT_WRAP_PRICE FLOAT; ALTER TABLE xyxx_db.maplemonk.FACT_ITEMS_XYXX ADD COLUMN GIFT_WRAP_TAX FLOAT; ALTER TABLE xyxx_db.maplemonk.FACT_ITEMS_XYXX MODIFY COLUMN ORDER_STATUS VARCHAR(100); INSERT INTO XYXX_db.maplemonk.FACT_ITEMS_XYXX SELECT \'Amazon\' AS SHOP_NAME, \"amazon-order-id\" AS ORDER_ID, NULL AS ORDER_NAME, NULL AS CUSTOMER_ID, NULL AS NAME, NULL AS EMAIL, NULL AS PHONE, NULL AS tags, NULL AS LINE_ITEM_ID, SKU, ASIN AS PRODUCT_ID, CURRENCY, case when \"order-status\" in (\'Shipped - Returned to Seller\', \'Shipped - Returning to Seller\',\'Shipped - Rejected by Buyer\',\'Shipped - Damaged\') then 1 else 0 end AS IS_REFUND, upper(\"ship-city\") AS CITY, upper(\"ship-state\") AS STATE, \"product-name\" AS Product_Name, NULL AS CATEGORY, \"order-status\" AS ORDER_STATUS, \"Purchase-datetime-PDT\" AS ORDER_TIMESTAMP, TRY_CAST(\"item-price\" AS FLOAT) AS LINE_ITEM_SALES, TRY_CAST(\"shipping-price\" AS FLOAT) AS SHIPPING_PRICE, TRY_CAST(QUANTITY AS FLOAT) AS QUANTITY, TRY_CAST(\"item-tax\" AS FLOAT) AS TAX, null as TAX_RATE, TRY_CAST(\"item-promotion-discount\" AS FLOAT) AS DISCOUNT, TRY_CAST(\"item-promotion-discount\" AS FLOAT) AS DISCOUNT_BEFORE_TAX, NULL AS GROSS_SALES_AFTER_TAX, NULL AS GROSS_SALES_BEFORE_TAX, NULL AS NET_SALES_BEFORE_TAX, ifnull(TRY_CAST(\"item-price\" AS FLOAT),0)-ifnull(TRY_CAST(\"item-promotion-discount\" AS FLOAT),0)AS TOTAL_SALES, \'Amazon\' AS SOURCE, NULL AS LANDING_UTM_MEDIUM, NULL AS LANDING_UTM_SOURCE, NULL AS LANDING_UTM_CAMPAIGN, NULL AS REFERRING_UTM_MEDIUM, NULL AS REFERRING_UTM_SOURCE, NULL AS LANDING_UTM_CHANNEL, NULL AS REFERRING_UTM_CHANNEL, NULL AS FINAL_UTM_CHANNEL, NULL AS NEW_CUSTOMER_FLAG, NULL AS ACQUISITION_CHANNEL, NULL AS ACQUISITION_PRODUCT, TRY_CAST(\"shipping-tax\" AS FLOAT) AS SHIPPING_TAX, TRY_CAST(\"ship-promotion-discount\" AS FLOAT) AS SHIP_PROMOTION_DISCOUNT, TRY_CAST(\"gift-wrap-price\" AS FLOAT) AS GIFT_WRAP_PRICE, TRY_CAST(\"gift-wrap-tax\" AS FLOAT) AS GIFT_WRAP_TAX FROM (SELECT *, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\', \"purchase-date\":: DATETIME) as \"Purchase-datetime-PDT\" FROM xyxx_db.maplemonk.ASP_IN_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL )X WHERE upper(CURRENCY) = \'INR\' AND \"item-price\" NOT IN(\'\',\'0.0\'); CREATE OR REPLACE TABLE XYXX_db.maplemonk.FACT_ITEMS_TEMP_Category as select fi.*,fi.SKU AS SKU_CODE,p.name as PRODUCT_NAME_Final,coalesce(Upper(p.CATEGORY),upper(fi.category)) AS Product_Category, Upper(q.category) as Product_Super_Category from XYXX_db.maplemonk.FACT_ITEMS_XYXX fi left join (select distinct skucode, name, category from XYXX_DB.maplemonk.sku_master) p on fi.sku = p.skucode left join (select distinct product_type, category from XYXX_DB.maplemonk.product_type_category_mapping) q on fi.category=q.product_type; CREATE OR REPLACE TABLE XYXX_db.maplemonk.FACT_ITEMS_XYXX AS SELECT * FROM XYXX_db.maplemonk.FACT_ITEMS_TEMP_Category; CREATE OR replace temporary TABLE xyxx_db.maplemonk.temp_product AS SELECT DISTINCT customer_id, product_name, Row_number() OVER (partition BY customer_id ORDER BY LINE_ITEM_SALES DESC) rowid FROM ( SELECT DISTINCT customer_id, order_timestamp, product_name, LINE_ITEM_SALES , Min(order_timestamp) OVER (partition BY customer_id) firstOrderdate FROM xyxx_db.maplemonk.FACT_ITEMS_XYXX )res WHERE order_timestamp=firstorderdate; UPDATE xyxx_db.maplemonk.FACT_ITEMS_XYXX AS A SET A.acquisition_product=B.product_name FROM ( SELECT * FROM xyxx_db.maplemonk.temp_product WHERE rowid=1)B WHERE A.customer_id = B.customer_id; CREATE OR REPLACE TABLE xyxx_db.maplemonk.FACT_ITEMS_SHOPIFY_XYXX AS SELECT FI.*, RI.* ,c.return_flag Return_flag ,c.Return_quantity Return_Quantity ,Total_sales*c.Return_quantity/QUANTITY as Return_Value ,c.shipping_last_update_date last_update_date ,c.shipping_status Shipping_status FROM xyxx_db.maplemonk.FACT_ITEMS_XYXX FI left join xyxx_db.maplemonk.region_iso_3166_codes RI on Upper(FI.state) = Upper(RI.Subdivision_name) left join (select * from (select order_id ,city ,state ,saleorderitemcode ,sales_order_item_id ,shippingpackagecode ,SHIPPINGPACKAGESTATUS ,shipping_status ,order_status ,Courier ,Dispatch_Date ,Delivered_date ,Return_flag ,Return_quantity ,cancelled_quantity ,shipping_last_update_date ,days_in_shipment ,awb ,payment_method ,row_number() over (partition by order_id, split_part(saleorderitemcode,\'-\',0) order by shipping_last_update_date desc) rw from XYXX_DB.maplemonk.UNICOMMERCE_FACT_ITEMS_XYXX_FINAL where lower(marketplace) like any (\'%shopify%\',\'%amazon%\')) where rw=1 )c on FI.order_id=c.order_id and FI.line_item_id=split_part(c.saleorderitemcode,\'-\',0) where lower(SOURCE) like (\'%shopify%\');",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from XYXX_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        