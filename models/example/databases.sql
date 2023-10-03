{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE TABLE IF NOT EXISTS prd_db.beardo.dwh_UTM_MAPPING ( UTM_SOURCE VARCHAR(16777216), UTM_MEDIUM VARCHAR(16777216), CHANNEL VARCHAR(16777216)); CREATE TABLE IF NOT EXISTS prd_db.beardo.dwh_SKU_MASTER ( skucode VARCHAR(16777216), name VARCHAR(16777216), category VARCHAR(16777216), sub_category VARCHAR(16777216)); CREATE OR REPLACE TABLE prd_db.beardo.dwh_Shopify_All_customers AS select *,\'Shopify_beardo\' AS Shop_Name from datalake_db.beardo.trn_Shopify_beardo_CUSTOMERS ; create or replace table prd_db.beardo.dwh_Shopify_beardo_UTM_Parameters as select ShopifyQL.* ,upper(coalesce(UTM_MAPPING.CHANNEL,UTM_MAPPING_REF.CHANNEL,ShopifyQL.ShopifyQL_Unmapped_Last_Source)) as ShopifyQL_MAPPED_CHANNEL ,upper(coalesce(UTM_MAPPING.UTM_SOURCE,UTM_MAPPING_REF.UTM_SOURCE,ShopifyQL.ShopifyQL_Unmapped_Last_Source)) as ShopifyQL_MAPPED_SOURCE from (select * from (select A.id ,A.name ,A.createdat ,replace(A.customerjourneysummary:\"momentsCount\",\'\"\',\'\') Moments_Count ,replace(A.customerjourneysummary:\"daysToConversion\",\'\"\',\'\') DaysToConvert ,replace(A.CUSTOMERJOURNEYSUMMARY:\"lastVisit\":\"utmParameters\":\"source\",\'\"\',\'\') LastVisit_UTM_Source ,replace(A.CUSTOMERJOURNEYSUMMARY:\"lastVisit\":\"source\",\'\"\',\'\') LastVisit_NON_UTM_Source ,replace(A.CUSTOMERJOURNEYSUMMARY:\"lastVisit\":\"utmParameters\":\"campaign\",\'\"\',\'\') LastVisit_UTM_Campaign ,replace(A.CUSTOMERJOURNEYSUMMARY:\"firstVisit\":\"utmParameters\":\"source\",\'\"\',\'\') FirstVisit_UTM_Source ,replace(A.CUSTOMERJOURNEYSUMMARY:\"firstVisit\":\"utmParameters\":\"medium\",\'\"\',\'\') FirstVisit_UTM_Medium ,replace(A.CUSTOMERJOURNEYSUMMARY:\"firstVisit\":\"utmParameters\":\"campaign\",\'\"\',\'\') FirstVisit_UTM_Campaign ,replace(B.Value:\"id\",\'gid://shopify/CustomerVisit/\',\'\') Moment_ID ,replace(B.value:\"utmParameters\":\"source\",\'\"\',\'\') Last_Moment_UTM_Source ,replace(B.value:\"utmParameters\":\"medium\",\'\"\',\'\') Last_Moment_UTM_Medium ,case when Moments_Count >1 then LastVisit_UTM_Source else FirstVisit_UTM_Source end CJSummary_utm_source ,referrerdisplaytext Referrer_Name ,customerjourneysummary ,customerjourney ,coalesce(Last_Moment_UTM_Source,LastVisit_NON_UTM_Source) ShopifyQL_Unmapped_Last_Source ,rank() over (partition by name order by MOMENT_ID desc) rw from datalake_db.beardo.trn_beardo_UTM_PARAMETERS A, lateral flatten (INPUT => customerjourney:\"moments\",OUTER => TRUE) B ) where rw=1 ) ShopifyQL left join (select * from (select * , row_number() over (partition by lower(utm_source) order by 1) rw from prd_db.beardo.dwh_utm_mapping) where rw=1 and utm_source is not null ) UTM_MAPPING on lower(ShopifyQL.ShopifyQL_Unmapped_Last_Source) = lower(UTM_MAPPING.utm_source) left join (select * from (select * , row_number() over (partition by lower(utm_source) order by 1) rw from prd_db.beardo.dwh_utm_mapping) where rw=1 and utm_source is not null ) UTM_MAPPING_REF on lower(ShopifyQL.referrer_name) = lower(UTM_MAPPING_REF.utm_source) left join (select * from (select * , row_number() over (partition by lower(utm_source) order by 1) rw from prd_db.beardo.dwh_utm_mapping) where rw=1 and utm_source is not null ) UTM_MAPPING_FIRST_CLICK on lower(ShopifyQL.FirstVisit_UTM_Source) = lower(UTM_MAPPING_FIRST_CLICK.utm_source) ; create or replace table prd_db.beardo.dwh_Shopify_All_orders as select AO.* ,UPPER(ShopifyQL.shopifyql_mapped_channel) shopifyql_mapped_channel ,UPPER(ShopifyQL.shopifyql_mapped_source) shopifyql_mapped_source ,UPPER(ShopifyQL.FIRSTVISIT_UTM_SOURCE) Shopifyql_FIRSTVISIT_UTM_SOURCE ,UPPER(ShopifyQL.FirstVisit_UTM_Campaign) Shopifyql_FIRSTVISIT_UTM_CAMPAIGN ,upper(ShopifyQL.LastVisit_UTM_Campaign) Shopifyql_LAST_VISIT_UTM_CAMPAIGN ,UPPER(ShopifyQL.LAST_MOMENT_UTM_SOURCE) Shopifyql_LAST_MOMENT_UTM_SOURCE ,UPPER(ShopifyQL.LastVisit_NON_UTM_Source) Shopifyql_LAST_VISIT_NON_UTM_SOURCE ,UPPER(ShopifyQL.LAST_MOMENT_UTM_MEDIUM) Shopifyql_LAST_MOMENT_UTM_MEDIUM ,UPPER(ShopifyQL.FIRSTVISIT_UTM_MEDIUM) Shopifyql_FIRSTVISIT_UTM_MEDIUM ,div0(ShopifyQL.MOMENTS_COUNT,count(1) over (partition by AO.name order by 1)) MOMENTS_COUNT ,div0(ShopifyQL.DAYSTOCONVERT,count(1) over (partition by AO.name order by 1)) DAYSTOCONVERT ,UPPER(Referrer_Name) Referrer_Name ,UPPER(ShopifyQL.LastVisit_UTM_Campaign) FINAL_UTM_CAMPAIGN ,Upper(coalesce(shopifyql_mapped_source,ShopifyQL.ShopifyQL_Unmapped_Last_Source,\'Direct\')) FINAL_UTM_SOURCE ,Upper(coalesce(shopifyql_mapped_channel,ShopifyQL.ShopifyQL_Unmapped_Last_Source,\'Direct\')) FINAL_UTM_CHANNEL from (select * ,\'Shopify_beardo\' AS Shop_Name from datalake_db.beardo.trn_Shopify_beardo_ORDERS) AO left join (select * from (select *, row_number() over (partition by name order by 1) rwb from prd_db.beardo.dwh_Shopify_beardo_UTM_Parameters ) where rwb = 1) ShopifyQL on AO.name = ShopifyQL.name ; ALTER TABLE prd_db.beardo.dwh_Shopify_All_orders RENAME COLUMN _AIRBYTE_trn_Shopify_beardo_ORDERS_HASHID to _AIRBYTE_ORDERS_HASHID; CREATE OR REPLACE TABLE prd_db.beardo.dwh_Shopify_All_products AS select *,\'Shopify_beardo\' AS Shop_Name from datalake_db.beardo.trn_Shopify_beardo_PRODUCTS ; ALTER TABLE prd_db.beardo.dwh_Shopify_All_products RENAME COLUMN _AIRBYTE_trn_Shopify_beardo_PRODUCTS_HASHID to _AIRBYTE_PRODUCTS_HASHID; CREATE OR REPLACE TABLE prd_db.beardo.dwh_Shopify_All_products_variants AS select *,\'Shopify_beardo\' AS Shop_Name from datalake_db.beardo.trn_Shopify_beardo_PRODUCTS_VARIANTS ; ALTER TABLE prd_db.beardo.dwh_SHOPIFY_ALL_PRODUCTS_VARIANTS RENAME COLUMN _AIRBYTE_trn_Shopify_beardo_PRODUCTS_HASHID to _AIRBYTE_PRODUCTS_HASHID; CREATE OR REPLACE TABLE prd_db.beardo.dwh_Shopify_All_customers_addresses AS select *,\'Shopify_beardo\' AS Shop_Name from datalake_db.beardo.trn_Shopify_beardo_CUSTOMERS_ADDRESSES ; CREATE OR REPLACE TABLE prd_db.beardo.dwh_Shopify_All_orders_items_discount AS SELECT order_id, LINE_ITEM_ID, SUM(B.VALUE:amount::FLOAT) AS DISCOUNT FROM( SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:discount_allocations AS discount_allocations FROM prd_db.beardo.dwh_Shopify_All_orders , LATERAL FLATTEN (INPUT => LINE_ITEMS)A)x,LATERAL FLATTEN (INPUT => x.discount_allocations)B GROUP BY ORDER_ID, LINE_ITEM_ID; CREATE OR REPLACE TABLE prd_db.beardo.dwh_Shopify_All_orders_items_tax AS SELECT order_id, LINE_ITEM_ID, SUM(B.VALUE:price::FLOAT) AS TAX, sum(B.VALUE:rate::float) as Tax_Rate FROM( SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:tax_lines AS tax_lines FROM prd_db.beardo.DWH_SHOPIFY_ALL_ORDERS, LATERAL FLATTEN (INPUT => LINE_ITEMS)A)x,LATERAL FLATTEN (INPUT => x.tax_lines)B GROUP BY order_id, LINE_ITEM_ID; CREATE OR REPLACE TABLE prd_db.beardo.dwh_Shopify_All_Refunds AS SELECT Name, ID AS Order_ID, Shop_name, C.value:line_item_id AS line_item_id, SUM(C.VALUE:quantity) AS quantity, SUM(C.VALUE:subtotal) AS subtotal FROM prd_db.beardo.DWH_SHOPIFY_ALL_ORDERS, LATERAL FLATTEN(INPUT => refunds)A, LATERAL FLATTEN(INPUT=>A.value)B, LATERAL FLATTEN(INPUT => B.value) C WHERE C.value:line_item_id IS NOT null GROUP BY Name, ID, Shop_name, C.value:line_item_id; CREATE OR REPLACE TABLE prd_db.beardo.dwh_Shopify_All_orders_fulfillment AS SELECT A.value:order_id AS order_id, B.value:id Line_Item_ID, replace(A.VALUE:tracking_number,\'\"\',\'\') as AWB, Upper(replace(A.VALUE:tracking_company,\'\"\',\'\')) as Courier, Upper(replace(A.VALUE:shipment_status,\'\"\',\'\')) as Shipping_status, replace(A.VALUE:updated_at,\'\"\',\'\') as shipping_status_update_date, replace(A.VALUE:tracking_url,\'\"\',\'\') as tracking_url, replace(A.VALUE:created_at,\'\"\',\'\') as Shipping_created_at FROM prd_db.beardo.DWH_SHOPIFY_ALL_ORDERS, LATERAL FLATTEN (INPUT => fulfillments)A,LATERAL FLATTEN (INPUT => A.value:line_items)B ; CREATE OR REPLACE TABLE prd_db.beardo.dwh_Shopify_All_discount_codes AS select ID, code, affiliate from ( select ID, CODE from ( SELECT id, replace(A.value:code , \'\"\',\'\') code, row_number() over (partition by id order by code) rw FROM prd_db.beardo.DWH_SHOPIFY_ALL_ORDERS, LATERAL FLATTEN (INPUT => discount_codes) A )where rw=1)a left join datalake_db.beardo.mst_code_affiliate_mapping b on upper(a.code) LIKE concat(\'%\',b.\"Code prefix\",\'%\') ; CREATE OR REPLACE TABLE prd_db.beardo.dwh_Shopify_All_orders_items AS WITH CTE AS (SELECT SHOP_NAME, ID::VARCHAR(16777216) AS ORDER_ID, NAME AS ORDER_NAME, CUSTOMER, replace(customer:default_address:name,\'\"\',\'\') NAME, coalesce(phone, replace(customer:default_address:phone,\'\"\',\'\')) phone, coalesce(email, replace(customer:email,\'\"\',\'\')) email, replace(shipping_address:zip,\'\"\',\'\') pincode, tags, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:sku::STRING AS SKU, A.VALUE:product_id::STRING AS PRODUCT_ID, A.VALUE:title::STRING AS PRODUCT_NAME, CURRENCY, CASE WHEN cancelled_at IS NOT NULL THEN \'CANCELLED\' ELSE \'Shopify_Processed\' END AS order_status, CREATED_AT::DATETIME AS order_timestamp, A.VALUE:price::FLOAT * A.VALUE:quantity::FLOAT AS LINE_ITEM_SALES, (TOTAL_SHIPPING_PRICE_SET:presentment_money:amount::FLOAT/ COUNT(ORDER_ID) OVER(PARTITION BY ORDER_ID ORDER BY ORDER_ID)) AS SHIPPING_PRICE, A.value:price/(1+A.value:tax_lines:rate), A.VALUE:quantity::FLOAT as QUANTITY, \'Shopify\' AS Source, MOMENTS_COUNT, DAYSTOCONVERT, SHOPIFYQL_FIRSTVISIT_UTM_SOURCE, SHOPIFYQL_MAPPED_CHANNEL, SHOPIFYQL_LAST_MOMENT_UTM_SOURCE, Shopifyql_LAST_VISIT_NON_UTM_SOURCE, Shopifyql_LAST_MOMENT_UTM_MEDIUM, Shopifyql_FIRSTVISIT_UTM_MEDIUM, Shopifyql_FIRSTVISIT_UTM_CAMPAIGN, Shopifyql_LAST_VISIT_UTM_CAMPAIGN, FINAL_UTM_CHANNEL, FINAL_UTM_SOURCE, FINAL_UTM_CAMPAIGN, Referrer_Name, NULL as product_sub_category, payment_gateway_names, discount_codes FROM prd_db.beardo.DWH_SHOPIFY_ALL_ORDERS, LATERAL FLATTEN (INPUT => LINE_ITEMS)A) SELECT CTE.*, dc.code discount_code, dc.affiliate, IFNULL(T.TAX_RATE,0) AS TAX_RATE, IFNULL(D.DISCOUNT,0) AS DISCOUNT, CTE.LINE_ITEM_SALES AS GROSS_SALES_AFTER_TAX, CTE.LINE_ITEM_SALES/(1+IFNULL(T.TAX_RATE,0)) AS GROSS_SALES_BEFORE_TAX, IFNULL(D.DISCOUNT,0)/(1+IFNULL(T.TAX_RATE,0)) AS DISCOUNT_BEFORE_TAX, CTE.LINE_ITEM_SALES/(1+IFNULL(T.TAX_RATE,0)) - IFNULL(D.DISCOUNT,0)/(1+IFNULL(T.TAX_RATE,0)) AS NET_SALES_BEFORE_TAX, case when IFNULL(T.TAX,0) = 0 then CTE.LINE_ITEM_SALES*tax_rate else IFNULL(T.TAX,0) end AS TAX, CTE.LINE_ITEM_SALES - IFNULL(D.DISCOUNT,0) + CTE.SHIPPING_PRICE AS TOTAL_SALES, CASE WHEN R.QUANTITY IS NOT NULL THEN 1 ELSE 0 END AS IS_REFUND, F.AWB AWB, F.SHIPPING_STATUS Shopify_Shipping_Status, F.SHIPPING_STATUS_UPDATE_DATE Shopify_Shipping_Updated_Date, F.COURIER SHOPIFY_COURIER, pg.mapped_pg_name payment_gateway_mapped FROM CTE LEFT JOIN prd_db.beardo.DWH_SHOPIFY_ALL_ORDERS_ITEMS_TAX T ON CTE.ORDER_ID = T.ORDER_ID AND CTE.LINE_ITEM_ID = T.LINE_ITEM_ID LEFT JOIN prd_db.beardo.dwh_Shopify_All_orders_items_discount D ON CTE.ORDER_ID = D.ORDER_ID AND CTE.LINE_ITEM_ID = D.LINE_ITEM_ID LEFT JOIN prd_db.beardo.DWH_SHOPIFY_ALL_REFUNDS R ON CTE.ORDER_ID = R.ORDER_ID AND CTE.LINE_ITEM_ID = R.LINE_ITEM_ID LEFT JOIN prd_db.beardo.DWH_SHOPIFY_ALL_ORDERS_FULFILLMENT F ON CTE.ORDER_ID = F.ORDER_ID AND CTE.LINE_ITEM_ID = F.LINE_ITEM_ID left join (select * from( select *, row_number() over (partition by id, code order by 1) rw from prd_db.beardo.dwh_Shopify_All_discount_codes) where rw = 1) DC on CTE.order_id = DC.id left join datalake_db.beardo.mst_payment_gateway_names_mapping pg on replace(pg.shopify_pg_name::string,\' \',\'\') = replace(cte.payment_gateway_names::string,\' \',\'\') ; CREATE OR REPLACE TABLE prd_db.beardo.dwh_SHOPIFY_FACT_ITEMS AS SELECT O.SHOP_NAME, O.ORDER_ID, O.ORDER_NAME, O.CUSTOMER:id::int AS customer_id, O.Name, O.EMAIL, o.pincode, O.PHONE, O.Tags, O.LINE_ITEM_ID, O.SKU, O.PRODUCT_ID, O.CURRENCY, O.IS_REFUND, CASE WHEN CD.city IS NULL OR CD.city = \'\' THEN \'NA\' ELSE upper(CD.city) END AS city, CASE WHEN CD.province IS NULL OR CD.province = \'\' THEN \'NA\' ELSE upper(CD.province) END AS state, CASE WHEN P.title = \'\' THEN \'NA\' ELSE P.title END AS product_name, CASE WHEN P.product_type = \'\' THEN \'NA\' ELSE P.product_type END AS category, O.order_status, O.order_timestamp, O.LINE_ITEM_SALES, O.SHIPPING_PRICE, O.QUANTITY, O.TAX, O.TAX_RATE, O.DISCOUNT, O.DISCOUNT_BEFORE_TAX, O.GROSS_SALES_AFTER_TAX, O.GROSS_SALES_BEFORE_TAX, O.NET_SALES_BEFORE_TAX, O.TOTAL_SALES, O.Source, O.MOMENTS_COUNT, O.DAYSTOCONVERT, O.SHOPIFYQL_FIRSTVISIT_UTM_SOURCE, O.SHOPIFYQL_MAPPED_CHANNEL, O.SHOPIFYQL_LAST_MOMENT_UTM_SOURCE, O.Shopifyql_LAST_VISIT_NON_UTM_SOURCE, O.Shopifyql_FIRSTVISIT_UTM_MEDIUM, O.Shopifyql_LAST_MOMENT_UTM_MEDIUM, O.FINAL_UTM_CHANNEL, O.FINAL_UTM_CAMPAIGN, O.FINAL_UTM_SOURCE, O.Referrer_Name, O.AWB, O.SHOPIFY_SHIPPING_STATUS, O.SHOPIFY_SHIPPING_UPDATED_DATE, O.SHOPIFY_COURIER, O.Shopifyql_FIRSTVISIT_UTM_CAMPAIGN, O.Shopifyql_LAST_VISIT_UTM_CAMPAIGN, o.product_sub_category, o.discount_code, o.affiliate, payment_gateway_mapped FROM prd_db.beardo.DWH_SHOPIFY_ALL_ORDERS_ITEMS O LEFT JOIN prd_db.beardo.dwh_Shopify_All_products P ON O.PRODUCT_ID = P.id LEFT JOIN(SELECT customer_id, city, province, row_number() OVER ( partition BY customer_id ORDER BY id DESC) rowid FROM prd_db.beardo.dwh_Shopify_All_customers_addresses) AS CD ON O.CUSTOMER:id::int = CD.customer_id AND CD.rowid = 1; ALTER TABLE prd_db.beardo.dwh_SHOPIFY_FACT_ITEMS ADD COLUMN new_customer_flag varchar(50); ALTER TABLE prd_db.beardo.dwh_SHOPIFY_FACT_ITEMS ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE prd_db.beardo.dwh_SHOPIFY_FACT_ITEMS ADD COLUMN acquisition_product varchar(16777216); UPDATE prd_db.beardo.dwh_SHOPIFY_FACT_ITEMS AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id, order_timestamp, CASE WHEN order_timestamp <> Min(order_timestamp) OVER ( partition BY customer_id) THEN \'Repeat\' ELSE \'New\' END AS Flag FROM prd_db.beardo.dwh_SHOPIFY_FACT_ITEMS)AS B WHERE A.order_id = B.order_id AND A.customer_id = B.customer_id; UPDATE prd_db.beardo.dwh_SHOPIFY_FACT_ITEMS SET new_customer_flag = CASE WHEN new_customer_flag IS NULL THEN \'New\' ELSE new_customer_flag END; CREATE OR replace temporary TABLE prd_db.beardo.dwh_temp_source AS SELECT DISTINCT customer_id, source FROM ( SELECT DISTINCT customer_id, order_timestamp, source, Min(order_timestamp) OVER ( partition BY customer_id) firstOrderdate FROM prd_db.beardo.dwh_SHOPIFY_FACT_ITEMS)res WHERE order_timestamp=firstorderdate; UPDATE prd_db.beardo.dwh_SHOPIFY_FACT_ITEMS AS a SET a.acquisition_channel=b.source FROM prd_db.beardo.dwh_temp_source b WHERE a.customer_id = b.customer_id; ALTER TABLE prd_db.beardo.dwh_SHOPIFY_FACT_ITEMS ADD COLUMN SHIPPING_TAX FLOAT; ALTER TABLE prd_db.beardo.dwh_SHOPIFY_FACT_ITEMS ADD COLUMN SHIP_PROMOTION_DISCOUNT FLOAT; ALTER TABLE prd_db.beardo.dwh_SHOPIFY_FACT_ITEMS ADD COLUMN GIFT_WRAP_PRICE FLOAT; ALTER TABLE prd_db.beardo.dwh_SHOPIFY_FACT_ITEMS ADD COLUMN GIFT_WRAP_TAX FLOAT; ALTER TABLE prd_db.beardo.dwh_SHOPIFY_FACT_ITEMS MODIFY COLUMN ORDER_STATUS VARCHAR(100); CREATE OR REPLACE TABLE prd_db.beardo.dwh_SHOPIFY_FACT_ITEMS_TEMP_Category as select fi.* ,fi.SKU AS SKU_CODE ,coalesce(p.Product_name_mapped,fi.product_name) as PRODUCT_NAME_Final ,coalesce(Upper(p.Product_category_mapped),upper(fi.category)) AS Product_Category ,Upper(p.Product_Sub_Category_Mapped) as Product_Super_Category ,p.MRP ,p.mrp*fi.quantity - (fi.GROSS_SALES_AFTER_TAX - discount) mrp_discount from prd_db.beardo.dwh_SHOPIFY_FACT_ITEMS fi left join (select distinct \"Product Code\" skucode, name Product_name_mapped, BUSINESS Product_category_mapped, \"Category Name\" Product_Sub_Category_Mapped, mrp::float mrp from datalake_db.beardo.mst_mapping_sku_master ) p on fi.sku = p.skucode; CREATE OR REPLACE TABLE prd_db.beardo.dwh_SHOPIFY_FACT_ITEMS AS SELECT * FROM prd_db.beardo.DWH_SHOPIFY_FACT_ITEMS_TEMP_CATEGORY; CREATE OR replace temporary TABLE prd_db.beardo.dwh_temp_product AS SELECT DISTINCT customer_id, product_name, Row_number() OVER (partition BY customer_id ORDER BY LINE_ITEM_SALES DESC) rowid FROM ( SELECT DISTINCT customer_id, order_timestamp, product_name, LINE_ITEM_SALES , Min(order_timestamp) OVER (partition BY customer_id) firstOrderdate FROM prd_db.beardo.DWH_SHOPIFY_FACT_ITEMS )res WHERE order_timestamp=firstorderdate; UPDATE prd_db.beardo.dwh_SHOPIFY_FACT_ITEMS AS A SET A.acquisition_product=B.product_name FROM ( SELECT * FROM prd_db.beardo.dwh_temp_product WHERE rowid=1)B WHERE A.customer_id = B.customer_id;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from PRD_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        