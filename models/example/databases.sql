{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE TABLE IF NOT EXISTS MAPLEMONKTEST185_DB.Maplemonk.UTM_MAPPING ( UTM_SOURCE VARCHAR(16777216), UTM_MEDIUM VARCHAR(16777216), SOURCE VARCHAR(16777216), CHANNEL VARCHAR(16777216)); CREATE TABLE IF NOT EXISTS MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SKU_MASTER ( skucode VARCHAR(16777216), name VARCHAR(16777216), category VARCHAR(16777216), sub_category VARCHAR(16777216)); create table if not exists MAPLEMONKTEST185_DB.Maplemonk.Shopify_gladful_UTM_Parameters (ID VARCHAR(16777216) ,NAME VARCHAR(16777216) ,CREATEDAT VARCHAR(16777216) ,CUSTOMERJOURNEY VARIANT ,CUSTOMERJOURNEYSUMMARY VARIANT ,REFERRERURL VARCHAR(16777216) ,REFERRALCODE VARCHAR(16777216) ,LANDINGPAGEURL VARCHAR(16777216) ,REFERRERDISPLAYTEXT VARCHAR(16777216) ,LANDINGPAGEDISPLAYTEXT VARCHAR(16777216) ); create or replace table MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_Shopify_UTM_Parameters as select ShopifyQL.* ,upper(coalesce(UTM_MAPPING.CHANNEL,UTM_MAPPING_REF.CHANNEL,ShopifyQL.ShopifyQL_Unmapped_Last_Source)) as ShopifyQL_MAPPED_CHANNEL ,upper(coalesce(UTM_MAPPING.SOURCE,UTM_MAPPING_REF.SOURCE,ShopifyQL.ShopifyQL_Unmapped_Last_Source)) as ShopifyQL_MAPPED_SOURCE from (select * from (select A.id ,A.name ,A.createdat ,replace(A.customerjourneysummary:\"momentsCount\",\'\"\',\'\') Moments_Count ,replace(A.customerjourneysummary:\"daysToConversion\",\'\"\',\'\') DaysToConvert ,replace(A.CUSTOMERJOURNEYSUMMARY:\"lastVisit\":\"utmParameters\":\"source\",\'\"\',\'\') LastVisit_UTM_Source ,replace(A.CUSTOMERJOURNEYSUMMARY:\"lastVisit\":\"source\",\'\"\',\'\') LastVisit_NON_UTM_Source ,replace(A.CUSTOMERJOURNEYSUMMARY:\"lastVisit\":\"utmParameters\":\"campaign\",\'\"\',\'\') LastVisit_UTM_Campaign ,replace(A.CUSTOMERJOURNEYSUMMARY:\"firstVisit\":\"utmParameters\":\"source\",\'\"\',\'\') FirstVisit_UTM_Source ,replace(A.CUSTOMERJOURNEYSUMMARY:\"firstVisit\":\"utmParameters\":\"medium\",\'\"\',\'\') FirstVisit_UTM_Medium ,replace(A.CUSTOMERJOURNEYSUMMARY:\"firstVisit\":\"utmParameters\":\"campaign\",\'\"\',\'\') FirstVisit_UTM_Campaign ,replace(B.Value:\"id\",\'gid://shopify/CustomerVisit/\',\'\') Moment_ID ,replace(B.value:\"utmParameters\":\"source\",\'\"\',\'\') Last_Moment_UTM_Source ,replace(B.value:\"utmParameters\":\"medium\",\'\"\',\'\') Last_Moment_UTM_Medium ,case when Moments_Count >1 then LastVisit_UTM_Source else FirstVisit_UTM_Source end CJSummary_utm_source ,referrerdisplaytext Referrer_Name ,customerjourneysummary ,customerjourney ,coalesce(Last_Moment_UTM_Source,LastVisit_NON_UTM_Source) ShopifyQL_Unmapped_Last_Source ,rank() over (partition by name order by MOMENT_ID desc) rw from MAPLEMONKTEST185_DB.Maplemonk.Shopify_gladful_UTM_Parameters A, lateral flatten (INPUT => customerjourney:\"moments\",OUTER => TRUE) B ) where rw=1 ) ShopifyQL left join (select * from (select * , row_number() over (partition by lower(utm_source) order by 1) rw from MAPLEMONKTEST185_DB.Maplemonk.UTM_MAPPING) where rw=1 and utm_source is not null ) UTM_MAPPING on lower(ShopifyQL.ShopifyQL_Unmapped_Last_Source) = lower(UTM_MAPPING.utm_source) left join (select * from (select * , row_number() over (partition by lower(utm_source) order by 1) rw from MAPLEMONKTEST185_DB.Maplemonk.utm_mapping) where rw=1 and UTM_Source is not null ) UTM_MAPPING_REF on lower(ShopifyQL.referrer_name) = lower(UTM_MAPPING_REF.utm_source) ; create or replace table MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_GOKWIK_SOURCE as With GO_KWIK as ( WITH utm_source_cte AS ( SELECT S.id, UPPER(A.value:\"value\") AS utm_source FROM MAPLEMONKTEST185_DB.Maplemonk.Shopify_gladful_ORDERS S, LATERAL FLATTEN(INPUT => note_attributes) A where LOWER(S.note_attributes) LIKE \'%gokwik%\' AND LOWER(A.value:\"name\") = \'utm_source\' ), utm_medium_cte AS ( SELECT S.id, UPPER(A.value:\"value\") AS utm_medium FROM MAPLEMONKTEST185_DB.Maplemonk.Shopify_gladful_ORDERS S, LATERAL FLATTEN(INPUT => note_attributes) A where LOWER(S.note_attributes) LIKE \'%gokwik%\' AND LOWER(A.value:\"name\") = \'utm_medium\' ), utm_campaign_cte AS ( SELECT S.id, UPPER(A.value:\"value\") AS utm_campaign FROM MAPLEMONKTEST185_DB.Maplemonk.Shopify_gladful_ORDERS S, LATERAL FLATTEN(INPUT => note_attributes) A where LOWER(S.note_attributes) LIKE \'%gokwik%\' AND LOWER(A.value:\"name\") = \'utm_campaign\' ), utm_content_cte AS ( SELECT S.id ,UPPER(A.value:\"value\") AS utm_content FROM MAPLEMONKTEST185_DB.Maplemonk.Shopify_gladful_ORDERS S, LATERAL FLATTEN(INPUT => note_attributes) A where LOWER(S.note_attributes) LIKE \'%gokwik%\' AND LOWER(A.value:\"name\") = \'utm_content\' ) SELECT S.id, S.tags, source_cte.utm_source AS GOKWIK_UTM_SOURCE, medium_cte.utm_medium AS GOKWIK_UTM_MEDIUM, campaign_cte.utm_campaign AS GOKWIK_UTM_CAMPAIGN, content_cte.utm_content AS GOKWIK_UTM_CONTENT FROM MAPLEMONKTEST185_DB.Maplemonk.Shopify_gladful_ORDERS S JOIN utm_source_cte source_cte ON S.id = source_cte.id JOIN utm_medium_cte medium_cte ON S.id = medium_cte.id JOIN utm_campaign_cte campaign_cte ON S.id = campaign_cte.id JOIN utm_content_cte content_cte ON S.id = content_cte.id WHERE LOWER(S.note_attributes) LIKE \'%gokwik%\' AND source_cte.utm_source IS NOT NULL AND medium_cte.utm_medium IS NOT NULL AND campaign_cte.utm_campaign IS NOT NULL AND content_cte.utm_content IS NOT NULL ) Select GO_KWIK.* ,coalesce(Upper(UTM_MAPPING.CHANNEL), upper(GOKWIK_UTM_SOURCE),\'DIRECT\') as GOKWIK_MAPPED_CHANNEL ,coalesce(Upper(UTM_MAPPING.CHANNEL), upper(GOKWIK_UTM_SOURCE),\'DIRECT\') as GOKWIK_MAPPED_SOURCE from GO_KWIK left join (select * from (select * , row_number() over (partition by lower(utm_source) order by 1) rw from MAPLEMONKTEST185_DB.Maplemonk.UTM_MAPPING) where rw=1 and utm_source is not null ) UTM_MAPPING on lower(GO_KWIK.GOKWIK_UTM_SOURCE) = lower(UTM_MAPPING.utm_source) ; CREATE OR REPLACE TABLE MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_customers AS select *,\'Shopify_gladful\' AS Shop_Name from MAPLEMONKTEST185_DB.Maplemonk.Shopify_gladful_CUSTOMERS ; create or replace table MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_orders as select AO.* ,UPPER(ShopifyQL.shopifyql_mapped_channel) shopifyql_mapped_channel ,UPPER(ShopifyQL.shopifyql_mapped_source) shopifyql_mapped_source ,UPPER(ShopifyQL.FIRSTVISIT_UTM_SOURCE) Shopifyql_FIRSTVISIT_UTM_SOURCE ,UPPER(ShopifyQL.FirstVisit_UTM_Campaign) Shopifyql_FIRSTVISIT_UTM_CAMPAIGN ,upper(ShopifyQL.LastVisit_UTM_Campaign) Shopifyql_LAST_VISIT_UTM_CAMPAIGN ,UPPER(ShopifyQL.LAST_MOMENT_UTM_SOURCE) Shopifyql_LAST_MOMENT_UTM_SOURCE ,UPPER(ShopifyQL.LastVisit_NON_UTM_Source) Shopifyql_LAST_VISIT_NON_UTM_SOURCE ,UPPER(ShopifyQL.LAST_MOMENT_UTM_MEDIUM) Shopifyql_LAST_MOMENT_UTM_MEDIUM ,UPPER(ShopifyQL.FIRSTVISIT_UTM_MEDIUM) Shopifyql_FIRSTVISIT_UTM_MEDIUM ,div0(ShopifyQL.MOMENTS_COUNT,count(1) over (partition by AO.name order by 1)) MOMENTS_COUNT ,div0(ShopifyQL.DAYSTOCONVERT,count(1) over (partition by AO.name order by 1)) DAYSTOCONVERT ,UPPER(GOKWIK.GOKWIK_UTM_SOURCE) GOKWIK_UTM_SOURCE ,UPPER(GOKWIK.GOKWIK_MAPPED_CHANNEL) GOKWIK_MAPPED_CHANNEL ,UPPER(GOKWIK.GOKWIK_MAPPED_SOURCE) GOKWIK_MAPPED_SOURCE ,UPPER(Referrer_Name) Referrer_Name ,UPPER(GOKWIK.GOKWIK_UTM_CAMPAIGN) GOKWIK_UTM_CAMPAIGN ,UPPER(coalesce(LastVisit_UTM_Campaign,GOKWIK_UTM_CAMPAIGN)) FINAL_UTM_CAMPAIGN ,Upper(coalesce(shopifyql_mapped_source,GOKWIK_MAPPED_SOURCE,ShopifyQL.ShopifyQL_Unmapped_Last_Source,\'Direct\')) FINAL_UTM_SOURCE ,Upper(coalesce(shopifyql_mapped_channel,GOKWIK_MAPPED_CHANNEL,ShopifyQL.ShopifyQL_Unmapped_Last_Source,\'Direct\')) FINAL_UTM_CHANNEL from (select * ,case when lower(tags) like \'%cred%\' then \'CRED_Shopify_gladful\' else \'Shopify_Shopify_gladful\' end AS Shop_Name from MAPLEMONKTEST185_DB.Maplemonk.Shopify_gladful_ORDERS) AO left join MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_Shopify_UTM_Parameters ShopifyQL on AO.name = ShopifyQL.name left join MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_GOKWIK_SOURCE GOKWIK on AO.ID = GOKWIK.ID ; ALTER TABLE MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_orders RENAME COLUMN _AIRBYTE_Shopify_gladful_ORDERS_HASHID to _AIRBYTE_ORDERS_HASHID; CREATE OR REPLACE TABLE MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_products AS select *,\'Shopify_gladful\' AS Shop_Name from MAPLEMONKTEST185_DB.Maplemonk.Shopify_gladful_PRODUCTS ; ALTER TABLE MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_products RENAME COLUMN _AIRBYTE_Shopify_gladful_PRODUCTS_HASHID to _AIRBYTE_PRODUCTS_HASHID; CREATE OR REPLACE TABLE MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_products_variants AS select *,\'Shopify_gladful\' AS Shop_Name from MAPLEMONKTEST185_DB.Maplemonk.Shopify_gladful_PRODUCTS_VARIANTS ; ALTER TABLE MAPLEMONKTEST185_DB.Maplemonk.SHOPIFY_ALL_PRODUCTS_VARIANTS RENAME COLUMN _AIRBYTE_Shopify_gladful_PRODUCTS_HASHID to _AIRBYTE_PRODUCTS_HASHID; CREATE OR REPLACE TABLE MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_customers_addresses AS select *,\'Shopify_gladful\' AS Shop_Name from MAPLEMONKTEST185_DB.Maplemonk.Shopify_gladful_CUSTOMERS_ADDRESSES ; CREATE OR REPLACE TABLE MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_orders_items_discount AS SELECT order_id, LINE_ITEM_ID, SUM(B.VALUE:amount::FLOAT) AS DISCOUNT FROM( SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:discount_allocations AS discount_allocations FROM MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A)x,LATERAL FLATTEN (INPUT => x.discount_allocations)B GROUP BY ORDER_ID, LINE_ITEM_ID; CREATE OR REPLACE TABLE MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_orders_items_tax AS SELECT order_id, LINE_ITEM_ID, SUM(B.VALUE:price::FLOAT) AS TAX, SUM(B.VALUE:rate::float) as Tax_Rate FROM( SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:tax_lines AS tax_lines FROM MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A)x,LATERAL FLATTEN (INPUT => x.tax_lines)B GROUP BY order_id, LINE_ITEM_ID; CREATE OR REPLACE TABLE MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_Refunds AS WITH refund_line_items AS ( SELECT refunds.value:order_id::STRING AS order_id, line_items.value:line_item_id::string as LINE_ITEM_ID, try_to_timestamp(replace(refunds.value:\"created_at\",\'\"\',\'\')) AS refund_date, sum(ifnull(line_items.value:quantity::INT,0)) AS refund_quantity, sum(ifnull(line_items.value:subtotal::FLOAT,0)) AS refund_subtotal FROM MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_orders, LATERAL FLATTEN(input => Shopify_All_orders.refunds) refunds, LATERAL FLATTEN(input => refunds.value:refund_line_items) line_items group by refunds.value:order_id::STRING, line_items.value:line_item_id::string, try_to_timestamp(replace(refunds.value:\"created_at\",\'\"\',\'\')) ), order_adjustments AS ( SELECT order_adj.value:order_id::STRING AS order_id, try_to_timestamp(replace(refunds.value:\"created_at\",\'\"\',\'\')) adjustment_date, sum(ifnull(order_adj.value:amount::FLOAT,0)) AS adjustment FROM MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_orders, LATERAL FLATTEN(input => Shopify_All_orders.refunds) refunds, LATERAL FLATTEN(input => refunds.value:order_adjustments) order_adj group by order_adj.value:order_id::STRING, try_to_timestamp(replace(refunds.value:\"created_at\",\'\"\',\'\')) ), adj_refund as ( Select distinct order_id, line_item_id, date from ( select coalesce(rl.order_id,oa.order_id) order_id, rl.line_item_id, case when rl.refund_date = oa.adjustment_date then rl.refund_date else coalesce(oa.adjustment_date, rl.refund_date) end as date from refund_line_items rl full outer join order_adjustments oa on rl.order_id = oa.order_id ) ), refund_summary as ( select ar.order_id, ar.line_item_id, ar.date, ifnull(rl.refund_quantity,0) refund_quantity, ifnull(rl.refund_subtotal,0) refund_subtotal, ifnull(div0(oa.adjustment,count(1) over (partition by ar.order_id, ar.date)),0) as Adjustment_amount, (ifnull(rl.refund_subtotal,0) - ifnull(Adjustment_amount,0)) Total_Refund from adj_refund ar left join refund_line_items rl on ar.order_id = rl.order_id and ar.date = rl.refund_date and ar.line_item_id = rl.line_item_id left join order_adjustments oa on ar.order_id = oa.order_id and ar.date = oa.adjustment_date ), aggregate_summary AS ( SELECT order_id, line_item_id, date, sum(refund_quantity) AS Refund_Quantity, sum(Total_Refund) AS Refund_Amount, sum(Adjustment_amount) AS Adjustment_Amount, sum(refund_subtotal) AS Refund_Before_Adjustment FROM refund_summary GROUP BY order_id, line_item_id, date ) SELECT asum.order_id, asum.line_item_id, sum(asum.Refund_Quantity) Quantity, sum(asum.Refund_Amount) Amount, sum(asum.Adjustment_Amount) Adjustment_Amount, sum(asum.Refund_Before_Adjustment) Refund_Before_Adjustment, ARRAY_AGG( Object_construct( \'Refund_Date\', asum.date, \'Refund_Quantity\', ifnull(to_varchar(CAST(asum.Refund_Quantity AS DECIMAL(38,2))), \'0\'), \'Adjustment_Amount\', ifnull(to_varchar(CAST(asum.Adjustment_Amount AS DECIMAL(38,2))), \'0\'), \'Refund_Amount\', ifnull(to_varchar(CAST(asum.Refund_Amount AS DECIMAL(38,2))), \'0\') ) ) AS Refund_Details FROM aggregate_summary asum GROUP BY asum.order_id, asum.line_item_id; CREATE OR REPLACE TABLE MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_orders_fulfillment AS SELECT A.value:order_id AS order_id, B.value:id Line_Item_ID, replace(A.VALUE:tracking_number,\'\"\',\'\') as AWB, Upper(replace(A.VALUE:tracking_company,\'\"\',\'\')) as Courier, Upper(replace(A.VALUE:shipment_status,\'\"\',\'\')) as Shipping_status, replace(A.VALUE:updated_at,\'\"\',\'\') as shipping_status_update_date, replace(A.VALUE:tracking_url,\'\"\',\'\') as tracking_url, replace(A.VALUE:created_at,\'\"\',\'\') as Shipping_created_at FROM MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_orders, LATERAL FLATTEN (INPUT => fulfillments)A,LATERAL FLATTEN (INPUT => A.value:line_items)B; create or replace table MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_orders_shipping_lines_discount as SELECT id as Order_ID, SUM(ifnull(CAST(da.value:amount AS FLOAT),0)) AS total_shipping_discount FROM MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_orders, LATERAL FLATTEN(input => shipping_lines) AS sl, LATERAL FLATTEN(input => sl.value:discount_allocations) AS da group by 1; CREATE OR REPLACE TABLE MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_orders_items AS WITH CTE AS (SELECT SHOP_NAME, ID::VARCHAR(16777216) AS ORDER_ID, NAME AS ORDER_NAME, CUSTOMER, replace(customer:default_address:name,\'\"\',\'\') NAME, PHONE, EMAIL, tags, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:sku::STRING AS SKU, A.VALUE:product_id::STRING AS PRODUCT_ID, upper(A.VALUE:title::STRING) AS PRODUCT_NAME, CURRENCY, CASE WHEN cancelled_at IS NOT NULL THEN \'CANCELLED\' ELSE \'SHOPIFY_PROCESSED\' END AS order_status, CREATED_AT::DATETIME AS order_timestamp, A.VALUE:price::FLOAT * A.VALUE:quantity::FLOAT AS LINE_ITEM_SALES, (TOTAL_SHIPPING_PRICE_SET:presentment_money:amount::FLOAT/ COUNT(ORDER_ID) OVER(PARTITION BY ORDER_ID ORDER BY ORDER_ID)) AS SHIPPING_PRICE, A.value:price/(1+A.value:tax_lines:rate) PRICE_BEFORE_TAX, A.VALUE:quantity::FLOAT as QUANTITY, \'SHOPIFY\' AS Source, MOMENTS_COUNT, DAYSTOCONVERT, Shopifyql_LAST_VISIT_UTM_CAMPAIGN, Shopifyql_FIRSTVISIT_UTM_CAMPAIGN, SHOPIFYQL_FIRSTVISIT_UTM_SOURCE, SHOPIFYQL_MAPPED_SOURCE, SHOPIFYQL_MAPPED_CHANNEL, SHOPIFYQL_LAST_MOMENT_UTM_SOURCE, Shopifyql_LAST_VISIT_NON_UTM_SOURCE, Shopifyql_LAST_MOMENT_UTM_MEDIUM, Shopifyql_FIRSTVISIT_UTM_MEDIUM, GOKWIK_UTM_SOURCE, GOKWIK_UTM_MEDIUM, GOKWIK_UTM_CAMPAIGN, shopifyql_mapped_channel, shopifyql_mapped_source, GOKWIK_MAPPED_CHANNEL, GOKWIK_MAPPED_SOURCE, GOKWIK_UTM_CONTENT, FINAL_UTM_CAMPAIGN, FINAL_UTM_CHANNEL, FINAL_UTM_SOURCE, Referrer_Name, GOKWIK_MAPPED_SOURCE, GOKWIK_MAPPED_CHANNEL, Upper(GATEWAY) as GATEWAY, SHIPPING_ADDRESS FROM MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A) SELECT CTE.*, IFNULL(T.TAX_RATE,0) AS TAX_RATE, IFNULL(D.DISCOUNT,0) AS DISCOUNT, CTE.LINE_ITEM_SALES AS GROSS_SALES_AFTER_TAX, CTE.LINE_ITEM_SALES/(1+IFNULL(T.TAX_RATE,0)) AS GROSS_SALES_BEFORE_TAX, CASE when IFNULL(T.TAX,0)=0 then IFNULL(D.DISCOUNT,0) else IFNULL(D.DISCOUNT,0)/(1+IFNULL(T.TAX_RATE,0)) end AS DISCOUNT_BEFORE_TAX, CASE when IFNULL(T.TAX,0)=0 then CTE.LINE_ITEM_SALES - IFNULL(D.DISCOUNT,0) else CTE.LINE_ITEM_SALES/(1+IFNULL(T.TAX_RATE,0)) - IFNULL(D.DISCOUNT,0)/(1+IFNULL(T.TAX_RATE,0)) end AS NET_SALES_BEFORE_TAX, IFNULL(T.TAX,0) AS TAX, case when IFNULL(T.TAX,0)=0 then (CTE.LINE_ITEM_SALES) - IFNULL(D.DISCOUNT,0) - div0(ifnull(SD.TOTAL_SHIPPING_DISCOUNT,0),count(1) over (partition by CTE.order_id)) + IFNULL(T.TAX,0) + CTE.SHIPPING_PRICE else (CTE.LINE_ITEM_SALES/(1+IFNULL(T.TAX_RATE,0))) - (IFNULL(D.DISCOUNT,0)/(1+IFNULL(T.TAX_RATE,0))) - div0(ifnull(SD.TOTAL_SHIPPING_DISCOUNT,0),count(1) over (partition by CTE.order_id)) + IFNULL(T.TAX,0) + CTE.SHIPPING_PRICE end AS TOTAL_SALES, CASE WHEN R.QUANTITY IS NOT NULL THEN 1 ELSE 0 END AS IS_REFUND, R.REFUND_DETAILS, R.QUANTITY Refund_Quantity, R.AMOUNT Refund_Value, F.AWB, F.Courier, F.Shipping_status, F.shipping_status_update_date, F.tracking_url, F.Shipping_created_at, div0(ifnull(SD.TOTAL_SHIPPING_DISCOUNT,0),count(1) over (partition by CTE.order_id)) TOTAL_SHIPPING_DISCOUNT FROM CTE LEFT JOIN MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_orders_items_tax T ON CTE.ORDER_ID = T.ORDER_ID AND CTE.LINE_ITEM_ID = T.LINE_ITEM_ID LEFT JOIN MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_orders_items_discount D ON CTE.ORDER_ID = D.ORDER_ID AND CTE.LINE_ITEM_ID = D.LINE_ITEM_ID LEFT JOIN MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_Refunds R ON CTE.ORDER_ID = R.ORDER_ID AND CTE.LINE_ITEM_ID = R.LINE_ITEM_ID LEFT JOIN MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_orders_fulfillment F ON CTE.ORDER_ID = F.ORDER_ID AND CTE.LINE_ITEM_ID = F.LINE_ITEM_ID LEFT JOIN MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_orders_shipping_lines_discount SD ON CTE.ORDER_ID = SD.ORDER_ID; CREATE OR REPLACE TABLE MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS_INTERMEDIATE AS SELECT O.SHOP_NAME, \'SHOPIFY\' as marketplace, O.ORDER_ID, O.ORDER_NAME, O.CUSTOMER:id::int AS customer_id, O.Name, O.EMAIL, O.PHONE, O.Tags, O.LINE_ITEM_ID, O.SKU, O.PRODUCT_ID, O.CURRENCY, O.IS_REFUND, upper(replace(O.shipping_address:city,\'\"\',\'\')) as shipping_city, upper(replace(O.shipping_address:province,\'\"\',\'\')) as shipping_State, CASE WHEN CD.city IS NULL OR CD.city = \'\' THEN \'NA\' ELSE upper(CD.city) END AS city, CASE WHEN CD.province IS NULL OR CD.province = \'\' THEN \'NA\' ELSE upper(CD.province) END AS state, CASE WHEN P.title = \'\' THEN \'NA\' ELSE upper(P.title) END AS product_name, CASE WHEN P.product_type = \'\' THEN \'NA\' ELSE upper(P.product_type) END AS category, upper(O.order_status) order_status, O.order_timestamp, O.LINE_ITEM_SALES, O.QUANTITY, O.Refund_Quantity, O.Refund_Value, O.TAX, O.TAX_RATE, O.DISCOUNT, O.DISCOUNT_BEFORE_TAX, O.GROSS_SALES_AFTER_TAX, O.GROSS_SALES_BEFORE_TAX, O.NET_SALES_BEFORE_TAX, (ifnull(O.SHIPPING_PRICE,0)-ifnull(O.TOTAL_SHIPPING_DISCOUNT,0)) as SHIPPING_PRICE, ifnull(O.TOTAL_SALES,0) TOTAL_SALES, O.Source, O.MOMENTS_COUNT, O.DAYSTOCONVERT, O.SHOPIFYQL_FIRSTVISIT_UTM_SOURCE, O.SHOPIFYQL_MAPPED_SOURCE, O.SHOPIFYQL_MAPPED_CHANNEL, O.SHOPIFYQL_LAST_MOMENT_UTM_SOURCE, O.Shopifyql_LAST_VISIT_NON_UTM_SOURCE, O.Shopifyql_FIRSTVISIT_UTM_MEDIUM, O.Shopifyql_LAST_MOMENT_UTM_MEDIUM, O.FINAL_UTM_CHANNEL, O.FINAL_UTM_SOURCE, O.Referrer_Name, O.GOKWIK_MAPPED_SOURCE, O.GOKWIK_MAPPED_CHANNEL, O.REFUND_DETAILS, O.AWB, upper(O.Courier) Courier, upper(O.Shipping_status) SHIPPING_STATUS, O.shipping_status_update_date, O.tracking_url, O.Shipping_created_at, O.GATEWAY FROM MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_orders_items O LEFT JOIN MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_products P ON O.PRODUCT_ID = P.id LEFT JOIN(SELECT customer_id, city, province, row_number() OVER ( partition BY customer_id ORDER BY id DESC) rowid FROM MAPLEMONKTEST185_DB.Maplemonk.Shopify_All_customers_addresses) AS CD ON O.CUSTOMER:id::int = CD.customer_id AND CD.rowid = 1; CREATE OR REPLACE TABLE MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS_TEMP_Category as select fi.* ,fi.SKU AS SKU_CODE ,Upper(coalesce(p.name,fi.product_name)) as PRODUCT_NAME_Final ,coalesce(Upper(p.CATEGORY),upper(fi.category)) AS Product_Category ,Upper(p.sub_category) as Product_Sub_Category from MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS_INTERMEDIATE fi left join (select * from (select skucode, name, category, sub_category, row_number() over (partition by skucode order by 1) rw from MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_sku_master) where rw = 1 ) p on fi.sku = p.skucode; CREATE OR REPLACE TABLE MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS_INTERMEDIATE AS SELECT * FROM MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS_TEMP_Category; create or replace table MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_Shopify_Final_customerID as with new_phone_numbers as ( select phone, contact_num ,19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(replace(phone,\' \',\'\'), \'[^a-zA-Z0-9]+\'),10) as contact_num, phone from MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS_INTERMEDIATE ) a ), int as ( select contact_num,email,coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email,maple_monk_id,19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email,maple_monk_id from ( select replace(phone,\' \',\'\') as contact_num,email from MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS_INTERMEDIATE ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as Shopify_customer_id_final , min(ORDER_TIMESTAMP::date) over(partition by Shopify_customer_id_final) as shopify_acquisition_date , min(case when lower(order_status) not in (\'cancelled\') then ORDER_TIMESTAMP::date end) over(partition by Shopify_customer_id_final) as shopify_first_complete_order_date , m.* from ( select c.maple_monk_id as maple_monk_id_phone, o.* from MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS_INTERMEDIATE o left join (select * from (select contact_num phone,maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_Shopify_Final_customerID) where magic =1 )c on c.phone = right(regexp_replace(o.phone, \'[^a-zA-Z0-9]+\'),10) )m left join (select distinct maple_monk_id, email from MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_Shopify_Final_customerID where contact_num is null )d on d.email = m.email ; ALTER TABLE MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS ADD COLUMN shopify_new_customer_flag varchar(50); ALTER TABLE MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS ADD COLUMN shopify_new_customer_flag_month varchar(50); ALTER TABLE MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS ADD COLUMN shopify_acquisition_product varchar(16777216); ALTER TABLE MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS ADD COLUMN shopify_acquisition_channel varchar(16777216); ALTER TABLE MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS ADD COLUMN shopify_acquisition_source varchar(16777216); UPDATE MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS AS A SET A.shopify_new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, Shopify_customer_id_final, ORDER_TIMESTAMP, CASE WHEN ORDER_TIMESTAMP::date = shopify_first_complete_order_date then \'New\' WHEN ORDER_TIMESTAMP::date < shopify_first_complete_order_date or shopify_first_complete_order_date is null THEN \'Yet to make completed order\' WHEN ORDER_TIMESTAMP::date > shopify_first_complete_order_date then \'Repeat\' END AS Flag FROM MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS )AS B WHERE A.order_id = B.order_id AND A.Shopify_customer_id_final = B.Shopify_customer_id_final AND A.ORDER_TIMESTAMP::date=B.ORDER_TIMESTAMP::Date; UPDATE MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS SET shopify_new_customer_flag = CASE WHEN shopify_new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' WHEN shopify_new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) in (\'cancelled\') end) THEN \'Yet to make completed order\' ELSE shopify_new_customer_flag END; UPDATE MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS AS A SET A.shopify_new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, shopify_customer_id_final, ORDER_TIMESTAMP::date Order_Date, CASE WHEN Last_day(ORDER_TIMESTAMP, \'month\') = Last_day(shopify_first_complete_order_date, \'month\') THEN \'New\' WHEN Last_day(ORDER_TIMESTAMP, \'month\') < Last_day(shopify_first_complete_order_date, \'month\') or shopify_acquisition_date is null THEN \'Yet to make completed order\' WHEN Last_day(order_date, \'month\') > Last_day(shopify_first_complete_order_date, \'month\') THEN \'Repeat\' END AS Flag FROM MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS )AS B WHERE A.order_id = B.order_id AND A.shopify_customer_id_final = B.shopify_customer_id_final; UPDATE MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS SET shopify_new_customer_flag_month = CASE WHEN shopify_new_customer_flag_month IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' ELSE shopify_new_customer_flag_month END; CREATE OR replace temporary TABLE MAPLEMONKTEST185_DB.Maplemonk.temp_source_1 AS SELECT DISTINCT shopify_customer_id_final, channel , source FROM ( SELECT DISTINCT shopify_customer_id_final, order_timestamp::date order_Date, FINAL_UTM_SOURCE as SOURCE, FINAL_UTM_CHANNEL as CHANNEL, Min(case when lower(order_status) not in (\'cancelled\') then order_timestamp::date end) OVER (partition BY shopify_customer_id_final) firstOrderdate FROM MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS ) res WHERE order_date=firstorderdate; UPDATE MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS AS a SET a.shopify_acquisition_channel=b.channel FROM MAPLEMONKTEST185_DB.Maplemonk.temp_source_1 b WHERE a.shopify_customer_id_final = b.shopify_customer_id_final; UPDATE MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS AS a SET a.shopify_acquisition_source=b.SOURCE FROM MAPLEMONKTEST185_DB.Maplemonk.temp_source_1 b WHERE a.shopify_customer_id_final = b.shopify_customer_id_final; CREATE OR replace temporary TABLE MAPLEMONKTEST185_DB.Maplemonk.temp_product_1 AS SELECT DISTINCT shopify_customer_id_final, product_name_final, Row_number() OVER (partition BY shopify_customer_id_final ORDER BY total_sales DESC) rowid FROM ( SELECT DISTINCT shopify_customer_id_final, order_timestamp::date order_date, product_name_final, TOTAL_SALES , Min(case when lower(order_status) not in (\'cancelled\') then order_timestamp::date end) OVER (partition BY shopify_customer_id_final) firstOrderdate FROM MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS )res WHERE order_date=firstorderdate; UPDATE MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS AS A SET A.shopify_acquisition_product=B.product_name_final FROM ( SELECT * FROM MAPLEMONKTEST185_DB.Maplemonk.temp_product_1 WHERE rowid=1)B WHERE A.shopify_customer_id_final = B.shopify_customer_id_final; ALTER TABLE MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS ADD COLUMN SHIPPING_TAX FLOAT; ALTER TABLE MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS ADD COLUMN SHIP_PROMOTION_DISCOUNT FLOAT; ALTER TABLE MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS ADD COLUMN GIFT_WRAP_PRICE FLOAT; ALTER TABLE MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS ADD COLUMN GIFT_WRAP_TAX FLOAT; ALTER TABLE MAPLEMONKTEST185_DB.Maplemonk.MAPLEMONKTEST185_DB_SHOPIFY_FACT_ITEMS MODIFY COLUMN ORDER_STATUS VARCHAR(100);",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from MAPLEMONKTEST185_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        