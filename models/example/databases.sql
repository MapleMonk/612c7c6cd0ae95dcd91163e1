{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE TABLE IF NOT EXISTS MAPLEMONK.UTM_MAPPING ( UTM_SOURCE STRING, UTM_MEDIUM STRING, SOURCE STRING, CHANNEL STRING ) ; CREATE TABLE IF NOT EXISTS MAPLEMONK.ZOUK_SKU_MASTER ( SKUCODE STRING, NAME STRING, CATEGORY STRING, SUB_CATEGORY STRING ) ; CREATE TABLE IF NOT EXISTS MAPLEMONK.SHOPIFY_ZOUKBAGS_UTM_PARAMETERS ( ID STRING, NAME STRING, CREATEDAT STRING, CUSTOMERJOURNEY JSON, CUSTOMERJOURNEYSUMMARY JSON, REFERRERURL STRING, REFERRALCODE STRING, LANDINGPAGEURL STRING, REFERRERDISPLAYTEXT STRING, LANDINGPAGEDISPLAYTEXT STRING ) ; create or replace table zouk-wh.maplemonk.zouk_Shopify_UTM_Parameters as select ShopifyQL.* ,upper(coalesce(UTM_MAPPING.CHANNEL,ShopifyQL.ShopifyQL_Unmapped_Last_Source, UTM_MAPPING_REF.CHANNEL)) as ShopifyQL_MAPPED_CHANNEL ,upper(coalesce(UTM_MAPPING.SOURCE,ShopifyQL.ShopifyQL_Unmapped_Last_Source,UTM_MAPPING_REF.SOURCE)) as ShopifyQL_MAPPED_SOURCE from (select * from ( select A.id ,A.name ,A.createdat ,cast (JSON_EXTRACT_SCALAR(A.customerjourneysummary,\'$.momentsCount\') as int) Moments_Count ,cast (JSON_EXTRACT_SCALAR(A.customerjourneysummary,\'$.daysToConversion\') as int) DaysToConvert ,cast (JSON_EXTRACT_SCALAR(A.customerjourneysummary,\'$.lastVisit.utmParameters.source\') as string) LastVisit_UTM_Source ,cast (JSON_EXTRACT_SCALAR(A.customerjourneysummary,\'$.lastVisit.utmParameters.medium\') as string) LastVisit_UTM_Medium ,cast (JSON_EXTRACT_SCALAR(A.customerjourneysummary,\'$.lastVisit.source\') as string) LastVisit_NON_UTM_Source ,cast (JSON_EXTRACT_SCALAR(A.customerjourneysummary,\'$.lastVisit.utmParameters.campaign\') as string) LastVisit_UTM_Campaign ,cast (JSON_EXTRACT_SCALAR(A.customerjourneysummary,\'$.firstVisit.utmParameters.source\') as string) FirstVisit_UTM_Source ,cast (JSON_EXTRACT_SCALAR(A.customerjourneysummary,\'$.firstVisit.utmParameters.medium\') as string) FirstVisit_UTM_Medium ,cast (JSON_EXTRACT_SCALAR(A.customerjourneysummary,\'$.firstVisit.utmParameters.campaign\') as string) FirstVisit_UTM_Campaign ,replace(JSON_EXTRACT_SCALAR(b,\'$.id\') ,\'gid://shopify/CustomerVisit/\',\'\') Moment_ID ,JSON_EXTRACT_SCALAR(b,\'$.utmParameters.source\') Last_Moment_UTM_Source ,JSON_EXTRACT_SCALAR(b,\'$.utmParameters.medium\') Last_Moment_UTM_Medium ,case when cast (JSON_EXTRACT_SCALAR(A.customerjourneysummary,\'$.momentsCount\') as int) >1 then cast (JSON_EXTRACT_SCALAR(A.customerjourneysummary,\'$.lastVisit.utmParameters.source\') as string) else cast (JSON_EXTRACT_SCALAR(A.customerjourneysummary,\'$.firstVisit.utmParameters.source\') as string) end CJSummary_utm_source ,referrerdisplaytext Referrer_Name ,customerjourneysummary ,customerjourney ,coalesce(JSON_EXTRACT_SCALAR(b,\'$.utmParameters.source\') ,cast (JSON_EXTRACT_SCALAR(A.customerjourneysummary,\'$.lastVisit.utmParameters.source\') as string) ,cast (JSON_EXTRACT_SCALAR(A.customerjourneysummary,\'$.lastVisit.source\') as string) ) ShopifyQL_Unmapped_Last_Source ,coalesce(JSON_EXTRACT_SCALAR(b,\'$.utmParameters.medium\') ,cast (JSON_EXTRACT_SCALAR(A.customerjourneysummary,\'$.lastVisit.utmParameters.medium\') as string) ) ShopifyQL_Unmapped_Last_Medium ,rank() over (partition by A.name order by replace(JSON_EXTRACT_SCALAR(b,\'$.id\') ,\'gid://shopify/CustomerVisit/\',\'\') desc) rw from zouk-wh.maplemonk.Shopify_zoukbags_UTM_Parameters A left join unnest(JSON_EXTRACT_ARRAY(JSON_EXTRACT(customerjourney, \'$.moments\'))) b ) where rw=1 ) ShopifyQL left join (select * from (select * , row_number() over (partition by lower(concat(ifnull(utm_source,\'\'),ifnull(utm_medium,\'\'))) order by 1) rw from zouk-wh.maplemonk.UTM_MAPPING) where rw=1 and lower(concat(ifnull(utm_source,\'\'),ifnull(utm_medium,\'\'))) is not null ) UTM_MAPPING on lower(concat(ifnull(ShopifyQL.ShopifyQL_Unmapped_Last_Source,\'\'),ifnull(ShopifyQL_Unmapped_Last_Medium,\'\'))) = lower(concat(ifnull(UTM_MAPPING.utm_source,\'\'),ifnull(UTM_MAPPING.utm_medium,\'\'))) left join (select * from (select * , row_number() over (partition by lower(utm_source) order by 1) rw from zouk-wh.maplemonk.utm_mapping) where rw=1 and UTM_Source is not null ) UTM_MAPPING_REF on lower(ShopifyQL.referrer_name) = lower(UTM_MAPPING_REF.utm_source) ; create or replace table zouk-wh.maplemonk.zouk_CHECKOUT_SOURCE as With CHECKOUT as ( WITH utm_source_cte AS ( SELECT S.id, UPPER(JSON_EXTRACT_SCALAR(A,\'$.value\') ) AS utm_source FROM zouk-wh.maplemonk.Shopify_zoukbags_ORDERS S left join UNNEST(S.note_attributes) AS A WHERE (LOWER(ARRAY_TO_STRING(note_attributes,\',\')) LIKE \'%gokwik%\' OR LOWER(ARRAY_TO_STRING(note_attributes,\',\')) LIKE \'%shopflo%\' OR LOWER(S.tags) LIKE \'%gokwik%\' OR LOWER(S.tags) LIKE \'%shopflo%\') AND LOWER(JSON_EXTRACT_SCALAR(A,\'$.name\')) = \'utm_source\' ), utm_medium_cte AS ( SELECT S.id, UPPER(JSON_EXTRACT_SCALAR(A,\'$.value\') ) AS utm_medium FROM zouk-wh.maplemonk.Shopify_zoukbags_ORDERS S left join UNNEST(S.note_attributes) AS A WHERE (LOWER(ARRAY_TO_STRING(note_attributes,\',\')) LIKE \'%gokwik%\' OR LOWER(ARRAY_TO_STRING(note_attributes,\',\')) LIKE \'%shopflo%\' OR LOWER(S.tags) LIKE \'%gokwik%\' OR LOWER(S.tags) LIKE \'%shopflo%\') AND LOWER(JSON_EXTRACT_SCALAR(A,\'$.name\')) = \'utm_medium\' ), utm_campaign_cte AS ( SELECT S.id, UPPER(JSON_EXTRACT_SCALAR(A,\'$.value\') ) AS utm_campaign FROM zouk-wh.maplemonk.Shopify_zoukbags_ORDERS S left join UNNEST(S.note_attributes) AS A WHERE (LOWER(ARRAY_TO_STRING(note_attributes,\',\')) LIKE \'%gokwik%\' OR LOWER(ARRAY_TO_STRING(note_attributes,\',\')) LIKE \'%shopflo%\' OR LOWER(S.tags) LIKE \'%gokwik%\' OR LOWER(S.tags) LIKE \'%shopflo%\') AND LOWER(JSON_EXTRACT_SCALAR(A,\'$.name\')) = \'utm_campaign\' ), utm_content_cte AS ( SELECT S.id, UPPER(JSON_EXTRACT_SCALAR(A,\'$.value\') ) AS utm_content FROM zouk-wh.maplemonk.Shopify_zoukbags_ORDERS S left join UNNEST(S.note_attributes) AS A WHERE (LOWER(ARRAY_TO_STRING(note_attributes,\',\')) LIKE \'%gokwik%\' OR LOWER(ARRAY_TO_STRING(note_attributes,\',\')) LIKE \'%shopflo%\' OR LOWER(S.tags) LIKE \'%gokwik%\' OR LOWER(S.tags) LIKE \'%shopflo%\') AND LOWER(JSON_EXTRACT_SCALAR(A,\'$.name\')) = \'utm_content\' ) SELECT S.id, S.tags, source_cte.utm_source AS CHECKOUT_UTM_SOURCE, medium_cte.utm_medium AS CHECKOUT_UTM_MEDIUM, campaign_cte.utm_campaign AS CHECKOUT_UTM_CAMPAIGN, content_cte.utm_content AS CHECKOUT_UTM_CONTENT FROM zouk-wh.maplemonk.Shopify_zoukbags_ORDERS S LEFT JOIN utm_source_cte source_cte ON S.id = source_cte.id LEFT JOIN utm_medium_cte medium_cte ON S.id = medium_cte.id LEFT JOIN utm_campaign_cte campaign_cte ON S.id = campaign_cte.id LEFT JOIN utm_content_cte content_cte ON S.id = content_cte.id where (LOWER(ARRAY_TO_STRING(note_attributes,\',\')) LIKE \'%gokwik%\' OR LOWER(ARRAY_TO_STRING(note_attributes,\',\')) LIKE \'%shopflo%\' OR LOWER(S.tags) LIKE \'%gokwik%\' OR LOWER(S.tags) LIKE \'%shopflo%\') ) Select CHECKOUT.* ,coalesce(Upper(UTM_MAPPING.CHANNEL), upper(CHECKOUT_UTM_SOURCE),\'DIRECT\') as CHECKOUT_MAPPED_CHANNEL ,coalesce(Upper(UTM_MAPPING.CHANNEL), upper(CHECKOUT_UTM_SOURCE),\'DIRECT\') as CHECKOUT_MAPPED_SOURCE from CHECKOUT left join (select * from (select * , row_number() over (partition by lower(concat(ifnull(utm_source,\'\'),ifnull(utm_medium,\'\'))) order by 1) rw from zouk-wh.maplemonk.UTM_MAPPING) where rw=1 and lower(concat(ifnull(utm_source,\'\'),ifnull(utm_medium,\'\'))) is not null ) UTM_MAPPING on lower(concat(ifnull(CHECKOUT.CHECKOUT_UTM_SOURCE,\'\'),ifnull(CHECKOUT_UTM_MEDIUM,\'\'))) = lower(concat(ifnull(UTM_MAPPING.utm_source,\'\'),ifnull(UTM_MAPPING.utm_medium,\'\'))) ; CREATE OR REPLACE TABLE zouk-wh.maplemonk.Shopify_All_customers AS select *,\'Shopify_zoukbags\' AS Shop_Name from zouk-wh.maplemonk.Shopify_zoukbags_CUSTOMERS ; create or replace table zouk-wh.maplemonk.Shopify_All_orders as select AO.* ,UPPER(ShopifyQL.shopifyql_mapped_channel) shopifyql_mapped_channel ,UPPER(ShopifyQL.shopifyql_mapped_source) shopifyql_mapped_source ,UPPER(ShopifyQL.FIRSTVISIT_UTM_SOURCE) Shopifyql_FIRSTVISIT_UTM_SOURCE ,UPPER(ShopifyQL.FirstVisit_UTM_Campaign) Shopifyql_FIRSTVISIT_UTM_CAMPAIGN ,upper(ShopifyQL.LastVisit_UTM_Campaign) Shopifyql_LAST_VISIT_UTM_CAMPAIGN ,UPPER(ShopifyQL.LAST_MOMENT_UTM_SOURCE) Shopifyql_LAST_MOMENT_UTM_SOURCE ,UPPER(ShopifyQL.LastVisit_NON_UTM_Source) Shopifyql_LAST_VISIT_NON_UTM_SOURCE ,UPPER(ShopifyQL.LAST_MOMENT_UTM_MEDIUM) Shopifyql_LAST_MOMENT_UTM_MEDIUM ,UPPER(ShopifyQL.FIRSTVISIT_UTM_MEDIUM) Shopifyql_FIRSTVISIT_UTM_MEDIUM ,safe_divide(ShopifyQL.MOMENTS_COUNT,count(1) over (partition by AO.name order by 1)) MOMENTS_COUNT ,safe_divide(ShopifyQL.DAYSTOCONVERT,count(1) over (partition by AO.name order by 1)) DAYSTOCONVERT ,UPPER(CHECKOUT.CHECKOUT_UTM_SOURCE) CHECKOUT_UTM_SOURCE ,UPPER(CHECKOUT.CHECKOUT_UTM_MEDIUM) CHECKOUT_UTM_MEDIUM ,UPPER(CHECKOUT.CHECKOUT_UTM_CONTENT) CHECKOUT_UTM_CONTENT ,UPPER(CHECKOUT.CHECKOUT_MAPPED_CHANNEL) CHECKOUT_MAPPED_CHANNEL ,UPPER(CHECKOUT.CHECKOUT_MAPPED_SOURCE) CHECKOUT_MAPPED_SOURCE ,UPPER(Referrer_Name) Referrer_Name ,UPPER(CHECKOUT.CHECKOUT_UTM_CAMPAIGN) CHECKOUT_UTM_CAMPAIGN ,UPPER(coalesce(LastVisit_UTM_Campaign,CHECKOUT_UTM_CAMPAIGN)) FINAL_UTM_CAMPAIGN ,Upper(coalesce(shopifyql_mapped_source,CHECKOUT_MAPPED_SOURCE,ShopifyQL.ShopifyQL_Unmapped_Last_Source,\'Direct\')) FINAL_UTM_SOURCE ,Upper(coalesce(shopifyql_mapped_channel,CHECKOUT_MAPPED_CHANNEL,ShopifyQL.ShopifyQL_Unmapped_Last_Source,\'Direct\')) FINAL_UTM_CHANNEL ,coalesce(JSON_EXTRACT_SCALAR(AO.SHIPPING_ADDRESS,\'$.zip\'),JSON_EXTRACT_SCALAR(AO.billing_address,\'$.zip\')) as pincode from (select *,\'Shopify_zoukbags\' AS Shop_Name from zouk-wh.maplemonk.Shopify_zoukbags_ORDERS) AO left join zouk-wh.maplemonk.zouk_Shopify_UTM_Parameters ShopifyQL on AO.name = ShopifyQL.name left join zouk-wh.maplemonk.zouk_CHECKOUT_SOURCE CHECKOUT on AO.ID = CHECKOUT.ID ; ALTER TABLE zouk-wh.maplemonk.Shopify_All_orders RENAME COLUMN _AIRBYTE_Shopify_zoukbags_ORDERS_HASHID to _AIRBYTE_ORDERS_HASHID; CREATE OR REPLACE TABLE zouk-wh.maplemonk.Shopify_All_products AS select *,\'Shopify_zoukbags\' AS Shop_Name from zouk-wh.maplemonk.Shopify_zoukbags_PRODUCTS ; ALTER TABLE zouk-wh.maplemonk.Shopify_All_products RENAME COLUMN _AIRBYTE_Shopify_zoukbags_PRODUCTS_HASHID to _AIRBYTE_PRODUCTS_HASHID; CREATE OR REPLACE TABLE zouk-wh.maplemonk.Shopify_All_products_variants AS select *,\'Shopify_zoukbags\' AS Shop_Name from zouk-wh.maplemonk.Shopify_zoukbags_PRODUCTS_VARIANTS ; ALTER TABLE zouk-wh.maplemonk.SHOPIFY_ALL_PRODUCTS_VARIANTS RENAME COLUMN _AIRBYTE_Shopify_zoukbags_PRODUCTS_HASHID to _AIRBYTE_PRODUCTS_HASHID; CREATE OR REPLACE TABLE zouk-wh.maplemonk.Shopify_All_customers_addresses AS select *,\'Shopify_zoukbags\' AS Shop_Name from zouk-wh.maplemonk.Shopify_zoukbags_CUSTOMERS_ADDRESSES ; CREATE OR REPLACE TABLE zouk-wh.maplemonk.Shopify_All_orders_items_discount AS SELECT order_id, LINE_ITEM_ID, SUM(cast(JSON_EXTRACT_SCALAR(c,\'$.amount\')as FLOAT64)) AS DISCOUNT from ( SELECT A.id AS order_id, JSON_EXTRACT_SCALAR(B,\'$.id\') AS LINE_ITEM_ID, JSON_EXTRACT_ARRAY(B,\'$.discount_allocations\') AS discount_allocations FROM zouk-wh.maplemonk.Shopify_All_orders A left join unnest(A.LINE_ITEMS)B) left join unnest(discount_allocations) C GROUP BY ORDER_ID, LINE_ITEM_ID; CREATE OR REPLACE TABLE zouk-wh.maplemonk.Shopify_All_orders_items_tax AS SELECT order_id, LINE_ITEM_ID, SUM(cast(JSON_EXTRACT_SCALAR(c,\'$.price\')as FLOAT64)) AS tax, SUM(cast(JSON_EXTRACT_SCALAR(c,\'$.rate\')as FLOAT64)) AS Tax_Rate FROM( SELECT A.id AS order_id, JSON_EXTRACT_SCALAR(B,\'$.id\') AS LINE_ITEM_ID, JSON_EXTRACT_ARRAY(B,\'$.tax_lines\') AS tax_lines FROM zouk-wh.maplemonk.Shopify_All_orders A left join unnest(A.LINE_ITEMS)B )x left join unnest(x.tax_lines) C group by order_id,LINE_ITEM_ID; CREATE OR REPLACE TABLE zouk-wh.maplemonk.Shopify_All_Refunds AS WITH refund_line_items AS ( SELECT JSON_EXTRACT_SCALAR(refunds,\'$.order_id\') as order_id, JSON_EXTRACT_SCALAR(line_items,\'$.line_item_id\') as LINE_ITEM_ID, JSON_EXTRACT_SCALAR(line_items,\'$.created_at\') as refund_date, sum(cast(JSON_EXTRACT_SCALAR(line_items,\'$.quantity\') as float64)) as refund_quantity, sum(cast(JSON_EXTRACT_SCALAR(line_items,\'$.subtotal\') as float64 )) as refund_subtotal, FROM zouk-wh.maplemonk.Shopify_All_orders A left join unnest(A.refunds) refunds left join unnest(JSON_EXTRACT_ARRAY(JSON_EXTRACT(refunds, \'$.refund_line_items\'))) line_items where JSON_EXTRACT_SCALAR(refunds,\'$.order_id\') is not null group by 1,2,3 ), order_adjustments AS ( SELECT JSON_EXTRACT_SCALAR(order_adj,\'$.order_id\') AS order_id, JSON_EXTRACT_SCALAR(refunds, \'$.created_at\') AS adjustment_date, sum(cast(JSON_EXTRACT_SCALAR(order_adj,\'$.amount\') as float64 )) adjustment FROM zouk-wh.maplemonk.Shopify_All_orders A left join unnest(A.refunds) refunds left join unnest(JSON_EXTRACT_ARRAY(JSON_EXTRACT(refunds, \'$.order_adjustments\'))) order_adj group by 1,2 ), adj_refund as ( Select distinct order_id, line_item_id, date from ( select coalesce(rl.order_id,oa.order_id) order_id, rl.line_item_id, case when rl.refund_date = oa.adjustment_date then rl.refund_date else coalesce(oa.adjustment_date, rl.refund_date) end as date from refund_line_items rl full outer join order_adjustments oa on rl.order_id = oa.order_id ) ), refund_summary as ( select ar.order_id, ar.line_item_id, ar.date, ifnull(rl.refund_quantity,0) refund_quantity, ifnull(rl.refund_subtotal,0) refund_subtotal, ifnull(safe_divide(oa.adjustment,count(1) over (partition by ar.order_id, ar.date)),0) as Adjustment_amount, (ifnull(rl.refund_subtotal,0) - ifnull(ifnull(safe_divide(oa.adjustment,count(1) over (partition by ar.order_id, ar.date)),0),0)) Total_Refund from adj_refund ar left join refund_line_items rl on ar.order_id = rl.order_id and ar.date = rl.refund_date and ar.line_item_id = rl.line_item_id left join order_adjustments oa on ar.order_id = oa.order_id and ar.date = oa.adjustment_date ), aggregate_summary AS ( SELECT order_id, line_item_id, date, sum(refund_quantity) AS Refund_Quantity, sum(Total_Refund) AS Refund_Amount, sum(Adjustment_amount) AS Adjustment_Amount, sum(refund_subtotal) AS Refund_Before_Adjustment FROM refund_summary GROUP BY order_id, line_item_id, date ) SELECT asum.order_id, asum.line_item_id, SUM(asum.Refund_Quantity) AS Quantity, SUM(asum.Refund_Amount) AS Amount, SUM(asum.Adjustment_Amount) AS Adjustment_Amount, SUM(asum.Refund_Before_Adjustment) AS Refund_Before_Adjustment, ARRAY_AGG( STRUCT( date AS Refund_Date, CAST(asum.Refund_Quantity AS FLOAT64) AS Refund_Quantity, CAST(asum.Adjustment_Amount AS FLOAT64) AS Adjustment_Amount, CAST(asum.Refund_Amount AS FLOAT64) AS Refund_Amount ) ) AS Refund_Details FROM aggregate_summary asum GROUP BY asum.order_id, asum.line_item_id; CREATE OR REPLACE TABLE zouk-wh.maplemonk.Shopify_All_orders_fulfillment AS SELECT JSON_EXTRACT_SCALAR(A,\'$.order_id\') AS order_id, JSON_EXTRACT_SCALAR(B,\'$.id\') AS Line_Item_ID, JSON_EXTRACT_SCALAR(A,\'$.tracking_number\') AS AWB, JSON_EXTRACT_SCALAR(A,\'$.tracking_company\') AS Courier, JSON_EXTRACT_SCALAR(A,\'$.shipment_status\') AS SHIPPING_STATUS , JSON_EXTRACT_SCALAR(A,\'$.updated_at\') AS shipping_status_update_date, JSON_EXTRACT_SCALAR(A,\'$.tracking_url\') AS tracking_url, JSON_EXTRACT_SCALAR(A,\'$.created_at\') AS Shipping_created_at, FROM zouk-wh.maplemonk.Shopify_All_orders left join unnest(fulfillments) A left join unnest(JSON_EXTRACT_ARRAY(JSON_EXTRACT(A, \'$.line_items\'))) B; create or replace table zouk-wh.maplemonk.Shopify_All_orders_shipping_lines_discount as SELECT id as Order_ID, sum(cast(JSON_EXTRACT_SCALAR(da,\'$.amount\') as float64)) AS total_shipping_discount, FROM zouk-wh.maplemonk.Shopify_All_orders left join unnest(shipping_lines)sl left join unnest(JSON_EXTRACT_ARRAY(JSON_EXTRACT(sl, \'$.discount_allocations\'))) da group by 1 ; CREATE OR REPLACE TABLE zouk-wh.maplemonk.Shopify_All_orders_items AS WITH CTE AS ( SELECT SHOP_NAME, CAST(ID AS STRING) AS ORDER_ID, NAME AS ORDER_NAME, CUSTOMER, JSON_EXTRACT_SCALAR(CUSTOMER, \'$.default_address.name\') AS NAME, PHONE, EMAIL, TAGS, JSON_EXTRACT_SCALAR(A, \'$.id\') AS LINE_ITEM_ID, JSON_EXTRACT_SCALAR(A, \'$.sku\') AS SKU, JSON_EXTRACT_SCALAR(A, \'$.product_id\') AS PRODUCT_ID, JSON_EXTRACT_SCALAR(A, \'$.title\') AS PRODUCT_NAME, CURRENCY, CASE WHEN CANCELLED_AT IS NOT NULL THEN \'CANCELLED\' ELSE \'SHOPIFY_PROCESSED\' END AS ORDER_STATUS, DATETIME(FORMAT_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\', created_at, \'Asia/Kolkata\')) AS ORDER_TIMESTAMP, CAST(JSON_EXTRACT_SCALAR(A, \'$.price\') AS FLOAT64) * CAST(JSON_EXTRACT_SCALAR(A, \'$.quantity\') AS FLOAT64) AS LINE_ITEM_SALES, (CAST(JSON_EXTRACT_SCALAR(TOTAL_SHIPPING_PRICE_SET, \'$.presentment_money.amount\') AS FLOAT64) / COUNT(ID) OVER(PARTITION BY ID ORDER BY ID)) AS SHIPPING_PRICE, CAST(JSON_EXTRACT_SCALAR(A, \'$.price\') AS FLOAT64) / (1 + CAST(JSON_EXTRACT_SCALAR(A, \'$.tax_lines.rate\') AS FLOAT64)) AS PRICE_BEFORE_TAX, CAST(JSON_EXTRACT_SCALAR(A, \'$.quantity\') AS INT64) AS QUANTITY, \'SHOPIFY\' AS SOURCE, MOMENTS_COUNT, DAYSTOCONVERT, Shopifyql_LAST_VISIT_UTM_CAMPAIGN, Shopifyql_FIRSTVISIT_UTM_CAMPAIGN, SHOPIFYQL_FIRSTVISIT_UTM_SOURCE, SHOPIFYQL_MAPPED_SOURCE, SHOPIFYQL_MAPPED_CHANNEL, SHOPIFYQL_LAST_MOMENT_UTM_SOURCE, Shopifyql_LAST_VISIT_NON_UTM_SOURCE, Shopifyql_LAST_MOMENT_UTM_MEDIUM, Shopifyql_FIRSTVISIT_UTM_MEDIUM, CHECKOUT_UTM_SOURCE, CHECKOUT_UTM_MEDIUM, CHECKOUT_UTM_CAMPAIGN, CHECKOUT_UTM_CONTENT, FINAL_UTM_CAMPAIGN, FINAL_UTM_CHANNEL, FINAL_UTM_SOURCE, REFERRER_NAME, CHECKOUT_MAPPED_SOURCE, CHECKOUT_MAPPED_CHANNEL, PAYMENT_GATEWAY_NAMES AS GATEWAY, SHIPPING_ADDRESS, PINCODE, case when LOWER(ARRAY_TO_STRING(PAYMENT_GATEWAY_NAMES,\',\')) like any (\'%cod%\',\'%cash%\') then \'COD\' else \'PREPAID\' end payment_mode FROM `zouk-wh.maplemonk.Shopify_All_orders` LEFT JOIN UNNEST(LINE_ITEMS) AS A ) SELECT CTE.*, IFNULL(T.TAX_RATE, 0) AS TAX_RATE, IFNULL(D.DISCOUNT, 0) AS DISCOUNT, CTE.LINE_ITEM_SALES AS GROSS_SALES_AFTER_TAX, CTE.LINE_ITEM_SALES / (1 + IFNULL(T.TAX_RATE, 0)) AS GROSS_SALES_BEFORE_TAX, CASE WHEN IFNULL(T.TAX, 0) = 0 THEN IFNULL(D.DISCOUNT, 0) ELSE IFNULL(D.DISCOUNT, 0) / (1 + IFNULL(T.TAX_RATE, 0)) END AS DISCOUNT_BEFORE_TAX, CASE WHEN IFNULL(T.TAX, 0) = 0 THEN CTE.LINE_ITEM_SALES - IFNULL(D.DISCOUNT, 0) ELSE CTE.LINE_ITEM_SALES / (1 + IFNULL(T.TAX_RATE, 0)) - IFNULL(D.DISCOUNT, 0) / (1 + IFNULL(T.TAX_RATE, 0)) END AS NET_SALES_BEFORE_TAX, IFNULL(T.TAX, 0) AS TAX, CASE WHEN IFNULL(T.TAX, 0) = 0 THEN CTE.LINE_ITEM_SALES - IFNULL(D.DISCOUNT, 0) - safe_divide(IFNULL(SD.TOTAL_SHIPPING_DISCOUNT, 0), COUNT(1) OVER (PARTITION BY CTE.ORDER_ID)) + IFNULL(T.TAX, 0) + CTE.SHIPPING_PRICE ELSE CTE.LINE_ITEM_SALES / (1 + IFNULL(T.TAX_RATE, 0)) - IFNULL(D.DISCOUNT, 0) / (1 + IFNULL(T.TAX_RATE, 0)) - safe_divide(IFNULL(SD.TOTAL_SHIPPING_DISCOUNT, 0), COUNT(1) OVER (PARTITION BY CTE.ORDER_ID)) + IFNULL(T.TAX, 0) + CTE.SHIPPING_PRICE END AS TOTAL_SALES, CASE WHEN R.QUANTITY IS NOT NULL THEN 1 ELSE 0 END AS IS_REFUND, R.REFUND_DETAILS, R.QUANTITY AS REFUND_QUANTITY, R.AMOUNT AS REFUND_VALUE, F.AWB, F.COURIER, F.SHIPPING_STATUS, F.SHIPPING_STATUS_UPDATE_DATE, F.TRACKING_URL, F.SHIPPING_CREATED_AT, safe_divide(IFNULL(SD.TOTAL_SHIPPING_DISCOUNT, 0), COUNT(1) OVER (PARTITION BY CTE.ORDER_ID)) AS TOTAL_SHIPPING_DISCOUNT FROM CTE LEFT JOIN `zouk-wh.maplemonk.Shopify_All_orders_items_tax` T ON CTE.ORDER_ID = CAST(T.ORDER_ID AS STRING) AND CTE.LINE_ITEM_ID = CAST(T.LINE_ITEM_ID AS STRING) LEFT JOIN `zouk-wh.maplemonk.Shopify_All_orders_items_discount` D ON CTE.ORDER_ID = CAST(D.ORDER_ID AS STRING) AND CTE.LINE_ITEM_ID = CAST(D.LINE_ITEM_ID AS STRING) LEFT JOIN `zouk-wh.maplemonk.Shopify_All_Refunds` R ON CTE.ORDER_ID = CAST(R.ORDER_ID AS STRING) AND CTE.LINE_ITEM_ID = CAST(R.LINE_ITEM_ID AS STRING) LEFT JOIN `zouk-wh.maplemonk.Shopify_All_orders_fulfillment` F ON CTE.ORDER_ID = CAST(F.ORDER_ID AS STRING) AND CTE.LINE_ITEM_ID = CAST(F.LINE_ITEM_ID AS STRING) LEFT JOIN `zouk-wh.maplemonk.Shopify_All_orders_shipping_lines_discount` SD ON CTE.ORDER_ID = CAST(SD.ORDER_ID AS STRING); CREATE OR REPLACE TABLE zouk-wh.maplemonk.zouk_SHOPIFY_FACT_ITEMS_INTERMEDIATE AS SELECT O.SHOP_NAME, \'SHOPIFY\' AS marketplace, O.ORDER_ID, O.ORDER_NAME, JSON_EXTRACT_SCALAR(O.CUSTOMER, \'$.id\') AS customer_id, O.Name, O.EMAIL, O.PHONE, O.Tags, O.LINE_ITEM_ID, O.SKU, O.PRODUCT_ID, O.CURRENCY, O.IS_REFUND, UPPER(JSON_EXTRACT_SCALAR(O.shipping_address, \'$.city\')) AS shipping_city, UPPER(JSON_EXTRACT_SCALAR(O.shipping_address, \'$.province\')) AS shipping_State, CASE WHEN CD.city IS NULL OR CD.city = \'\' THEN \'NA\' ELSE UPPER(CD.city) END AS city, CASE WHEN CD.province IS NULL OR CD.province = \'\' THEN \'NA\' ELSE UPPER(CD.province) END AS state, CASE WHEN P.title = \'\' THEN \'NA\' ELSE UPPER(P.title) END AS product_name, CASE WHEN P.product_type = \'\' THEN \'NA\' ELSE UPPER(P.product_type) END AS category, UPPER(O.order_status) AS order_status, O.order_timestamp, O.LINE_ITEM_SALES, O.QUANTITY, O.Refund_Quantity, O.Refund_Value, O.TAX, O.TAX_RATE, O.DISCOUNT, O.DISCOUNT_BEFORE_TAX, O.GROSS_SALES_AFTER_TAX, O.GROSS_SALES_BEFORE_TAX, O.NET_SALES_BEFORE_TAX, (IFNULL(O.SHIPPING_PRICE, 0) - IFNULL(O.TOTAL_SHIPPING_DISCOUNT, 0)) AS SHIPPING_PRICE, IFNULL(O.TOTAL_SALES, 0) AS TOTAL_SALES, O.Source, O.MOMENTS_COUNT, O.DAYSTOCONVERT, O.SHOPIFYQL_FIRSTVISIT_UTM_SOURCE, O.SHOPIFYQL_MAPPED_SOURCE, O.SHOPIFYQL_MAPPED_CHANNEL, O.SHOPIFYQL_LAST_MOMENT_UTM_SOURCE, O.Shopifyql_LAST_VISIT_NON_UTM_SOURCE, O.Shopifyql_FIRSTVISIT_UTM_MEDIUM, O.Shopifyql_LAST_MOMENT_UTM_MEDIUM, O.FINAL_UTM_CHANNEL, O.FINAL_UTM_CAMPAIGN, O.FINAL_UTM_SOURCE, O.Referrer_Name, O.CHECKOUT_MAPPED_SOURCE, O.CHECKOUT_MAPPED_CHANNEL, O.REFUND_DETAILS, O.AWB, UPPER(O.Courier) AS Courier, UPPER(O.Shipping_status) AS SHIPPING_STATUS, O.shipping_status_update_date, O.tracking_url, O.Shipping_created_at, O.GATEWAY, O.payment_mode, O.pincode FROM zouk-wh.maplemonk.Shopify_All_orders_items O LEFT JOIN zouk-wh.maplemonk.Shopify_All_products P ON cast(O.PRODUCT_ID as string) = cast(P.id as string) LEFT JOIN (SELECT customer_id, city, province, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY id DESC) AS rowid FROM zouk-wh.maplemonk.Shopify_All_customers_addresses) AS CD ON JSON_EXTRACT_SCALAR(O.CUSTOMER, \'$.id\') = CAST(CD.customer_id AS STRING) AND cast(CD.rowid as int) = 1; CREATE OR REPLACE TABLE zouk-wh.maplemonk.zouk_SHOPIFY_FACT_ITEMS_TEMP_Category as select fi.* ,coalesce(p.commonsku,fi.SKU) AS SKU_CODE ,Upper(coalesce(p.name,fi.product_name)) as PRODUCT_NAME_Final ,coalesce(Upper(p.CATEGORY),upper(fi.category)) AS Product_Category ,Upper(p.sub_category) as Product_Sub_Category ,Upper(p.collection) Collection ,upper(p.print) PRINT ,upper(P.product_type) PRODUCT_TYPE ,p.BAU_OFFLINE ,p.BAU_ONLINE ,p.TAX_RATE FINAL_TAX_RATE from zouk-wh.maplemonk.zouk_SHOPIFY_FACT_ITEMS_INTERMEDIATE fi left join (select * from (select marketplace_sku skucode, name, category, sub_category, category_code, collection, print, PRODUCT_TYPE, commonsku, BAU_OFFLINE, BAU_ONLINE, TAX_RATE, row_number() over (partition by marketplace_sku order by 1) rw from zouk-wh.maplemonk.final_sku_master where lower(marketplace) like \'%shopify%\') where rw = 1 ) p on lower(fi.sku) = lower(p.skucode); CREATE OR REPLACE TABLE zouk-wh.maplemonk.zouk_SHOPIFY_FACT_ITEMS_INTERMEDIATE AS SELECT * FROM zouk-wh.maplemonk.zouk_SHOPIFY_FACT_ITEMS_TEMP_Category; CREATE OR REPLACE TABLE `zouk-wh.maplemonk.zouk_Shopify_Final_customerID` AS WITH new_phone_numbers AS ( SELECT phone, contact_num, 19700000000 + ROW_NUMBER() OVER (ORDER BY contact_num ASC) AS maple_monk_id FROM ( SELECT DISTINCT RIGHT(REGEXP_REPLACE(REPLACE(phone, \' \', \'\'), r\'[^a-zA-Z0-9]+\', \'\'), 10) AS contact_num, phone FROM `zouk-wh.maplemonk.zouk_SHOPIFY_FACT_ITEMS_INTERMEDIATE` ) a ), int AS ( SELECT contact_num, email, COALESCE(maple_monk_id, id2) AS maple_monk_id FROM ( SELECT contact_num, email, maple_monk_id, 19800000000 + ROW_NUMBER() OVER (PARTITION BY maple_monk_id IS NULL ORDER BY email ASC) AS id2 FROM ( SELECT DISTINCT COALESCE(p.contact_num, RIGHT(REGEXP_REPLACE(e.contact_num, r\'[^a-zA-Z0-9]+\', \'\'), 10)) AS contact_num, e.email, maple_monk_id FROM ( SELECT REPLACE(phone, \' \', \'\') AS contact_num, email FROM `zouk-wh.maplemonk.zouk_SHOPIFY_FACT_ITEMS_INTERMEDIATE` ) e LEFT JOIN new_phone_numbers p ON p.contact_num = RIGHT(REGEXP_REPLACE(e.contact_num, r\'[^a-zA-Z0-9]+\', \'\'), 10) ) a ) b ) SELECT contact_num, email, maple_monk_id FROM int WHERE COALESCE(contact_num, email) IS NOT NULL; CREATE OR REPLACE TABLE `zouk-wh.maplemonk.zouk_SHOPIFY_FACT_ITEMS` AS SELECT COALESCE(m.maple_monk_id_phone, d.maple_monk_id) AS Shopify_customer_id_final, MIN(CAST(ORDER_TIMESTAMP AS DATE)) OVER (PARTITION BY COALESCE(m.maple_monk_id_phone, d.maple_monk_id)) AS shopify_acquisition_date, MIN(CASE WHEN LOWER(order_status) NOT IN (\'cancelled\') THEN CAST(ORDER_TIMESTAMP AS DATE) END) OVER (PARTITION BY COALESCE(m.maple_monk_id_phone, d.maple_monk_id)) AS shopify_first_complete_order_date, m.* FROM ( SELECT c.maple_monk_id AS maple_monk_id_phone, o.* FROM `zouk-wh.maplemonk.zouk_SHOPIFY_FACT_ITEMS_INTERMEDIATE` o LEFT JOIN ( SELECT contact_num AS phone, maple_monk_id FROM ( SELECT contact_num, maple_monk_id, ROW_NUMBER() OVER (PARTITION BY contact_num ORDER BY maple_monk_id ASC) AS magic FROM `zouk-wh.maplemonk.zouk_Shopify_Final_customerID` ) WHERE magic = 1 ) c ON c.phone = RIGHT(REGEXP_REPLACE(o.phone, r\'[^a-zA-Z0-9]+\', \'\'), 10) ) m LEFT JOIN ( SELECT DISTINCT maple_monk_id, email FROM `zouk-wh.maplemonk.zouk_Shopify_Final_customerID` WHERE contact_num IS NULL ) d ON d.email = m.email; ALTER TABLE `zouk-wh.maplemonk.zouk_SHOPIFY_FACT_ITEMS` ADD COLUMN shopify_new_customer_flag STRING, ADD COLUMN shopify_new_customer_flag_month STRING, ADD COLUMN shopify_acquisition_product STRING, ADD COLUMN shopify_acquisition_channel STRING, ADD COLUMN shopify_acquisition_source STRING; UPDATE `zouk-wh.maplemonk.zouk_SHOPIFY_FACT_ITEMS` AS A SET A.shopify_new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, Shopify_customer_id_final, ORDER_TIMESTAMP, CASE WHEN DATE(ORDER_TIMESTAMP) = shopify_first_complete_order_date THEN \'New\' WHEN DATE(ORDER_TIMESTAMP) < shopify_first_complete_order_date OR shopify_first_complete_order_date IS NULL THEN \'Yet to make completed order\' WHEN DATE(ORDER_TIMESTAMP) > shopify_first_complete_order_date THEN \'Repeat\' END AS flag FROM `zouk-wh.maplemonk.zouk_SHOPIFY_FACT_ITEMS` ) AS B WHERE A.order_id = B.order_id AND A.Shopify_customer_id_final = B.Shopify_customer_id_final AND DATE(A.ORDER_TIMESTAMP) = DATE(B.ORDER_TIMESTAMP); UPDATE `zouk-wh.maplemonk.zouk_SHOPIFY_FACT_ITEMS` SET shopify_new_customer_flag = CASE WHEN shopify_new_customer_flag IS NULL AND (CASE WHEN LOWER(order_status) IS NULL THEN TRUE ELSE LOWER(order_status) NOT IN (\'cancelled\') END) THEN \'New\' WHEN shopify_new_customer_flag IS NULL AND (CASE WHEN LOWER(order_status) IS NULL THEN TRUE ELSE LOWER(order_status) IN (\'cancelled\') END) THEN \'Yet to make completed order\' ELSE shopify_new_customer_flag END where true; UPDATE `zouk-wh.maplemonk.zouk_SHOPIFY_FACT_ITEMS` AS A SET A.shopify_new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, shopify_customer_id_final, DATE(ORDER_TIMESTAMP) AS Order_Date, CASE WHEN LAST_DAY(DATE(ORDER_TIMESTAMP)) = LAST_DAY(DATE(shopify_first_complete_order_date)) THEN \'New\' WHEN LAST_DAY(DATE(ORDER_TIMESTAMP)) < LAST_DAY(DATE(shopify_first_complete_order_date)) OR shopify_acquisition_date IS NULL THEN \'Yet to make completed order\' WHEN LAST_DAY(DATE(ORDER_TIMESTAMP)) > LAST_DAY(DATE(shopify_first_complete_order_date)) THEN \'Repeat\' END AS Flag FROM `zouk-wh.maplemonk.zouk_SHOPIFY_FACT_ITEMS` ) AS B WHERE A.order_id = B.order_id AND A.shopify_customer_id_final = B.shopify_customer_id_final; UPDATE `zouk-wh.maplemonk.zouk_SHOPIFY_FACT_ITEMS` SET shopify_new_customer_flag_month = CASE WHEN shopify_new_customer_flag_month IS NULL AND (CASE WHEN LOWER(order_status) IS NULL THEN TRUE ELSE LOWER(order_status) NOT IN (\'cancelled\') END) THEN \'New\' ELSE shopify_new_customer_flag_month END WHERE true; CREATE OR REPLACE TABLE `zouk-wh.maplemonk.temp_source_1` AS SELECT shopify_customer_id_final, channel, source FROM ( SELECT DISTINCT shopify_customer_id_final, DATE(order_timestamp) AS order_Date, FINAL_UTM_SOURCE AS SOURCE, FINAL_UTM_CHANNEL AS CHANNEL, MIN(CASE WHEN LOWER(order_status) NOT IN (\'cancelled\') THEN DATE(order_timestamp) END) OVER (PARTITION BY shopify_customer_id_final) AS firstOrderdate, row_number() over(partition by shopify_customer_id_final order by 1 desc) rw FROM `zouk-wh.maplemonk.zouk_SHOPIFY_FACT_ITEMS` ) res WHERE order_Date = firstOrderdate and rw = 1; UPDATE `zouk-wh.maplemonk.zouk_SHOPIFY_FACT_ITEMS` AS a SET a.shopify_acquisition_channel = b.channel FROM `zouk-wh.maplemonk.temp_source_1` b WHERE a.shopify_customer_id_final = b.shopify_customer_id_final; UPDATE `zouk-wh.maplemonk.zouk_SHOPIFY_FACT_ITEMS` AS a SET a.shopify_acquisition_source = b.source FROM `zouk-wh.maplemonk.temp_source_1` b WHERE a.shopify_customer_id_final = b.shopify_customer_id_final; CREATE OR REPLACE TABLE `zouk-wh.maplemonk.temp_product_1` AS SELECT DISTINCT shopify_customer_id_final, product_name_final, ROW_NUMBER() OVER (PARTITION BY shopify_customer_id_final ORDER BY total_sales DESC) AS rowid FROM ( SELECT DISTINCT shopify_customer_id_final, CAST(order_timestamp AS DATE) AS order_date, product_name_final, total_sales, MIN(CASE WHEN LOWER(order_status) NOT IN (\'cancelled\') THEN CAST(order_timestamp AS DATE) END) OVER (PARTITION BY shopify_customer_id_final) AS firstOrderdate FROM `zouk-wh.maplemonk.zouk_SHOPIFY_FACT_ITEMS` ) res WHERE order_date = firstOrderdate; UPDATE zouk-wh.maplemonk.zouk_SHOPIFY_FACT_ITEMS AS A SET A.shopify_acquisition_product=B.product_name_final FROM ( SELECT * FROM zouk-wh.maplemonk.temp_product_1 WHERE rowid=1)B WHERE A.shopify_customer_id_final = B.shopify_customer_id_final; ALTER TABLE `zouk-wh.maplemonk.zouk_SHOPIFY_FACT_ITEMS` ADD COLUMN SHIPPING_TAX FLOAT64, ADD COLUMN SHIP_PROMOTION_DISCOUNT FLOAT64, ADD COLUMN GIFT_WRAP_PRICE FLOAT64, ADD COLUMN GIFT_WRAP_TAX FLOAT64;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            