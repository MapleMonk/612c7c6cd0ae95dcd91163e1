{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE RPSG_DB.MAPLEMONK.Shopify_DRV_All_customers AS select *,\'Shopify_DRV\' AS Shop_Name from RPSG_DB.MAPLEMONK.SHOPIFY_DRV_CUSTOMERS; create or replace table RPSG_DB.MAPLEMONK.DRV_SHOPIFY_UTM_PARAMETERS_FINAL as select ShopifyQL.* ,upper(coalesce(UTM_MAPPING.CHANNEL,UTM_MAPPING_REF.CHANNEL,case when lower(ShopifyQL.ShopifyQL_Unmapped_Last_Source) like any (\'bik\', \'%webengage%\') then \'Retention\' else ShopifyQL.ShopifyQL_Unmapped_Last_Source end,ShopifyQL.Referrer_Name)) as ShopifyQL_MAPPED_CHANNEL ,upper(coalesce(UTM_MAPPING.SOURCE,UTM_MAPPING_REF.SOURCE,ShopifyQL.ShopifyQL_Unmapped_Last_Source,ShopifyQL.Referrer_Name)) as ShopifyQL_MAPPED_SOURCE ,upper(coalesce(UTM_MAPPING_FIRST_CLICK.SOURCE,ShopifyQL.FirstVisit_UTM_Source)) as ShopifyQL_MAPPED_FIRSTCLICK_SOURCE ,upper(UTM_MAPPING_FIRST_CLICK.CHANNEL) as ShopifyQL_MAPPED_FIRSTCLICK_CHANNEL from (select * from (select A.id ,A.name ,A.createdat ,replace(A.customerjourneysummary:\"momentsCount\",\'\"\',\'\') Moments_Count ,replace(A.customerjourneysummary:\"daysToConversion\",\'\"\',\'\') DaysToConvert ,replace(A.CUSTOMERJOURNEYSUMMARY:\"lastVisit\":\"utmParameters\":\"source\",\'\"\',\'\') LastVisit_UTM_Source ,replace(A.CUSTOMERJOURNEYSUMMARY:\"lastVisit\":\"source\",\'\"\',\'\') LastVisit_NON_UTM_Source ,replace(A.CUSTOMERJOURNEYSUMMARY:\"lastVisit\":\"utmParameters\":\"campaign\",\'\"\',\'\') LastVisit_UTM_Campaign ,replace(A.CUSTOMERJOURNEYSUMMARY:\"firstVisit\":\"utmParameters\":\"source\",\'\"\',\'\') FirstVisit_UTM_Source ,replace(A.CUSTOMERJOURNEYSUMMARY:\"firstVisit\":\"utmParameters\":\"medium\",\'\"\',\'\') FirstVisit_UTM_Medium ,replace(A.CUSTOMERJOURNEYSUMMARY:\"firstVisit\":\"utmParameters\":\"campaign\",\'\"\',\'\') FirstVisit_UTM_Campaign ,replace(B.Value:\"id\",\'gid://shopify/CustomerVisit/\',\'\') Moment_ID ,replace(B.value:\"utmParameters\":\"source\",\'\"\',\'\') Last_Moment_UTM_Source ,replace(B.value:\"utmParameters\":\"medium\",\'\"\',\'\') Last_Moment_UTM_Medium ,case when Moments_Count >1 then LastVisit_UTM_Source else FirstVisit_UTM_Source end CJSummary_utm_source ,referrerdisplaytext Referrer_Name ,customerjourneysummary ,customerjourney ,coalesce(Last_Moment_UTM_Source,LastVisit_NON_UTM_Source) ShopifyQL_Unmapped_Last_Source ,coalesce(Last_Moment_UTM_Medium,LastVisit_UTM_Source) ShopifyQL_Unmapped_Last_Medium ,rank() over (partition by name order by MOMENT_ID desc) rw from rpsg_db.maplemonk.drv_shopify_utm_parameters A, lateral flatten (INPUT => customerjourney:\"moments\", OUTER => TRUE) B ) where rw=1 ) ShopifyQL left join ( select * from (select *,\"New Channel\" as channel , row_number() over (partition by lower(concat(lower(ifnull(source,\'\')),lower(ifnull(medium,\'\')))) order by \"New Channel\") rw from RPSG_DB.MAPLEMONK.utm_ga_consolidated_channel_mapping ) where rw=1 and (source is not null or medium is not null) ) UTM_MAPPING on lower(concat(ifnull(ShopifyQL.ShopifyQL_Unmapped_Last_Source,\'\'), ifnull(ShopifyQL.ShopifyQL_Unmapped_Last_Medium,\'\'))) = lower(concat(lower(ifnull(UTM_MAPPING.source,\'\')),lower(ifnull(UTM_MAPPING.medium,\'\')))) left join ( select * from (select *,\"New Channel\" as channel , row_number() over (partition by lower(ifnull(source,\'\')) order by \"New Channel\") rw from RPSG_DB.MAPLEMONK.utm_ga_consolidated_channel_mapping ) where rw=1 and (source is not null) ) UTM_MAPPING_REF on lower(ifnull(ShopifyQL.referrer_name,\'\')) = lower(ifnull(UTM_MAPPING_REF.source,\'\')) left join ( select * from (select *,\"New Channel\" as channel , row_number() over (partition by lower(concat(lower(ifnull(source,\'\')),lower(ifnull(medium,\'\')))) order by \"New Channel\") rw from RPSG_DB.MAPLEMONK.utm_ga_consolidated_channel_mapping ) where rw=1 and (source is not null or medium is not null) ) UTM_MAPPING_FIRST_CLICK on lower(concat(ifnull(ShopifyQL.ShopifyQL_Unmapped_Last_Source,\'\'), ifnull(ShopifyQL.ShopifyQL_Unmapped_Last_Medium,\'\'))) = concat(lower(ifnull(UTM_MAPPING_FIRST_CLICK.source,\'\')),lower(ifnull(UTM_MAPPING_FIRST_CLICK.medium,\'\'))) ; create or replace table RPSG_DB.MAPLEMONK.DRV_GOKWIK_SOURCE as With GOKWIK as ( WITH utm_source_cte AS ( SELECT S.id, UPPER(A.value:\"value\") AS utm_source FROM RPSG_DB.MAPLEMONK.shopify_drv_orders S, LATERAL FLATTEN(INPUT => note_attributes) A where (LOWER(S.note_attributes) LIKE \'%gokwik%\' or LOWER(S.tags) LIKE \'%gokwik%\') AND LOWER(A.value:\"name\") = \'utm_source\' ), utm_medium_cte AS ( SELECT S.id, UPPER(A.value:\"value\") AS utm_medium FROM RPSG_DB.MAPLEMONK.shopify_drv_orders S, LATERAL FLATTEN(INPUT => note_attributes) A where LOWER(S.note_attributes) LIKE \'%gokwik%\' AND LOWER(A.value:\"name\") = \'utm_medium\' ), utm_campaign_cte AS ( SELECT S.id, UPPER(case when A.value:\"value\" = \'NA\' then \'SHOPPING FEED\' else A.value:\"value\" end) AS utm_campaign FROM RPSG_DB.MAPLEMONK.shopify_drv_orders S, LATERAL FLATTEN(INPUT => note_attributes) A where LOWER(S.note_attributes) LIKE \'%gokwik%\' AND LOWER(A.value:\"name\") = \'utm_campaign\' ), utm_content_cte AS ( SELECT S.id ,UPPER(A.value:\"value\") AS utm_content FROM RPSG_DB.MAPLEMONK.shopify_drv_orders S, LATERAL FLATTEN(INPUT => note_attributes) A where LOWER(S.note_attributes) LIKE \'%gokwik%\' AND LOWER(A.value:\"name\") = \'utm_content\' ) SELECT S.id, S.tags, trim(source_cte.utm_source) AS GOKWIK_UTM_SOURCE, trim(medium_cte.utm_medium) AS GOKWIK_UTM_MEDIUM, campaign_cte.utm_campaign AS GOKWIK_UTM_CAMPAIGN, content_cte.utm_content AS GOKWIK_UTM_CONTENT FROM RPSG_DB.MAPLEMONK.shopify_drv_orders S LEFT JOIN utm_source_cte source_cte ON S.id = source_cte.id LEFT JOIN utm_medium_cte medium_cte ON S.id = medium_cte.id LEFT JOIN utm_campaign_cte campaign_cte ON S.id = campaign_cte.id LEFT JOIN utm_content_cte content_cte ON S.id = content_cte.id WHERE LOWER(S.note_attributes) LIKE \'%gokwik%\' ) Select GOKWIK.* ,coalesce(Upper(UTM_MAPPING.CHANNEL), upper(case when lower(GOKWIK_UTM_SOURCE) like any (\'bik\', \'%webengage%\') then \'Retention\' else GOKWIK_UTM_SOURCE end)) as GOKWIK_MAPPED_CHANNEL ,coalesce(Upper(UTM_MAPPING.channel), upper(GOKWIK_UTM_SOURCE)) as GOKWIK_MAPPED_MODE from GOKWIK left join (select * from (select *,\"New Channel\" as channel , row_number() over (partition by lower(concat(lower(ifnull(source,\'\')),lower(ifnull(medium,\'\')))) order by \"New Channel\") rw from RPSG_DB.MAPLEMONK.utm_ga_consolidated_channel_mapping ) where rw=1 and (source is not null or medium is not null) ) UTM_MAPPING on lower(concat(ifnull(GOKWIK.GOKWIK_UTM_SOURCE,\'\'),ifnull(GOKWIK.GOKWIK_UTM_MEDIUM,\'\'))) = lower(concat(ifnull(UTM_MAPPING.source,\'\'),ifnull(UTM_MAPPING.medium,\'\'))); create or replace table RPSG_DB.MAPLEMONK.Shopify_DRV_All_orders as select AO.* ,UPPER(ShopifyQL.shopifyql_mapped_channel) shopifyql_mapped_channel ,UPPER(ShopifyQL.ShopifyQL_Unmapped_Last_Source) ShopifyQL_Unmapped_Last_Source ,UPPER(ShopifyQL.SHOPIFYQL_UNMAPPED_LAST_MEDIUM) SHOPIFYQL_UNMAPPED_LAST_MEDIUM ,UPPER(ShopifyQL.shopifyql_mapped_source) shopifyql_mapped_source ,UPPER(ShopifyQL.FIRSTVISIT_UTM_SOURCE) Shopifyql_FIRSTVISIT_UTM_SOURCE ,UPPER(ShopifyQL.LAST_MOMENT_UTM_SOURCE) Shopifyql_LAST_MOMENT_UTM_SOURCE ,UPPER(ShopifyQL.LastVisit_NON_UTM_Source) Shopifyql_LAST_VISIT_NON_UTM_SOURCE ,upper(ShopifyQL.LastVisit_UTM_Campaign) Shopifyql_LAST_VISIT_UTM_CAMPAIGN ,UPPER(ShopifyQL.LAST_MOMENT_UTM_MEDIUM) Shopifyql_LAST_MOMENT_UTM_MEDIUM ,UPPER(ShopifyQL.FIRSTVISIT_UTM_MEDIUM) Shopifyql_FIRSTVISIT_UTM_MEDIUM ,UPPER(ShopifyQL.FirstVisit_UTM_Campaign) Shopifyql_FIRSTVISIT_UTM_CAMPAIGN ,UPPER(ShopifyQL.ShopifyQL_MAPPED_FIRSTCLICK_SOURCE) ShopifyQL_MAPPED_FIRSTCLICK_SOURCE ,UPPER(ShopifyQL.ShopifyQL_MAPPED_FIRSTCLICK_CHANNEL) ShopifyQL_MAPPED_FIRSTCLICK_CHANNEL ,div0(ShopifyQL.MOMENTS_COUNT,count(1) over (partition by AO.name order by 1)) MOMENTS_COUNT ,div0(ShopifyQL.DAYSTOCONVERT,count(1) over (partition by AO.name order by 1)) DAYSTOCONVERT ,UPPER(GOKWIK.GOKWIK_UTM_SOURCE) GOKWIK_UTM_SOURCE ,UPPER(GOKWIK.GOKWIK_UTM_MEDIUM) GOKWIK_UTM_MEDIUM ,UPPER(GOKWIK.GOKWIK_UTM_CAMPAIGN) GOKWIK_UTM_CAMPAIGN ,UPPER(GOKWIK.GOKWIK_MAPPED_CHANNEL) GOKWIK_MAPPED_CHANNEL ,UPPER(GOKWIK.GOKWIK_MAPPED_MODE) GOKWIK_MAPPED_MODE ,UPPER(GOKWIK.GOKWIK_UTM_CONTENT) GOKWIK_UTM_CONTENT ,UPPER(ShopifyQL.Referrer_Name) Referrer_Name ,UPPER(coalesce(GOKWIK_UTM_CAMPAIGN,Shopifyql_LAST_VISIT_UTM_CAMPAIGN)) FINAL_UTM_CAMPAIGN ,Upper(coalesce(GOKWIK_MAPPED_MODE,shopifyql_mapped_source)) FINAL_UTM_SOURCE ,Upper(coalesce(GOKWIK_MAPPED_CHANNEL,shopifyql_mapped_channel)) FINAL_UTM_CHANNEL from (select * ,case when tags like \'%CRED%\' then \'CRED\' else \'Shopify_DRV\' end AS Shop_Name from RPSG_DB.MAPLEMONK.shopify_drv_orders) AO left join RPSG_DB.MAPLEMONK.DRV_SHOPIFY_UTM_PARAMETERS_FINAL ShopifyQL on AO.name = ShopifyQL.name left join RPSG_DB.MAPLEMONK.DRV_GOKWIK_SOURCE GOKWIK on AO.ID = GOKWIK.ID ; ALTER TABLE RPSG_DB.maplemonk.Shopify_DRV_All_orders RENAME COLUMN _AIRBYTE_SHOPIFY_DRV_ORDERS_HASHID to _AIRBYTE_ORDERS_HASHID; CREATE OR REPLACE TABLE RPSG_DB.maplemonk.Shopify_DRV_All_products AS select *,\'Shopify_DRV\' AS Shop_Name from RPSG_DB.MAPLEMONK.SHOPIFY_DRV_PRODUCTS; ALTER TABLE RPSG_DB.maplemonk.Shopify_DRV_All_products RENAME COLUMN _AIRBYTE_Shopify_DRV_PRODUCTS_HASHID to _AIRBYTE_PRODUCTS_HASHID; CREATE OR REPLACE TABLE RPSG_DB.maplemonk.Shopify_DRV_All_products_variants AS select *,\'Shopify_DRV\' AS Shop_Name from RPSG_DB.MAPLEMONK.SHOPIFY_DRV_PRODUCTS_VARIANTS; ALTER TABLE RPSG_DB.MAPLEMONK.SHOPIFY_DRV_ALL_PRODUCTS_VARIANTS RENAME COLUMN _AIRBYTE_SHOPIFY_DRV_PRODUCTS_HASHID to _AIRBYTE_PRODUCTS_HASHID; CREATE OR REPLACE TABLE RPSG_DB.maplemonk.Shopify_DRV_All_customers_addresses AS select *,\'Shopify_DRV\' AS Shop_Name from RPSG_DB.MAPLEMONK.SHOPIFY_DRV_CUSTOMERS_ADDRESSES; CREATE OR REPLACE TABLE RPSG_DB.maplemonk.Shopify_DRV_All_orders_items_discount AS SELECT order_id, LINE_ITEM_ID, SUM(B.VALUE:amount::FLOAT) AS DISCOUNT FROM( SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:discount_allocations AS discount_allocations FROM RPSG_DB.maplemonk.Shopify_DRV_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A)x,LATERAL FLATTEN (INPUT => x.discount_allocations)B GROUP BY ORDER_ID, LINE_ITEM_ID; CREATE OR REPLACE TABLE RPSG_DB.maplemonk.Shopify_DRV_All_orders_items_tax AS SELECT order_id, LINE_ITEM_ID, SUM(B.VALUE:price::FLOAT) AS TAX, sum(B.VALUE:rate::float) as Tax_Rate FROM( SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:tax_lines AS tax_lines FROM RPSG_DB.maplemonk.Shopify_DRV_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A)x,LATERAL FLATTEN (INPUT => x.tax_lines)B GROUP BY order_id, LINE_ITEM_ID; CREATE OR REPLACE TABLE RPSG_DB.maplemonk.Shopify_DRV_All_Refunds AS WITH refund_line_items AS ( SELECT refunds.value:order_id::STRING AS order_id, line_items.value:line_item_id::string as LINE_ITEM_ID, try_to_timestamp(replace(refunds.value:\"created_at\",\'\"\',\'\')) AS refund_date, sum(ifnull(line_items.value:quantity::INT,0)) AS refund_quantity, sum(ifnull(line_items.value:subtotal::FLOAT,0)) AS refund_subtotal FROM RPSG_DB.maplemonk.Shopify_drv_All_orders, LATERAL FLATTEN(input => Shopify_drv_All_orders.refunds) refunds, LATERAL FLATTEN(input => refunds.value:refund_line_items) line_items group by refunds.value:order_id::STRING, line_items.value:line_item_id::string, try_to_timestamp(replace(refunds.value:\"created_at\",\'\"\',\'\')) ), order_adjustments AS ( SELECT order_adj.value:order_id::STRING AS order_id, try_to_timestamp(replace(refunds.value:\"created_at\",\'\"\',\'\')) adjustment_date, sum(ifnull(order_adj.value:amount::FLOAT,0)) AS adjustment FROM RPSG_DB.maplemonk.Shopify_drv_All_orders, LATERAL FLATTEN(input => Shopify_drv_All_orders.refunds) refunds, LATERAL FLATTEN(input => refunds.value:order_adjustments) order_adj group by order_adj.value:order_id::STRING, try_to_timestamp(replace(refunds.value:\"created_at\",\'\"\',\'\')) ), adj_refund as ( Select distinct order_id, line_item_id, date from ( select coalesce(rl.order_id,oa.order_id) order_id, rl.line_item_id, case when rl.refund_date = oa.adjustment_date then rl.refund_date else coalesce(oa.adjustment_date, rl.refund_date) end as date from refund_line_items rl full outer join order_adjustments oa on rl.order_id = oa.order_id ) ), refund_summary as ( select ar.order_id, ar.line_item_id, ar.date, ifnull(rl.refund_quantity,0) refund_quantity, ifnull(rl.refund_subtotal,0) refund_subtotal, ifnull(div0(oa.adjustment,count(1) over (partition by ar.order_id, ar.date)),0) as Adjustment_amount, (ifnull(rl.refund_subtotal,0) - ifnull(Adjustment_amount,0)) Total_Refund from adj_refund ar left join refund_line_items rl on ar.order_id = rl.order_id and ar.date = rl.refund_date and ar.line_item_id = rl.line_item_id left join order_adjustments oa on ar.order_id = oa.order_id and ar.date = oa.adjustment_date ), aggregate_summary AS ( SELECT order_id, line_item_id, date, sum(refund_quantity) AS Refund_Quantity, sum(Total_Refund) AS Refund_Amount, sum(Adjustment_amount) AS Adjustment_Amount, sum(refund_subtotal) AS Refund_Before_Adjustment FROM refund_summary GROUP BY order_id, line_item_id, date ) SELECT asum.order_id, asum.line_item_id, sum(asum.Refund_Quantity) Quantity, sum(asum.Refund_Amount) Amount, sum(asum.Adjustment_Amount) Adjustment_Amount, sum(asum.Refund_Before_Adjustment) Refund_Before_Adjustment, ARRAY_AGG( Object_construct( \'Refund_Date\', asum.date, \'Refund_Quantity\', ifnull(to_varchar(CAST(asum.Refund_Quantity AS DECIMAL(38,2))), \'0\'), \'Adjustment_Amount\', ifnull(to_varchar(CAST(asum.Adjustment_Amount AS DECIMAL(38,2))), \'0\'), \'Refund_Amount\', ifnull(to_varchar(CAST(asum.Refund_Amount AS DECIMAL(38,2))), \'0\') ) ) AS Refund_Details FROM aggregate_summary asum GROUP BY asum.order_id, asum.line_item_id; CREATE OR REPLACE TABLE RPSG_DB.maplemonk.Shopify_DRV_All_orders_fulfillment AS select * from ( select A.value:order_id AS order_id, B.value:id Line_Item_ID, replace(A.VALUE:tracking_number,\'\"\',\'\') as AWB, Upper(replace(A.VALUE:tracking_company,\'\"\',\'\')) as Courier, Upper(replace(A.VALUE:shipment_status,\'\"\',\'\')) as Shipping_status, replace(A.VALUE:updated_at,\'\"\',\'\') as shipping_status_update_date, replace(A.VALUE:tracking_url,\'\"\',\'\') as tracking_url, replace(A.VALUE:created_at,\'\"\',\'\') as Shipping_created_at, row_number() over(partition by order_id,Line_Item_ID order by shipping_status_update_date)rw FROM RPSG_DB.maplemonk.Shopify_DRV_All_orders, LATERAL FLATTEN (INPUT => fulfillments)A,LATERAL FLATTEN (INPUT => A.value:line_items)B ) where rw=1; create or replace table RPSG_DB.maplemonk.Shopify_DRV_shipping_lines_discount as SELECT id as Order_ID, SUM(ifnull(CAST(da.value:amount AS FLOAT),0)) AS total_shipping_discount FROM RPSG_DB.maplemonk.shopify_DRV_all_orders, LATERAL FLATTEN(input => shipping_lines) AS sl, LATERAL FLATTEN(input => sl.value:discount_allocations) AS da group by 1; CREATE OR REPLACE TABLE RPSG_DB.maplemonk.Shopify_DRV_All_orders_items AS WITH CTE AS (SELECT SHOP_NAME, ID::VARCHAR(16777216) AS ORDER_ID, NAME AS ORDER_NAME, CUSTOMER, replace(customer:default_address:name,\'\"\',\'\') NAME, PHONE, EMAIL, tags, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:sku::STRING AS SKU, A.VALUE:product_id::STRING AS PRODUCT_ID, A.VALUE:title::STRING AS PRODUCT_NAME, CURRENCY, cancelled_at, CASE WHEN cancelled_at IS NOT NULL THEN \'CANCELLED\' ELSE \'Shopify_Processed\' END AS order_status, CREATED_AT::DATETIME AS order_timestamp, A.VALUE:price::FLOAT * A.VALUE:quantity::FLOAT AS LINE_ITEM_SALES, (TOTAL_SHIPPING_PRICE_SET:presentment_money:amount::FLOAT/ COUNT(ORDER_ID) OVER(PARTITION BY ORDER_ID ORDER BY ORDER_ID)) AS SHIPPING_PRICE, A.value:price/(1+A.value:tax_lines:rate), A.VALUE:quantity::FLOAT as QUANTITY, \'Shopify\' AS Source ,Shopifyql_LAST_VISIT_UTM_CAMPAIGN ,Shopifyql_LAST_MOMENT_UTM_MEDIUM ,Shopifyql_FIRSTVISIT_UTM_MEDIUM ,Shopifyql_FIRSTVISIT_UTM_CAMPAIGN ,ShopifyQL_MAPPED_FIRSTCLICK_SOURCE ,ShopifyQL_MAPPED_FIRSTCLICK_CHANNEL ,SHOPIFYQL_LAST_MOMENT_UTM_SOURCE ,SHOPIFYQL_FIRSTVISIT_UTM_SOURCE ,SHOPIFYQL_LAST_VISIT_NON_UTM_SOURCE ,MOMENTS_COUNT ,DAYSTOCONVERT ,GOKWIK_UTM_SOURCE ,GOKWIK_UTM_MEDIUM ,GOKWIK_UTM_CAMPAIGN ,shopifyql_mapped_channel ,shopifyql_mapped_source ,GOKWIK_MAPPED_CHANNEL ,GOKWIK_MAPPED_MODE ,GOKWIK_UTM_CONTENT ,Referrer_Name ,FINAL_UTM_CAMPAIGN ,FINAL_UTM_SOURCE ,FINAL_UTM_CHANNEL ,SHIPPING_ADDRESS FROM RPSG_DB.maplemonk.Shopify_DRV_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A) SELECT CTE.*, IFNULL(T.TAX_RATE,0) AS TAX_RATE, IFNULL(D.DISCOUNT,0) AS DISCOUNT, CTE.LINE_ITEM_SALES AS GROSS_SALES_AFTER_TAX, CTE.LINE_ITEM_SALES/(1+IFNULL(T.TAX_RATE,0)) AS GROSS_SALES_BEFORE_TAX, IFNULL(D.DISCOUNT,0)/(1+IFNULL(T.TAX_RATE,0)) AS DISCOUNT_BEFORE_TAX, CTE.LINE_ITEM_SALES/(1+IFNULL(T.TAX_RATE,0)) - IFNULL(D.DISCOUNT,0)/(1+IFNULL(T.TAX_RATE,0)) AS NET_SALES_BEFORE_TAX, IFNULL(T.TAX,0) AS TAX, (CTE.LINE_ITEM_SALES/(1+IFNULL(T.TAX_RATE,0))) - (IFNULL(D.DISCOUNT,0)/(1+IFNULL(T.TAX_RATE,0))) + IFNULL(T.TAX,0) + CTE.SHIPPING_PRICE AS TOTAL_SALES, CASE WHEN R.QUANTITY IS NOT NULL THEN 1 ELSE 0 END AS IS_REFUND, F.AWB AWB, F.SHIPPING_STATUS Shopify_Shipping_Status, F.SHIPPING_STATUS_UPDATE_DATE Shopify_Shipping_Updated_Date, F.COURIER SHOPIFY_COURIER, div0(ifnull(SD.TOTAL_SHIPPING_DISCOUNT,0),count(1) over (partition by CTE.order_id)) TOTAL_SHIPPING_DISCOUNT, R.REFUND_DETAILS FROM CTE LEFT JOIN RPSG_DB.maplemonk.Shopify_DRV_All_orders_items_tax T ON CTE.ORDER_ID = T.ORDER_ID AND CTE.LINE_ITEM_ID = T.LINE_ITEM_ID LEFT JOIN RPSG_DB.maplemonk.Shopify_DRV_All_orders_items_discount D ON CTE.ORDER_ID = D.ORDER_ID AND CTE.LINE_ITEM_ID = D.LINE_ITEM_ID LEFT JOIN RPSG_DB.maplemonk.Shopify_DRV_All_Refunds R ON CTE.ORDER_ID = R.ORDER_ID AND CTE.LINE_ITEM_ID = R.LINE_ITEM_ID LEFT JOIN RPSG_DB.maplemonk.Shopify_DRV_All_orders_fulfillment F ON CTE.ORDER_ID = F.ORDER_ID AND CTE.LINE_ITEM_ID = F.LINE_ITEM_ID LEFT JOIN RPSG_DB.maplemonk.Shopify_DRV_shipping_lines_discount SD ON CTE.ORDER_ID = SD.ORDER_ID; CREATE OR REPLACE TABLE RPSG_DB.maplemonk.FACT_ITEMS_SHOPIFY_DRV AS SELECT O.SHOP_NAME, O.ORDER_ID, O.ORDER_NAME, O.CUSTOMER:id::int AS customer_id, O.Name, O.EMAIL, O.PHONE, O.Tags, O.LINE_ITEM_ID, O.SKU, O.PRODUCT_ID, O.CURRENCY, O.IS_REFUND, upper(replace(O.shipping_address:city,\'\"\',\'\')) as city, upper(replace(O.shipping_address:province,\'\"\',\'\')) as State, CASE WHEN P.title = \'\' THEN \'NA\' ELSE P.title END AS product_name, CASE WHEN P.product_type = \'\' THEN \'NA\' ELSE P.product_type END AS category, O.order_status, O.order_timestamp, O.LINE_ITEM_SALES, (ifnull(O.SHIPPING_PRICE,0)-ifnull(O.TOTAL_SHIPPING_DISCOUNT,0)) as SHIPPING_PRICE, O.QUANTITY, O.TAX, O.TAX_RATE, O.DISCOUNT, O.DISCOUNT_BEFORE_TAX, O.GROSS_SALES_AFTER_TAX, O.GROSS_SALES_BEFORE_TAX, O.NET_SALES_BEFORE_TAX, O.TOTAL_SALES, O.Source, O.MOMENTS_COUNT, O.DAYSTOCONVERT, O.SHOPIFYQL_FIRSTVISIT_UTM_SOURCE, O.SHOPIFYQL_MAPPED_SOURCE, O.SHOPIFYQL_MAPPED_CHANNEL, O.SHOPIFYQL_LAST_MOMENT_UTM_SOURCE, O.Shopifyql_LAST_VISIT_NON_UTM_SOURCE, O.Shopifyql_FIRSTVISIT_UTM_MEDIUM, O.Shopifyql_LAST_MOMENT_UTM_MEDIUM, O.ShopifyQL_MAPPED_FIRSTCLICK_SOURCE, O.ShopifyQL_MAPPED_FIRSTCLICK_CHANNEL, O.GOKWIK_UTM_SOURCE, O.GOKWIK_UTM_MEDIUM, O.GOKWIK_UTM_CAMPAIGN, O.GOKWIK_MAPPED_CHANNEL, O.GOKWIK_MAPPED_MODE, O.GOKWIK_UTM_CONTENT, O.FINAL_UTM_CHANNEL, O.FINAL_UTM_SOURCE, O.FINAL_UTM_CAMPAIGN, O.Referrer_Name, O.REFUND_DETAILS, O.AWB, O.SHOPIFY_SHIPPING_STATUS, O.SHOPIFY_SHIPPING_UPDATED_DATE, O.SHOPIFY_COURIER, O.cancelled_at FROM RPSG_DB.maplemonk.Shopify_DRV_All_orders_items O LEFT JOIN RPSG_DB.maplemonk.Shopify_DRV_All_products P ON O.PRODUCT_ID = P.id LEFT JOIN(SELECT customer_id, city, province, row_number() OVER ( partition BY customer_id ORDER BY id DESC) rowid FROM RPSG_DB.maplemonk.Shopify_DRV_All_customers_addresses) AS CD ON O.CUSTOMER:id::int = CD.customer_id AND CD.rowid = 1; ALTER TABLE RPSG_DB.maplemonk.FACT_ITEMS_SHOPIFY_DRV ADD COLUMN new_customer_flag varchar(50); ALTER TABLE RPSG_DB.maplemonk.FACT_ITEMS_SHOPIFY_DRV ADD COLUMN new_customer_flag_month varchar(50); ALTER TABLE RPSG_DB.maplemonk.FACT_ITEMS_SHOPIFY_DRV ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE RPSG_DB.maplemonk.FACT_ITEMS_SHOPIFY_DRV ADD COLUMN acquisition_product varchar(16777216); UPDATE RPSG_DB.maplemonk.FACT_ITEMS_SHOPIFY_DRV AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id, order_timestamp, CASE WHEN order_timestamp <> Min(order_timestamp) OVER ( partition BY customer_id) THEN \'Repeat\' ELSE \'New\' END AS Flag FROM RPSG_DB.maplemonk.FACT_ITEMS_SHOPIFY_DRV)AS B WHERE A.order_id = B.order_id AND A.customer_id = B.customer_id; UPDATE RPSG_DB.maplemonk.FACT_ITEMS_SHOPIFY_DRV SET new_customer_flag = CASE WHEN new_customer_flag IS NULL THEN \'New\' ELSE new_customer_flag END; CREATE OR replace temporary TABLE RPSG_DB.maplemonk.temp_source AS SELECT DISTINCT customer_id, source FROM ( SELECT DISTINCT customer_id, order_timestamp, source, Min(order_timestamp) OVER ( partition BY customer_id) firstOrderdate FROM RPSG_DB.maplemonk.FACT_ITEMS_SHOPIFY_DRV)res WHERE order_timestamp=firstorderdate; UPDATE RPSG_DB.maplemonk.FACT_ITEMS_SHOPIFY_DRV AS a SET a.acquisition_channel=b.source FROM RPSG_DB.maplemonk.temp_source b WHERE a.customer_id = b.customer_id; UPDATE RPSG_DB.maplemonk.FACT_ITEMS_SHOPIFY_DRV AS A SET A.new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, customer_id, order_timestamp, CASE WHEN Last_day(order_timestamp, \'month\') <> Last_day(Min(order_timestamp) OVER ( partition BY customer_id)) THEN \'Repeated\' ELSE \'New\' END AS Flag FROM RPSG_DB.maplemonk.FACT_ITEMS_SHOPIFY_DRV)AS B WHERE A.order_id = B.order_id AND A.customer_id = B.customer_id; CREATE OR replace temporary TABLE RPSG_DB.maplemonk.temp_product AS SELECT DISTINCT customer_id, product_name, Row_number() OVER (partition BY customer_id ORDER BY LINE_ITEM_SALES DESC) rowid FROM ( SELECT DISTINCT customer_id, order_timestamp, product_name, LINE_ITEM_SALES , Min(order_timestamp) OVER (partition BY customer_id) firstOrderdate FROM RPSG_DB.maplemonk.FACT_ITEMS_SHOPIFY_DRV )res WHERE order_timestamp=firstorderdate; UPDATE RPSG_DB.maplemonk.FACT_ITEMS_SHOPIFY_DRV AS A SET A.acquisition_product=B.product_name FROM ( SELECT * FROM RPSG_DB.maplemonk.temp_product WHERE rowid=1)B WHERE A.customer_id = B.customer_id; CREATE OR REPLACE TABLE RPSG_DB.maplemonk.FACT_ITEMS_SHOPIFY_DRV AS SELECT *, RI.ISO_3166_2_CODE AS ISO_Region_Code FROM RPSG_DB.maplemonk.FACT_ITEMS_SHOPIFY_DRV FI left join RPSG_DB.maplemonk.region_iso_3166_codes RI on Upper(FI.state) = Upper(RI.Subdivision_name);",
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
                        