{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE TABLE IF NOT EXISTS prd_db.justherbs.dwh_UTM_MAPPING ( UTM_SOURCE VARCHAR(16777216), UTM_MEDIUM VARCHAR(16777216), CHANNEL VARCHAR(16777216)); CREATE TABLE IF NOT EXISTS prd_db.justherbs.dwh_SKU_MASTER ( skucode VARCHAR(16777216), name VARCHAR(16777216), category VARCHAR(16777216), sub_category VARCHAR(16777216)); CREATE OR REPLACE TABLE prd_db.justherbs.dwh_Shopify_All_customers AS select *,\'Shopify_justherbs\' AS Shop_Name from datalake_db.justherbs.trn_Shopify_jh_CUSTOMERS ; create or replace table prd_db.justherbs.dwh_Shopify_jh_UTM_Parameters as select ShopifyQL.* ,upper(coalesce(UTM_MAPPING.CHANNEL,UTM_MAPPING_REF.CHANNEL,ShopifyQL.ShopifyQL_Unmapped_Last_Source)) as ShopifyQL_MAPPED_CHANNEL ,upper(coalesce(UTM_MAPPING.UTM_SOURCE,UTM_MAPPING_REF.UTM_SOURCE,ShopifyQL.ShopifyQL_Unmapped_Last_Source)) as ShopifyQL_MAPPED_SOURCE from (select * from (select A.id ,A.name ,A.createdat ,replace(A.customerjourneysummary:\"momentsCount\",\'\"\',\'\') Moments_Count ,replace(A.customerjourneysummary:\"daysToConversion\",\'\"\',\'\') DaysToConvert ,replace(A.CUSTOMERJOURNEYSUMMARY:\"lastVisit\":\"utmParameters\":\"source\",\'\"\',\'\') LastVisit_UTM_Source ,replace(A.CUSTOMERJOURNEYSUMMARY:\"lastVisit\":\"source\",\'\"\',\'\') LastVisit_NON_UTM_Source ,replace(A.CUSTOMERJOURNEYSUMMARY:\"lastVisit\":\"utmParameters\":\"campaign\",\'\"\',\'\') LastVisit_UTM_Campaign ,replace(A.CUSTOMERJOURNEYSUMMARY:\"firstVisit\":\"utmParameters\":\"source\",\'\"\',\'\') FirstVisit_UTM_Source ,replace(A.CUSTOMERJOURNEYSUMMARY:\"firstVisit\":\"utmParameters\":\"medium\",\'\"\',\'\') FirstVisit_UTM_Medium ,replace(A.CUSTOMERJOURNEYSUMMARY:\"firstVisit\":\"utmParameters\":\"campaign\",\'\"\',\'\') FirstVisit_UTM_Campaign ,replace(B.Value:\"id\",\'gid://shopify/CustomerVisit/\',\'\') Moment_ID ,replace(B.value:\"utmParameters\":\"source\",\'\"\',\'\') Last_Moment_UTM_Source ,replace(B.value:\"utmParameters\":\"medium\",\'\"\',\'\') Last_Moment_UTM_Medium ,case when Moments_Count >1 then LastVisit_UTM_Source else FirstVisit_UTM_Source end CJSummary_utm_source ,referrerdisplaytext Referrer_Name ,customerjourneysummary ,customerjourney ,coalesce(Last_Moment_UTM_Source,LastVisit_NON_UTM_Source) ShopifyQL_Unmapped_Last_Source ,rank() over (partition by name order by MOMENT_ID desc) rw from datalake_db.justherbs.TRN_SHOPIFY_JH_UTM_PARAMETERS A, lateral flatten (INPUT => customerjourney:\"moments\",OUTER => TRUE) B ) where rw=1 ) ShopifyQL left join (select * from (select * , row_number() over (partition by lower(utm_source) order by 1) rw from datalake_db.justherbs.mst_utm_mapping) where rw=1 and utm_source is not null ) UTM_MAPPING on lower(ShopifyQL.ShopifyQL_Unmapped_Last_Source) = lower(UTM_MAPPING.utm_source) left join (select * from (select * , row_number() over (partition by lower(utm_source) order by 1) rw from datalake_db.justherbs.mst_utm_mapping) where rw=1 and utm_source is not null ) UTM_MAPPING_REF on lower(ShopifyQL.referrer_name) = lower(UTM_MAPPING_REF.utm_source) left join (select * from (select * , row_number() over (partition by lower(utm_source) order by 1) rw from datalake_db.justherbs.mst_utm_mapping) where rw=1 and utm_source is not null ) UTM_MAPPING_FIRST_CLICK on lower(ShopifyQL.FirstVisit_UTM_Source) = lower(UTM_MAPPING_FIRST_CLICK.utm_source) ; create or replace table prd_db.justherbs.dwh_GOKWIK_SOURCE as With GO_KWIK as ( WITH utm_source_cte AS ( SELECT S.id, UPPER(A.value:\"value\") AS utm_source FROM datalake_db.justherbs.trn_Shopify_jh_ORDERS S, LATERAL FLATTEN(INPUT => note_attributes) A where (LOWER(S.note_attributes) LIKE \'%gokwik%\' or LOWER(S.tags) LIKE \'%gokwik%\') AND LOWER(A.value:\"name\") = \'utm_source\' ), utm_medium_cte AS ( SELECT S.id, UPPER(A.value:\"value\") AS utm_medium FROM datalake_db.justherbs.trn_Shopify_jh_ORDERS S, LATERAL FLATTEN(INPUT => note_attributes) A where (LOWER(S.note_attributes) LIKE \'%gokwik%\' or LOWER(S.tags) LIKE \'%gokwik%\') AND LOWER(A.value:\"name\") = \'utm_medium\' ), utm_campaign_cte AS ( SELECT S.id, UPPER(A.value:\"value\") AS utm_campaign FROM datalake_db.justherbs.trn_Shopify_jh_ORDERS S, LATERAL FLATTEN(INPUT => note_attributes) A where (LOWER(S.note_attributes) LIKE \'%gokwik%\' or LOWER(S.tags) LIKE \'%gokwik%\') AND LOWER(A.value:\"name\") = \'utm_campaign\' ), utm_content_cte AS ( SELECT S.id ,UPPER(A.value:\"value\") AS utm_content FROM datalake_db.justherbs.trn_Shopify_jh_ORDERS S, LATERAL FLATTEN(INPUT => note_attributes) A where (LOWER(S.note_attributes) LIKE \'%gokwik%\' or LOWER(S.tags) LIKE \'%gokwik%\') AND LOWER(A.value:\"name\") = \'utm_content\' ) SELECT S.id, S.tags, source_cte.utm_source AS GOKWIK_UTM_SOURCE, medium_cte.utm_medium AS GOKWIK_UTM_MEDIUM, campaign_cte.utm_campaign AS GOKWIK_UTM_CAMPAIGN, content_cte.utm_content AS GOKWIK_UTM_CONTENT FROM datalake_db.justherbs.trn_Shopify_jh_ORDERS S LEFT JOIN utm_source_cte source_cte ON S.id = source_cte.id LEFT JOIN utm_medium_cte medium_cte ON S.id = medium_cte.id LEFT JOIN utm_campaign_cte campaign_cte ON S.id = campaign_cte.id LEFT JOIN utm_content_cte content_cte ON S.id = content_cte.id WHERE LOWER(S.note_attributes) LIKE \'%gokwik%\' ) Select GO_KWIK.* ,coalesce(Upper(UTM_MAPPING.CHANNEL), upper(GOKWIK_UTM_MEDIUM)) as GOKWIK_MAPPED_CHANNEL ,coalesce(Upper(UTM_MAPPING.CHANNEL), upper(GOKWIK_UTM_SOURCE)) as GOKWIK_MAPPED_SOURCE from GO_KWIK left join (select * from (select * , row_number() over (partition by lower(concat(ifnull(utm_source,\'\'),ifnull(utm_medium,\'\'))) order by 1) rw from datalake_db.justherbs.mst_utm_mapping) where rw=1 and lower(concat(ifnull(utm_source,\'\'),ifnull(utm_medium,\'\'))) is not null ) UTM_MAPPING on lower(concat(ifnull(GO_KWIK.GOKWIK_UTM_SOURCE,\'\'),ifnull(GOKWIK_UTM_MEDIUM,\'\'))) = lower(concat(ifnull(UTM_MAPPING.utm_source,\'\'),ifnull(UTM_MAPPING.utm_medium,\'\'))) ; create or replace table prd_db.justherbs.dwh_Shopify_All_orders as select AO.* ,UPPER(ShopifyQL.shopifyql_mapped_channel) shopifyql_mapped_channel ,UPPER(ShopifyQL.shopifyql_mapped_source) shopifyql_mapped_source ,UPPER(ShopifyQL.FIRSTVISIT_UTM_SOURCE) Shopifyql_FIRSTVISIT_UTM_SOURCE ,UPPER(ShopifyQL.FirstVisit_UTM_Campaign) Shopifyql_FIRSTVISIT_UTM_CAMPAIGN ,upper(ShopifyQL.LastVisit_UTM_Campaign) Shopifyql_LAST_VISIT_UTM_CAMPAIGN ,UPPER(ShopifyQL.LAST_MOMENT_UTM_SOURCE) Shopifyql_LAST_MOMENT_UTM_SOURCE ,UPPER(ShopifyQL.LastVisit_NON_UTM_Source) Shopifyql_LAST_VISIT_NON_UTM_SOURCE ,UPPER(ShopifyQL.LAST_MOMENT_UTM_MEDIUM) Shopifyql_LAST_MOMENT_UTM_MEDIUM ,UPPER(ShopifyQL.FIRSTVISIT_UTM_MEDIUM) Shopifyql_FIRSTVISIT_UTM_MEDIUM ,div0(ShopifyQL.MOMENTS_COUNT,count(1) over (partition by AO.name order by 1)) MOMENTS_COUNT ,div0(ShopifyQL.DAYSTOCONVERT,count(1) over (partition by AO.name order by 1)) DAYSTOCONVERT ,UPPER(GOKWIK.GOKWIK_UTM_SOURCE) GOKWIK_UTM_SOURCE ,UPPER(GOKWIK.GOKWIK_UTM_MEDIUM) GOKWIK_UTM_MEDIUM ,UPPER(GOKWIK.GOKWIK_UTM_CONTENT) GOKWIK_UTM_CONTENT ,UPPER(GOKWIK.GOKWIK_MAPPED_CHANNEL) GOKWIK_MAPPED_CHANNEL ,UPPER(GOKWIK.GOKWIK_MAPPED_SOURCE) GOKWIK_MAPPED_SOURCE ,UPPER(Referrer_Name) Referrer_Name ,UPPER(GOKWIK.GOKWIK_UTM_CAMPAIGN) GOKWIK_UTM_CAMPAIGN ,UPPER(coalesce(GOKWIK_UTM_CAMPAIGN,LastVisit_UTM_Campaign)) FINAL_UTM_CAMPAIGN ,Upper(coalesce(GOKWIK_MAPPED_SOURCE,shopifyql_mapped_source,\'Direct\')) FINAL_UTM_SOURCE ,Upper(coalesce(GOKWIK_MAPPED_CHANNEL,shopifyql_mapped_channel,\'Direct\')) FINAL_UTM_CHANNEL from (select * ,\'Shopify_justherbs\' AS Shop_Name from datalake_db.justherbs.trn_Shopify_jh_ORDERS) AO left join (select * from (select *, row_number() over (partition by name order by 1) rwb from prd_db.justherbs.DWH_SHOPIFY_JH_UTM_PARAMETERS ) where rwb = 1) ShopifyQL on AO.name = ShopifyQL.name left join prd_db.justherbs.dwh_GOKWIK_SOURCE GOKWIK on AO.ID = GOKWIK.ID ; ALTER TABLE prd_db.justherbs.dwh_Shopify_All_orders RENAME COLUMN _AIRBYTE_trn_Shopify_jh_ORDERS_HASHID to _AIRBYTE_ORDERS_HASHID; CREATE OR REPLACE TABLE prd_db.justherbs.dwh_Shopify_All_products AS select *,\'Shopify_justherbs\' AS Shop_Name from datalake_db.justherbs.trn_Shopify_jh_PRODUCTS ; ALTER TABLE prd_db.justherbs.dwh_Shopify_All_products RENAME COLUMN _AIRBYTE_trn_Shopify_jh_PRODUCTS_HASHID to _AIRBYTE_PRODUCTS_HASHID; CREATE OR REPLACE TABLE prd_db.justherbs.dwh_Shopify_All_products_variants AS select *,\'Shopify_justherbs\' AS Shop_Name from datalake_db.justherbs.trn_Shopify_jh_PRODUCTS_VARIANTS ; ALTER TABLE prd_db.justherbs.dwh_SHOPIFY_ALL_PRODUCTS_VARIANTS RENAME COLUMN _AIRBYTE_trn_Shopify_jh_PRODUCTS_HASHID to _AIRBYTE_PRODUCTS_HASHID; CREATE OR REPLACE TABLE prd_db.justherbs.dwh_Shopify_All_customers_addresses AS select *,\'Shopify_justherbs\' AS Shop_Name from datalake_db.justherbs.trn_Shopify_jh_CUSTOMERS_ADDRESSES ; CREATE OR REPLACE TABLE prd_db.justherbs.dwh_Shopify_All_orders_items_discount AS SELECT order_id, LINE_ITEM_ID, SUM(B.VALUE:amount::FLOAT) AS DISCOUNT FROM( SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:discount_allocations AS discount_allocations FROM prd_db.justherbs.dwh_Shopify_All_orders , LATERAL FLATTEN (INPUT => LINE_ITEMS)A)x,LATERAL FLATTEN (INPUT => x.discount_allocations)B GROUP BY ORDER_ID, LINE_ITEM_ID; CREATE OR REPLACE TABLE prd_db.justherbs.dwh_Shopify_All_orders_items_tax AS SELECT order_id, LINE_ITEM_ID, SUM(B.VALUE:price::FLOAT) AS TAX, sum(B.VALUE:rate::float) as Tax_Rate FROM( SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:tax_lines AS tax_lines FROM prd_db.justherbs.DWH_SHOPIFY_ALL_ORDERS, LATERAL FLATTEN (INPUT => LINE_ITEMS)A)x,LATERAL FLATTEN (INPUT => x.tax_lines)B GROUP BY order_id, LINE_ITEM_ID; CREATE OR REPLACE TABLE prd_db.justherbs.dwh_Shopify_All_Refunds AS WITH refund_line_items AS ( SELECT refunds.value:order_id::STRING AS order_id, line_items.value:line_item_id::string as LINE_ITEM_ID, try_to_timestamp(replace(refunds.value:\"created_at\",\'\"\',\'\')) AS refund_date, sum(ifnull(line_items.value:quantity::INT,0)) AS refund_quantity, sum(ifnull(line_items.value:subtotal::FLOAT,0)) AS refund_subtotal FROM prd_db.justherbs.dwh_Shopify_All_orders, LATERAL FLATTEN(input => dwh_Shopify_All_orders.refunds) refunds, LATERAL FLATTEN(input => refunds.value:refund_line_items) line_items group by refunds.value:order_id::STRING, line_items.value:line_item_id::string, try_to_timestamp(replace(refunds.value:\"created_at\",\'\"\',\'\')) ), order_adjustments AS ( SELECT order_adj.value:order_id::STRING AS order_id, try_to_timestamp(replace(refunds.value:\"created_at\",\'\"\',\'\')) adjustment_date, sum(ifnull(order_adj.value:amount::FLOAT,0)) AS adjustment FROM prd_db.justherbs.dwh_Shopify_All_orders, LATERAL FLATTEN(input => dwh_Shopify_All_orders.refunds) refunds, LATERAL FLATTEN(input => refunds.value:order_adjustments) order_adj group by order_adj.value:order_id::STRING, try_to_timestamp(replace(refunds.value:\"created_at\",\'\"\',\'\')) ), adj_refund as ( Select distinct order_id, line_item_id, date from ( select coalesce(rl.order_id,oa.order_id) order_id, rl.line_item_id, case when rl.refund_date = oa.adjustment_date then rl.refund_date else coalesce(oa.adjustment_date, rl.refund_date) end as date from refund_line_items rl full outer join order_adjustments oa on rl.order_id = oa.order_id ) ), refund_summary as ( select ar.order_id, ar.line_item_id, ar.date, ifnull(rl.refund_quantity,0) refund_quantity, ifnull(rl.refund_subtotal,0) refund_subtotal, ifnull(div0(oa.adjustment,count(1) over (partition by ar.order_id, ar.date)),0) as Adjustment_amount, (ifnull(rl.refund_subtotal,0) - ifnull(Adjustment_amount,0)) Total_Refund from adj_refund ar left join refund_line_items rl on ar.order_id = rl.order_id and ar.date = rl.refund_date and ar.line_item_id = rl.line_item_id left join order_adjustments oa on ar.order_id = oa.order_id and ar.date = oa.adjustment_date ), aggregate_summary AS ( SELECT order_id, line_item_id, date, sum(refund_quantity) AS Refund_Quantity, sum(Total_Refund) AS Refund_Amount, sum(Adjustment_amount) AS Adjustment_Amount, sum(refund_subtotal) AS Refund_Before_Adjustment FROM refund_summary GROUP BY order_id, line_item_id, date ) SELECT asum.order_id, asum.line_item_id, sum(asum.Refund_Quantity) Quantity, sum(asum.Refund_Amount) Amount, sum(asum.Adjustment_Amount) Adjustment_Amount, sum(asum.Refund_Before_Adjustment) Refund_Before_Adjustment, ARRAY_AGG( Object_construct( \'Refund_Date\', asum.date, \'Refund_Quantity\', ifnull(to_varchar(CAST(asum.Refund_Quantity AS DECIMAL(38,2))), \'0\'), \'Adjustment_Amount\', ifnull(to_varchar(CAST(asum.Adjustment_Amount AS DECIMAL(38,2))), \'0\'), \'Refund_Amount\', ifnull(to_varchar(CAST(asum.Refund_Amount AS DECIMAL(38,2))), \'0\') ) ) AS Refund_Details FROM aggregate_summary asum GROUP BY asum.order_id, asum.line_item_id; CREATE OR REPLACE TABLE prd_db.justherbs.dwh_Shopify_All_orders_fulfillment AS SELECT A.value:order_id AS order_id, B.value:id Line_Item_ID, replace(A.VALUE:tracking_number,\'\"\',\'\') as AWB, Upper(replace(A.VALUE:tracking_company,\'\"\',\'\')) as Courier, Upper(replace(A.VALUE:shipment_status,\'\"\',\'\')) as Shipping_status, replace(A.VALUE:updated_at,\'\"\',\'\') as shipping_status_update_date, replace(A.VALUE:tracking_url,\'\"\',\'\') as tracking_url, replace(A.VALUE:created_at,\'\"\',\'\') as Shipping_created_at FROM prd_db.justherbs.DWH_SHOPIFY_ALL_ORDERS, LATERAL FLATTEN (INPUT => fulfillments)A,LATERAL FLATTEN (INPUT => A.value:line_items)B ; CREATE OR REPLACE TABLE prd_db.justherbs.dwh_Shopify_All_discount_codes AS select ID, CODE from ( SELECT id, replace(A.value:code , \'\"\',\'\') code, row_number() over (partition by id order by code) rw FROM prd_db.justherbs.DWH_SHOPIFY_ALL_ORDERS, LATERAL FLATTEN (INPUT => discount_codes) A )where rw=1 ; CREATE OR REPLACE TABLE prd_db.justherbs.dwh_Shopify_All_orders_items AS WITH CTE AS (SELECT SHOP_NAME, ID::VARCHAR(16777216) AS ORDER_ID, NAME AS ORDER_NAME, CUSTOMER, replace(customer:default_address:name,\'\"\',\'\') NAME, coalesce(phone, replace(customer:default_address:phone,\'\"\',\'\')) phone, coalesce(email, replace(customer:email,\'\"\',\'\')) email, replace(shipping_address:zip,\'\"\',\'\') pincode, tags, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:sku::STRING AS SKU, A.VALUE:product_id::STRING AS PRODUCT_ID, A.VALUE:title::STRING AS PRODUCT_NAME, CURRENCY, CASE WHEN cancelled_at IS NOT NULL THEN \'CANCELLED\' ELSE \'Shopify_Processed\' END AS order_status, CREATED_AT::DATETIME AS order_timestamp, A.VALUE:price::FLOAT offer_price, A.VALUE:price::FLOAT * A.VALUE:quantity::FLOAT AS LINE_ITEM_SALES, (TOTAL_SHIPPING_PRICE_SET:presentment_money:amount::FLOAT/ COUNT(ORDER_ID) OVER(PARTITION BY ORDER_ID ORDER BY ORDER_ID)) AS SHIPPING_PRICE, A.value:price/(1+A.value:tax_lines:rate), A.VALUE:quantity::FLOAT as QUANTITY, \'Shopify\' AS Source, MOMENTS_COUNT, DAYSTOCONVERT, SHOPIFYQL_FIRSTVISIT_UTM_SOURCE, SHOPIFYQL_MAPPED_CHANNEL, SHOPIFYQL_LAST_MOMENT_UTM_SOURCE, Shopifyql_LAST_VISIT_NON_UTM_SOURCE, Shopifyql_LAST_MOMENT_UTM_MEDIUM, Shopifyql_FIRSTVISIT_UTM_MEDIUM, Shopifyql_FIRSTVISIT_UTM_CAMPAIGN, Shopifyql_LAST_VISIT_UTM_CAMPAIGN, FINAL_UTM_CHANNEL, FINAL_UTM_SOURCE, FINAL_UTM_CAMPAIGN, Referrer_Name, NULL as product_sub_category, payment_gateway_names, discount_codes FROM prd_db.justherbs.DWH_SHOPIFY_ALL_ORDERS, LATERAL FLATTEN (INPUT => LINE_ITEMS)A) SELECT CTE.*, dc.code discount_code, IFNULL(T.TAX_RATE,0) AS TAX_RATE, IFNULL(D.DISCOUNT,0) AS DISCOUNT, CTE.LINE_ITEM_SALES AS GROSS_SALES_AFTER_TAX, CTE.LINE_ITEM_SALES/(1+IFNULL(T.TAX_RATE,0)) AS GROSS_SALES_BEFORE_TAX, IFNULL(D.DISCOUNT,0)/(1+IFNULL(T.TAX_RATE,0)) AS DISCOUNT_BEFORE_TAX, CTE.LINE_ITEM_SALES/(1+IFNULL(T.TAX_RATE,0)) - IFNULL(D.DISCOUNT,0)/(1+IFNULL(T.TAX_RATE,0)) AS NET_SALES_BEFORE_TAX, case when IFNULL(T.TAX,0) = 0 then CTE.LINE_ITEM_SALES*tax_rate else IFNULL(T.TAX,0) end AS TAX, CTE.LINE_ITEM_SALES - IFNULL(D.DISCOUNT,0) + CTE.SHIPPING_PRICE AS TOTAL_SALES, CASE WHEN R.QUANTITY IS NOT NULL THEN 1 ELSE 0 END AS IS_REFUND, R.REFUND_DETAILS, R.QUANTITY Refund_Quantity, R.AMOUNT Refund_Value, F.AWB AWB, F.SHIPPING_STATUS Shopify_Shipping_Status, F.SHIPPING_STATUS_UPDATE_DATE Shopify_Shipping_Updated_Date, F.COURIER SHOPIFY_COURIER, FROM CTE LEFT JOIN prd_db.justherbs.DWH_SHOPIFY_ALL_ORDERS_ITEMS_TAX T ON CTE.ORDER_ID = T.ORDER_ID AND CTE.LINE_ITEM_ID = T.LINE_ITEM_ID LEFT JOIN prd_db.justherbs.dwh_Shopify_All_orders_items_discount D ON CTE.ORDER_ID = D.ORDER_ID AND CTE.LINE_ITEM_ID = D.LINE_ITEM_ID LEFT JOIN prd_db.justherbs.DWH_SHOPIFY_ALL_REFUNDS R ON CTE.ORDER_ID = R.ORDER_ID AND CTE.LINE_ITEM_ID = R.LINE_ITEM_ID LEFT JOIN prd_db.justherbs.DWH_SHOPIFY_ALL_ORDERS_FULFILLMENT F ON CTE.ORDER_ID = F.ORDER_ID AND CTE.LINE_ITEM_ID = F.LINE_ITEM_ID left join (select * from( select *, row_number() over (partition by id, code order by 1) rw from prd_db.justherbs.dwh_Shopify_All_discount_codes) where rw = 1) DC on CTE.order_id = DC.id ; CREATE OR REPLACE TABLE prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS_INTERMEDIATE AS SELECT O.SHOP_NAME, O.ORDER_ID, O.ORDER_NAME, O.CUSTOMER:id::int AS customer_id, O.Name, O.EMAIL, o.pincode, O.PHONE, O.Tags, O.LINE_ITEM_ID, O.SKU, O.PRODUCT_ID, O.CURRENCY, O.IS_REFUND, CASE WHEN CD.city IS NULL OR CD.city = \'\' THEN \'NA\' ELSE upper(CD.city) END AS city, CASE WHEN CD.province IS NULL OR CD.province = \'\' THEN \'NA\' ELSE upper(CD.province) END AS state, CASE WHEN P.title = \'\' THEN \'NA\' ELSE P.title END AS product_name, CASE WHEN P.product_type = \'\' THEN \'NA\' ELSE P.product_type END AS category, O.order_status, O.order_timestamp, offer_price, O.LINE_ITEM_SALES, O.SHIPPING_PRICE, O.QUANTITY, O.TAX, O.TAX_RATE, O.DISCOUNT, O.DISCOUNT_BEFORE_TAX, O.GROSS_SALES_AFTER_TAX, O.GROSS_SALES_BEFORE_TAX, O.NET_SALES_BEFORE_TAX, O.TOTAL_SALES, O.Source, O.MOMENTS_COUNT, O.DAYSTOCONVERT, O.SHOPIFYQL_FIRSTVISIT_UTM_SOURCE, O.SHOPIFYQL_MAPPED_CHANNEL, O.SHOPIFYQL_LAST_MOMENT_UTM_SOURCE, O.Shopifyql_LAST_VISIT_NON_UTM_SOURCE, O.Shopifyql_FIRSTVISIT_UTM_MEDIUM, O.Shopifyql_LAST_MOMENT_UTM_MEDIUM, O.FINAL_UTM_CHANNEL, O.FINAL_UTM_CAMPAIGN, O.FINAL_UTM_SOURCE, O.Referrer_Name, O.AWB, O.SHOPIFY_SHIPPING_STATUS, O.SHOPIFY_SHIPPING_UPDATED_DATE, O.SHOPIFY_COURIER, O.Shopifyql_FIRSTVISIT_UTM_CAMPAIGN, O.Shopifyql_LAST_VISIT_UTM_CAMPAIGN, o.product_sub_category sub_Category, o.discount_code, payment_gateway_names, o.REFUND_DETAILS, o.Refund_Quantity, o.Refund_Value, FROM prd_db.justherbs.DWH_SHOPIFY_ALL_ORDERS_ITEMS O LEFT JOIN prd_db.justherbs.dwh_Shopify_All_products P ON O.PRODUCT_ID = P.id LEFT JOIN(SELECT customer_id, city, province, row_number() OVER ( partition BY customer_id ORDER BY id DESC) rowid FROM prd_db.justherbs.dwh_Shopify_All_customers_addresses) AS CD ON O.CUSTOMER:id::int = CD.customer_id AND CD.rowid = 1; CREATE OR REPLACE TABLE prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS_TEMP_Category as select fi.* ,fi.SKU AS SKU_CODE ,Upper(coalesce(p.name,fi.product_name)) as PRODUCT_NAME_Final ,coalesce(Upper(p.CATEGORY),upper(fi.category)) AS Product_Category ,coalesce(Upper(p.sub_category), upper(fi.Sub_Category)) as Product_Sub_Category ,p.BRAND from prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS_INTERMEDIATE fi left join (select * from (select SKU skucode, UPPER(product_name) name, upper(category_name) CATEGORY, null sub_category, upper(brand) Brand, row_number() over (partition by SKU order by 1) rw from datalake_db.justherbs.mst_easyecom_jh_product_master) where rw = 1 ) p on fi.sku = p.skucode; CREATE OR REPLACE TABLE prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS_INTERMEDIATE AS SELECT * FROM prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS_TEMP_Category; create or replace table prd_db.justherbs.dwh_Shopify_Final_customerID as with new_phone_numbers as ( select phone, contact_num ,19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(replace(phone,\' \',\'\'), \'[^a-zA-Z0-9]+\'),10) as contact_num, phone from prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS_INTERMEDIATE ) a ), int as ( select contact_num,email,coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email,maple_monk_id,19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email,maple_monk_id from ( select replace(phone,\' \',\'\') as contact_num,email from prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS_INTERMEDIATE ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as Shopify_customer_id_final , min(ORDER_TIMESTAMP::date) over(partition by Shopify_customer_id_final) as shopify_acquisition_date , min(case when lower(order_status) not in (\'cancelled\') then ORDER_TIMESTAMP::date end) over(partition by Shopify_customer_id_final) as shopify_first_complete_order_date , m.* from ( select c.maple_monk_id as maple_monk_id_phone, o.* from prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS_INTERMEDIATE o left join (select * from (select contact_num phone,maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from prd_db.justherbs.dwh_Shopify_Final_customerID) where magic =1 )c on c.phone = right(regexp_replace(o.phone, \'[^a-zA-Z0-9]+\'),10) )m left join (select distinct maple_monk_id, email from prd_db.justherbs.dwh_Shopify_Final_customerID where contact_num is null )d on d.email = m.email ; ALTER TABLE prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS ADD COLUMN shopify_new_customer_flag varchar(50); ALTER TABLE prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS ADD COLUMN shopify_new_customer_flag_month varchar(50); ALTER TABLE prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS ADD COLUMN shopify_acquisition_product varchar(16777216); ALTER TABLE prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS ADD COLUMN shopify_acquisition_channel varchar(16777216); ALTER TABLE prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS ADD COLUMN shopify_acquisition_source varchar(16777216); UPDATE prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS AS A SET A.shopify_new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, Shopify_customer_id_final, ORDER_TIMESTAMP, CASE WHEN ORDER_TIMESTAMP::date = shopify_first_complete_order_date then \'New\' WHEN ORDER_TIMESTAMP::date < shopify_first_complete_order_date or shopify_first_complete_order_date is null THEN \'Yet to make completed order\' WHEN ORDER_TIMESTAMP::date > shopify_first_complete_order_date then \'Repeat\' END AS Flag FROM prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS )AS B WHERE A.order_id = B.order_id AND A.Shopify_customer_id_final = B.Shopify_customer_id_final AND A.ORDER_TIMESTAMP::date=B.ORDER_TIMESTAMP::Date; UPDATE prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS SET shopify_new_customer_flag = CASE WHEN shopify_new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' WHEN shopify_new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) in (\'cancelled\') end) THEN \'Yet to make completed order\' ELSE shopify_new_customer_flag END; UPDATE prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS AS A SET A.shopify_new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, shopify_customer_id_final, ORDER_TIMESTAMP::date Order_Date, CASE WHEN Last_day(ORDER_TIMESTAMP, \'month\') = Last_day(shopify_first_complete_order_date, \'month\') THEN \'New\' WHEN Last_day(ORDER_TIMESTAMP, \'month\') < Last_day(shopify_first_complete_order_date, \'month\') or shopify_acquisition_date is null THEN \'Yet to make completed order\' WHEN Last_day(order_date, \'month\') > Last_day(shopify_first_complete_order_date, \'month\') THEN \'Repeat\' END AS Flag FROM prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS )AS B WHERE A.order_id = B.order_id AND A.shopify_customer_id_final = B.shopify_customer_id_final; UPDATE prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS SET shopify_new_customer_flag_month = CASE WHEN shopify_new_customer_flag_month IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' ELSE shopify_new_customer_flag_month END; CREATE OR replace temporary TABLE prd_db.justherbs.dwh_temp_source_1 AS SELECT DISTINCT shopify_customer_id_final, channel , source FROM ( SELECT DISTINCT shopify_customer_id_final, order_timestamp::date order_Date, FINAL_UTM_SOURCE as SOURCE, FINAL_UTM_CHANNEL as CHANNEL, Min(case when lower(order_status) not in (\'cancelled\') then order_timestamp::date end) OVER (partition BY shopify_customer_id_final) firstOrderdate FROM prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS ) res WHERE order_date=firstorderdate; UPDATE prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS AS a SET a.shopify_acquisition_channel=b.channel FROM prd_db.justherbs.dwh_temp_source_1 b WHERE a.shopify_customer_id_final = b.shopify_customer_id_final; UPDATE prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS AS a SET a.shopify_acquisition_source=b.SOURCE FROM prd_db.justherbs.dwh_temp_source_1 b WHERE a.shopify_customer_id_final = b.shopify_customer_id_final; CREATE OR replace temporary TABLE prd_db.justherbs.dwh_temp_product_1 AS SELECT DISTINCT shopify_customer_id_final, product_name_final, Row_number() OVER (partition BY shopify_customer_id_final ORDER BY total_sales DESC) rowid FROM ( SELECT DISTINCT shopify_customer_id_final, order_timestamp::date order_date, product_name_final, TOTAL_SALES , Min(case when lower(order_status) not in (\'cancelled\') then order_timestamp::date end) OVER (partition BY shopify_customer_id_final) firstOrderdate FROM prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS )res WHERE order_date=firstorderdate; UPDATE prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS AS A SET A.shopify_acquisition_product=B.product_name_final FROM ( SELECT * FROM prd_db.justherbs.dwh_temp_product_1 WHERE rowid=1)B WHERE A.shopify_customer_id_final = B.shopify_customer_id_final; ALTER TABLE prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS ADD COLUMN SHIPPING_TAX FLOAT; ALTER TABLE prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS ADD COLUMN SHIP_PROMOTION_DISCOUNT FLOAT; ALTER TABLE prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS ADD COLUMN GIFT_WRAP_PRICE FLOAT; ALTER TABLE prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS ADD COLUMN GIFT_WRAP_TAX FLOAT; ALTER TABLE prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS MODIFY COLUMN ORDER_STATUS VARCHAR(100); select distinct final_utm_channel from prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS",
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
            