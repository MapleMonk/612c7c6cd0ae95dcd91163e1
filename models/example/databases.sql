{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_DB.MAPLEMONK.Shopify_UTM_Parameters as select *, coalesce(ShopifyQL_Unmapped_Last_Source,Referrer_Name) as ShopifyQL_MAPPED_SOURCE, FirstVisit_UTM_Source as ShopifyQL_MAPPED_FIRSTCLICK_SOURCE from (select A.id ,A.name ,A.createdat ,replace(A.customerjourneysummary:\"momentsCount\",\'\"\',\'\') Moments_Count ,replace(A.customerjourneysummary:\"daysToConversion\",\'\"\',\'\') DaysToConvert ,replace(A.CUSTOMERJOURNEYSUMMARY:\"lastVisit\":\"utmParameters\":\"source\",\'\"\',\'\') LastVisit_UTM_Source ,replace(A.CUSTOMERJOURNEYSUMMARY:\"lastVisit\":\"utmParameters\":\"medium\",\'\"\',\'\') LastVisit_UTM_Medium ,replace(A.CUSTOMERJOURNEYSUMMARY:\"lastVisit\":\"source\",\'\"\',\'\') LastVisit_NON_UTM_Source ,replace(A.CUSTOMERJOURNEYSUMMARY:\"lastVisit\":\"utmParameters\":\"campaign\",\'\"\',\'\') LastVisit_UTM_Campaign ,replace(A.CUSTOMERJOURNEYSUMMARY:\"firstVisit\":\"utmParameters\":\"source\",\'\"\',\'\') FirstVisit_UTM_Source ,replace(A.CUSTOMERJOURNEYSUMMARY:\"firstVisit\":\"utmParameters\":\"medium\",\'\"\',\'\') FirstVisit_UTM_Medium ,replace(A.CUSTOMERJOURNEYSUMMARY:\"firstVisit\":\"utmParameters\":\"campaign\",\'\"\',\'\') FirstVisit_UTM_Campaign ,replace(B.Value:\"id\",\'gid://shopify/CustomerVisit/\',\'\') Moment_ID ,replace(B.value:\"utmParameters\":\"source\",\'\"\',\'\') Last_Moment_UTM_Source ,replace(B.value:\"utmParameters\":\"medium\",\'\"\',\'\') Last_Moment_UTM_Medium ,case when Moments_Count >1 then LastVisit_UTM_Source else FirstVisit_UTM_Source end CJSummary_utm_source ,referrerdisplaytext Referrer_Name ,customerjourneysummary ,customerjourney ,Last_Moment_UTM_medium ShopifyQL_Unmapped_Last_medium ,coalesce(Last_Moment_UTM_Source,LastVisit_NON_UTM_Source) ShopifyQL_Unmapped_Last_Source ,rank() over (partition by name order by MOMENT_ID desc) rw from (select id, name, createdat, customerjourney, customerjourneysummary, referrerurl, referralcode, referrerdisplaytext, landingpageurl, LANDINGPAGEDISPLAYTEXT from Snitch_db.MAPLEMONK.shopifyindia_utm_parameters ) A, lateral flatten (INPUT => customerjourney:\"moments\") B ) where rw=1 ; create or replace table snitch_db.maplemonk.GOKWIK_SOURCE as With GO_KWIK as ( WITH utm_source_cte AS ( SELECT S.id, UPPER(A.value:\"value\") AS gokwik_utm_source FROM Snitch_db.MAPLEMONK.SHOPIFYINDIA_ORDERS S, LATERAL FLATTEN(INPUT => note_attributes) A WHERE LOWER(S.note_attributes) LIKE \'%gokwik%\' AND LOWER(A.value:\"name\") = \'utm_source\' ), utm_medium_cte AS ( SELECT S.id, UPPER(A.value:\"value\") AS gokwik_utm_medium FROM Snitch_db.MAPLEMONK.SHOPIFYINDIA_ORDERS S, LATERAL FLATTEN(INPUT => note_attributes) A WHERE LOWER(S.note_attributes) LIKE \'%gokwik%\' AND LOWER(A.value:\"name\") = \'utm_medium\' ) SELECT S.id, S.note_attributes, source_cte.gokwik_utm_source AS GOKWIK_UTM_SOURCE, medium_cte.gokwik_utm_medium AS GOKWIK_UTM_medium FROM Snitch_db.MAPLEMONK.SHOPIFYINDIA_ORDERS S LEFT JOIN utm_source_cte source_cte ON S.id = source_cte.id LEFT JOIN utm_medium_cte medium_cte ON S.id = medium_cte.id WHERE LOWER(S.note_attributes) LIKE \'%gokwik%\' AND source_cte.gokwik_utm_source IS NOT NULL ) Select * from GO_KWIK ; create or replace table snitch_db.maplemonk.simpl_SOURCE as With simpl as ( WITH utm_source_cte AS ( SELECT S.id, UPPER(A.value:\"value\") AS simpl_utm_source FROM Snitch_db.MAPLEMONK.SHOPIFYINDIA_ORDERS S, LATERAL FLATTEN(INPUT => note_attributes) A WHERE LOWER(S.PAYMENT_GATEWAY_NAMES) LIKE \'%simpl%\' AND LOWER(A.value:\"name\") = \'utm_source\' ), utm_medium_cte AS ( SELECT S.id, UPPER(A.value:\"value\") AS simpl_utm_medium FROM Snitch_db.MAPLEMONK.SHOPIFYINDIA_ORDERS S, LATERAL FLATTEN(INPUT => note_attributes) A WHERE LOWER(S.PAYMENT_GATEWAY_NAMES) LIKE \'%simpl%\' AND LOWER(A.value:\"name\") = \'utm_medium\' ) SELECT S.id, S.note_attributes, s.payment_gateway_names, source_cte.simpl_utm_source AS simpl_UTM_SOURCE, medium_cte.simpl_utm_medium AS simpl_UTM_medium FROM Snitch_db.MAPLEMONK.SHOPIFYINDIA_ORDERS S LEFT JOIN utm_source_cte source_cte ON S.id = source_cte.id LEFT JOIN utm_medium_cte medium_cte ON S.id = medium_cte.id WHERE LOWER(S.PAYMENT_GATEWAY_NAMES) LIKE \'%simpl%\' AND source_cte.simpl_utm_source IS NOT NULL ) Select * from simpl ; CREATE OR REPLACE TABLE Snitch_db.maplemonk.Shopify_All_customers AS select *,\'Shopify_India\' AS Shop_Name from Snitch_db.maplemonk.shopifyindia_customers; CREATE OR REPLACE TABLE Snitch_db.MAPLEMONK.Shopify_All_orders AS select o.* ,UPPER(ShopifyQL.shopifyql_mapped_source) shopifyql_mapped_source ,UPPER(ShopifyQL.FIRSTVISIT_UTM_SOURCE) Shopifyql_FIRSTVISIT_UTM_SOURCE ,UPPER(ShopifyQL.LAST_MOMENT_UTM_SOURCE) Shopifyql_LAST_MOMENT_UTM_SOURCE ,UPPER(ShopifyQL.LastVisit_NON_UTM_Source) Shopifyql_LAST_VISIT_NON_UTM_SOURCE ,upper(ShopifyQL.LastVisit_UTM_Campaign) Shopifyql_LAST_VISIT_UTM_CAMPAIGN ,upper(ShopifyQL.LastVisit_UTM_Medium) Shopifyql_LAST_VISIT_UTM_Medium ,UPPER(ShopifyQL.LAST_MOMENT_UTM_MEDIUM) Shopifyql_LAST_MOMENT_UTM_MEDIUM ,UPPER(ShopifyQL.FIRSTVISIT_UTM_MEDIUM) Shopifyql_FIRSTVISIT_UTM_MEDIUM ,UPPER(ShopifyQL.FirstVisit_UTM_Campaign) Shopifyql_FIRSTVISIT_UTM_CAMPAIGN ,UPPER(ShopifyQL.ShopifyQL_MAPPED_FIRSTCLICK_SOURCE) ShopifyQL_MAPPED_FIRSTCLICK_SOURCE ,div0(ShopifyQL.MOMENTS_COUNT,count(1) over (partition by O.name order by 1)) MOMENTS_COUNT ,div0(ShopifyQL.DAYSTOCONVERT,count(1) over (partition by O.name order by 1)) DAYSTOCONVERT ,UPPER(GOKWIK.GOKWIK_UTM_SOURCE) GOKWIK_MAPPED_SOURCE ,UPPER(simpl.SIMPL_UTM_SOURCE) SIMPL_MAPPED_SOURCE ,UPPER(GOKWIK.GOKWIK_UTM_MEDIUM) GOKWIK_MAPPED_medium ,UPPER(simpl.SIMPL_UTM_MEDIUM) SIMPL_MAPPED_medium ,UPPER(Referrer_Name) Referrer_Name ,Upper(coalesce(shopifyql_mapped_source,GOKWIK_UTM_SOURCE,simpl_UTM_SOURCE,\'Direct\')) FINAL_UTM_SOURCE ,upper(coalesce(Shopifyql_LAST_MOMENT_UTM_MEDIUM, Shopifyql_LAST_VISIT_UTM_Medium, Shopifyql_FIRSTVISIT_UTM_MEDIUM, GOKWIK_MAPPED_medium, SIMPL_MAPPED_medium)) FINAL_UTM_MEDIUM ,\'Shopify_India\' AS Shop_Name, CASE when o.created_at::date < \'2022-06-16\' and s.amount = 0 then \'Prepaid\' when o.created_at::date < \'2022-06-16\' and s.amount <> 0 then \'COD\' end as payment_mode from Snitch_db.MAPLEMONK.SHOPIFYINDIA_ORDERS o LEFT JOIN (select distinct _AIRBYTE_SHOPIFYINDIA_ORDERS_HASHID,presentment_money:amount amount from Snitch_db.maplemonk.SHOPIFYINDIA_ORDERS_TOTAL_SHIPPING_PRICE_SET) s on s._AIRBYTE_SHOPIFYINDIA_ORDERS_HASHID = o._AIRBYTE_shopifyindia_ORDERS_HASHID left join snitch_db.maplemonk.Shopify_UTM_Parameters ShopifyQL on O.name = ShopifyQL.name left join snitch_db.maplemonk.GOKWIK_SOURCE GOKWIK on O.ID = GOKWIK.ID left join snitch_db.maplemonk.simpl_SOURCE simpl on O.ID = simpl.ID ; ALTER TABLE Snitch_db.maplemonk.Shopify_All_orders RENAME COLUMN _AIRBYTE_SHOPIFYINDIA_ORDERS_HASHID to _AIRBYTE_ORDERS_HASHID; CREATE OR REPLACE TABLE Snitch_db.maplemonk.Shopify_All_products AS select *,\'Shopify_India\' AS Shop_Name from Snitch_db.maplemonk.shopifyindia_products ; ALTER TABLE Snitch_db.maplemonk.Shopify_All_products RENAME COLUMN _AIRBYTE_SHOPIFYINDIA_PRODUCTS_HASHID to _AIRBYTE_PRODUCTS_HASHID; CREATE OR REPLACE TABLE Snitch_db.maplemonk.Shopify_All_products_variants AS select *,\'Shopify_India\' AS Shop_Name from Snitch_db.maplemonk.shopifyindia_products_variants ; ALTER TABLE Snitch_db.maplemonk.Shopify_All_products_variants RENAME COLUMN _AIRBYTE_SHOPIFYINDIA_PRODUCTS_HASHID to _AIRBYTE_PRODUCTS_HASHID; CREATE OR REPLACE TABLE Snitch_db.maplemonk.Shopify_All_customers_addresses AS select *,\'Shopify_India\' AS Shop_Name from Snitch_db.maplemonk.shopifyindia_customers_addresses ; CREATE OR REPLACE TABLE Snitch_db.maplemonk.Shopify_All_orders_items_discount AS SELECT order_id, LINE_ITEM_ID, SUM(B.VALUE:amount::FLOAT) AS DISCOUNT FROM( SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:discount_allocations AS discount_allocations FROM Snitch_db.maplemonk.Shopify_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A)x,LATERAL FLATTEN (INPUT => x.discount_allocations)B GROUP BY ORDER_ID, LINE_ITEM_ID; CREATE OR REPLACE TABLE snitch_db.maplemonk.Shopify_All_orders_items_tax AS SELECT order_id, LINE_ITEM_ID, SUM(B.VALUE:price::FLOAT) AS TAX, sum(B.VALUE:rate::float) as Tax_Rate FROM( SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:tax_lines AS tax_lines FROM snitch_db.maplemonk.Shopify_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A)x,LATERAL FLATTEN (INPUT => x.tax_lines)B GROUP BY order_id, LINE_ITEM_ID; CREATE OR REPLACE TABLE Snitch_db.maplemonk.Shopify_All_Refunds AS SELECT Name, ID AS Order_ID, Shop_name, C.value:line_item_id AS line_item_id, SUM(C.VALUE:quantity) AS quantity, SUM(C.VALUE:subtotal) AS subtotal FROM Snitch_db.maplemonk.Shopify_All_orders, LATERAL FLATTEN(INPUT => refunds)A, LATERAL FLATTEN(INPUT=>A.value)B, LATERAL FLATTEN(INPUT => B.value) C WHERE C.value:line_item_id IS NOT null GROUP BY Name, ID, Shop_name, C.value:line_item_id; CREATE OR REPLACE TABLE snitch_db.maplemonk.Shopify_All_discount_codes AS select ID, CODE from ( SELECT id, replace(A.value:code , \'\"\',\'\') code, row_number() over (partition by id order by code) rw FROM snitch_db.maplemonk.Shopify_All_orders, LATERAL FLATTEN (INPUT => discount_codes) A ) where rw=1; CREATE OR REPLACE TABLE Snitch_db.maplemonk.Shopify_All_orders_items AS WITH CTE AS (SELECT SHOP_NAME, ID::VARCHAR(16777216) AS ORDER_ID, NAME AS ORDER_NAME, CUSTOMER, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:sku::STRING AS SKU, A.VALUE:product_id::STRING AS PRODUCT_ID, A.VALUE:title::STRING AS PRODUCT_NAME, app_id, coalesce(phone, replace(customer:default_address:phone,\'\"\',\'\')) phone, coalesce(email, replace(customer:email,\'\"\',\'\')) email, replace(customer:default_address:zip,\'\"\',\'\') pincode, CURRENCY, tags, gateway, CASE WHEN cancelled_at IS NOT NULL THEN \'CANCELLED\' ELSE \'Shopify_Processed\' END AS order_status, CREATED_AT::DATETIME AS order_timestamp, A.VALUE:price::FLOAT * A.VALUE:quantity::FLOAT AS LINE_ITEM_SALES, (TOTAL_SHIPPING_PRICE_SET:presentment_money:amount::FLOAT/ COUNT(ORDER_ID) OVER(PARTITION BY ORDER_ID ORDER BY ORDER_ID)) AS SHIPPING_PRICE, A.VALUE:quantity::FLOAT as QUANTITY, \'Shopify\' AS Source, LANDING_SITE, REFERRING_SITE, payment_mode, MOMENTS_COUNT, DAYSTOCONVERT, SHOPIFYQL_FIRSTVISIT_UTM_SOURCE, SHOPIFYQL_MAPPED_SOURCE, SHOPIFYQL_LAST_MOMENT_UTM_SOURCE, Shopifyql_LAST_VISIT_NON_UTM_SOURCE, Shopifyql_LAST_MOMENT_UTM_MEDIUM, Shopifyql_FIRSTVISIT_UTM_MEDIUM, ShopifyQL_MAPPED_FIRSTCLICK_SOURCE, Shopifyql_FIRSTVISIT_UTM_CAMPAIGN, Shopifyql_LAST_VISIT_UTM_CAMPAIGN, FINAL_UTM_SOURCE, FINAL_UTM_MEDIUM, Referrer_Name, GOKWIK_MAPPED_SOURCE FROM Snitch_db.maplemonk.Shopify_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A) SELECT CTE.*, DC.code discount_code, IFNULL(T.TAX_RATE,0) AS TAX_RATE, IFNULL(T.TAX,0) AS TAX, IFNULL(D.DISCOUNT,0) AS DISCOUNT, CASE when T.TAX=0 then IFNULL(D.DISCOUNT,0) else IFNULL(D.DISCOUNT,0)/(1+IFNULL(T.TAX_RATE,0)) end AS DISCOUNT_BEFORE_TAX, CTE.LINE_ITEM_SALES - IFNULL(D.DISCOUNT,0) + IFNULL(CTE.SHIPPING_PRICE,0) AS NET_SALES, CTE.LINE_ITEM_SALES - IFNULL(D.DISCOUNT,0) as Gross_Sales, case when T.TAX=0 then (CTE.LINE_ITEM_SALES) - IFNULL(D.DISCOUNT,0) + IFNULL(T.TAX,0) + CTE.SHIPPING_PRICE else (CTE.LINE_ITEM_SALES/(1+IFNULL(T.TAX_RATE,0))) - (IFNULL(D.DISCOUNT,0)/(1+IFNULL(T.TAX_RATE,0))) + IFNULL(T.TAX,0) + CTE.SHIPPING_PRICE end AS TOTAL_SALES, CTE.LINE_ITEM_SALES/(1+IFNULL(T.TAX_RATE,0)) AS GROSS_SALES_BEFORE_TAX, CASE WHEN R.QUANTITY IS NOT NULL THEN 1 ELSE 0 END AS IS_REFUND FROM CTE LEFT JOIN Snitch_db.maplemonk.Shopify_All_orders_items_tax T ON CTE.ORDER_ID = T.ORDER_ID AND CTE.LINE_ITEM_ID = T.LINE_ITEM_ID LEFT JOIN Snitch_db.maplemonk.Shopify_All_orders_items_discount D ON CTE.ORDER_ID = D.ORDER_ID AND CTE.LINE_ITEM_ID = D.LINE_ITEM_ID LEFT JOIN Snitch_db.maplemonk.Shopify_All_Refunds R ON CTE.ORDER_ID = R.ORDER_ID AND CTE.LINE_ITEM_ID = R.LINE_ITEM_ID left join snitch_db.maplemonk.Shopify_All_discount_codes DC on CTE.order_id = DC.id ; CREATE OR REPLACE TABLE Snitch_db.maplemonk.FACT_ITEMS_SNITCH AS select a.*, case when cm.mapped_cuty is not null then upper(cm.mapped_cuty) else upper(a.city) end as city_mapped, case when lower(city_mapped) in (\'bangalore\',\'bengaluru\',\'hyderabad\',\'delhi\',\'new delhi\',\'mumbai\',\'bombay\',\'kolkata\',\'calcutta\',\'chennai\') then \'Metro\' else \'Non-Metro\' end as Metro from ( SELECT O.SHOP_NAME, O.ORDER_ID, O.ORDER_NAME, O.CUSTOMER:id::int AS customer_id, replace(O.customer:default_address:name,\'\"\',\'\') as customer_name, O.LINE_ITEM_ID, o.app_id, coalesce(o.phone,c.phone) phone, coalesce(o.email,c.email) email, pincode, gateway, o.tags, case when o.order_timestamp::date >= \'2022-06-16\' and lower(o.gateway) in (\'cashfree payments\',\'cashfree\') then \'Prepaid\' when o.order_timestamp::date >= \'2022-06-16\' and lower(o.gateway) = \'\' and lower(o.tags) like \'%breeze%\' then \'Prepaid\' when o.order_timestamp::date >= \'2022-06-16\' and lower(o.gateway) like \'%cash%\' and lower(o.gateway) <> \'cashfree payments\' then \'COD\' when o.order_timestamp::date >= \'2022-06-16\' and lower(o.gateway) not like \'%cash%\' and lower(o.gateway) <> \'\' then \'Prepaid\' when o.order_timestamp::date >= \'2022-06-16\' and lower(o.gateway) not like \'%cash%\' and lower(o.gateway) = \'\' then \'Exchange\' when line_item_sales = discount then \'Exchange\' else o.payment_mode end as payment_method, case when lower(o.gateway) like \'%gokwik%\' then \'GoKwik\' when lower(o.gateway) like \'%payu%\' then \'GoKwik\' when lower(o.gateway) like \'%easebuzz%\' then \'Easebuzz\' when lower(o.gateway) like \'%cred pay%\' then \'CRED Pay\' when lower(o.gateway) like \'%snapmint%\' then \'Snapmint\' when lower(o.gateway) like \'%bharatx%\' then \'BharatPe\' when lower(o.gateway) like \'%simpl%\' then \'Simpl\' when lower(o.gateway) like \'%cashfree%\' then \'CashFree\' when lower(o.gateway) like \'%razor%\' then \'RazorPay\' when lower(o.gateway) like \'%cash%\' then \'COD\' when lower(o.gateway) like \'%axio%\' then \'Axio\' when lower(o.gateway) like \'\' then \'Exchange\' else o.gateway end as payment_gateway, case when lower(o.gateway) like \'%gokwik%\' then \'GoKwik\' when lower(o.tags) like \'%breeze%\' then \'Breeze\' when lower(o.tags) like \'%simpl%\' then \'Simpl\' else \'Shopify\' end as Checkout, case when lower(o.gateway) like \'%simple%\' then \'BNPL\' when lower(o.gateway) like \'%axio%\' then \'BNPL\' when lower(o.gateway) like \'%snapmint%\' then \'BNPL\' when lower(o.gateway) like \'%bharatx%\' then \'BNPL\' else \'Non-BNPL\' end as BNPL_flag, O.SKU, case when right(o.sku,2) = \'-S\' then left(o.sku,len(o.sku)-2) else replace(o.sku,concat(\'-\',split_part(o.sku,\'-\',-1)),\'\') end sku_group, pv.size, pv.colour, O.PRODUCT_ID, case when lower(p.tags) like \'%mad sale%\' then \'MAD Sale Collection\' when lower(p.tags) like \'%warmest winter sale%\' then \'Warmest Winter Sale Collection\' when lower(p.tags) like \'%cold%\' then \'COLD\' else \'Others\' end as Collection, CASE WHEN O.PRODUCT_NAME IS NULL THEN \'NA\' ELSE O.PRODUCT_NAME END AS PRODUCT_NAME, p.tags as product_tags, replace(p.image:src,\'\"\"\',\'\') IMAGE_lINK, O.CURRENCY, O.IS_REFUND, CASE WHEN CD.city IS NULL OR CD.city = \'\' THEN \'NA\' ELSE Cd.city END AS city, CASE WHEN CD.province IS NULL OR CD.province = \'\' THEN \'NA\' ELSE CD.province END AS state, CASE WHEN P.product_type = \'\' THEN \'NA\' WHEN P.product_type = \'Jeans\' THEN \'Denim\' WHEN P.product_type = \'Pant\' then \'Pants\' ELSE P.product_type END AS category, case when p.product_type in (\'Denim\',\'Jeans\') and lower(o.product_name) like \'%bootcut%\' then \'Bootcut Denim\' when p.product_type in (\'Denim\',\'Jeans\') and lower(o.product_name) like \'%straight fit%\' then \'Straight Fit\' when p.product_type in (\'Denim\',\'Jeans\') and (lower(product_name) not like \'%bootcut%\' and lower(product_name) not like \'%straight fit%\') then \'Other Denim\' end as Denim_type, CASE WHEN O.SKU LIKE \'%4MSQ0011%\' THEN \'LUXE\' WHEN O.SKU NOT LIKE \'%4MSQ0011%\' AND M.SKUS is NOT NULL THEN \'PLUS\' WHEN O.SKU NOT LIKE \'%4MSQ0011%\' AND N.SKUS is NOT NULL THEN \'INNERWEAR\' when P.product_type = \'Perfume & Cologne\' then \'PERFUMES\' ELSE \'OTHER\' END AS Segment, CASE WHEN O.APP_ID = \'2653365\' THEN \'Shopney\' when o.app_id = \'944701441\' then \'Appbrew\' when o.app_id = \'23414800385\' then \'Appbrew\' ELSE \'Web\' end as WebShopney, case when p.body_html like \'%Tencil%\' then \'Tencil\' when p.body_html like \'%Rayon%\' then \'Rayon\' when p.body_html like \'%Cotton%\' then \'Cotton\' when p.body_html like \'%Polyester%\' then \'Polyester\' when p.body_html like \'%Viscose%\' then \'Viscose\' end as Material, O.order_status, O.order_timestamp, O.LINE_ITEM_SALES, O.SHIPPING_PRICE, O.QUANTITY, O.TAX, O.DISCOUNT, o.discount_code, o.discount_before_tax, O.NET_SALES, o.total_sales as gross_sales, o.gross_sales_before_tax, O.Source, O.landing_site, o.referring_site, o.payment_mode, c.return_flag, c.return_quantity, MOMENTS_COUNT, DAYSTOCONVERT, SHOPIFYQL_FIRSTVISIT_UTM_SOURCE, SHOPIFYQL_MAPPED_SOURCE, SHOPIFYQL_LAST_MOMENT_UTM_SOURCE, Shopifyql_LAST_VISIT_NON_UTM_SOURCE, Shopifyql_LAST_MOMENT_UTM_MEDIUM, Shopifyql_FIRSTVISIT_UTM_MEDIUM, ShopifyQL_MAPPED_FIRSTCLICK_SOURCE, Shopifyql_FIRSTVISIT_UTM_CAMPAIGN, Shopifyql_LAST_VISIT_UTM_CAMPAIGN, FINAL_UTM_SOURCE, FINAL_UTM_MEDIUM, Referrer_Name, GOKWIK_MAPPED_SOURCE FROM Snitch_db.maplemonk.Shopify_All_orders_items O left join (select distinct * from (select order_id ,saleorderitemcode ,phone ,email ,return_flag ,return_quantity ,row_number() over (partition by order_id, split_part(saleorderitemcode,\'-\',0) order by shipping_last_update_date desc) rw from snitch_DB.maplemonk.UNICOMMERCE_FACT_ITEMS_snitch where lower(marketplace) like any (\'%shopify%\') ) where rw=1 )c on o.order_id=c.order_id and o.line_item_id=split_part(c.saleorderitemcode,\'-\',0) LEFT JOIN (select * from Snitch_db.maplemonk.Shopify_All_products) P ON O.PRODUCT_ID = P.id LEFT JOIN ( select distinct sku, size, colour from ( select distinct sku, row_number() over (partition by sku order by title) row_num, case when option2 in (\'XL\',\'S\',\'3XL\',\'34\',\'44\',\'2XL\',\'5XL\',\'36\',\'L\',\'4XL\',\'46\',\'M\',\'XXL\',\'32\',\'30\',\'40\',\'28\',\'38\',\'42\') then option2 when option2 is null then option1 else option1 end as size, case when option2 in (\'XL\',\'S\',\'3XL\',\'34\',\'44\',\'2XL\',\'5XL\',\'36\',\'L\',\'4XL\',\'46\',\'M\',\'XXL\',\'32\',\'30\',\'40\',\'28\',\'38\',\'42\') then option1 when option2 is null then \'None\' else option2 end as colour from Snitch_db.maplemonk.SHOPIFY_ALL_PRODUCTS_VARIANTs ) where row_num=1) pv on pv.sku = o.sku left join snitch_db.maplemonk.snitch_plus_sku_mapping m on o.sku = m.skus left join snitch_db.maplemonk.snitch_innerwear_sku_mapping n on o.sku = n.skus LEFT JOIN(SELECT distinct customer_id, city, province, row_number() OVER ( partition BY customer_id ORDER BY id DESC) rowid FROM Snitch_db.maplemonk.Shopify_All_customers_addresses) AS CD ON O.CUSTOMER:id::int = CD.customer_id AND CD.rowid = 1 ) a left join (select distinct city, mapped_cuty from snitch_db.maplemonk.snitch_city_mapping where mapped_cuty is not null) cm on a.city = cm.city ; ALTER TABLE Snitch_db.maplemonk.FACT_ITEMS_SNITCH ADD COLUMN customer_flag varchar(50); ALTER TABLE Snitch_db.maplemonk.FACT_ITEMS_SNITCH ADD COLUMN new_customer_flag varchar(50); ALTER TABLE Snitch_db.maplemonk.FACT_ITEMS_SNITCH ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE Snitch_db.maplemonk.FACT_ITEMS_SNITCH ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE Snitch_db.maplemonk.FACT_ITEMS_SNITCH ADD COLUMN acquisition_material_colour varchar(16777216); ALTER TABLE Snitch_db.maplemonk.FACT_ITEMS_SNITCH ADD COLUMN ACQUISITION_date timestamp; UPDATE Snitch_db.maplemonk.FACT_ITEMS_SNITCH AS A SET A.ACQUISITION_DATE = B.ACQUISITION_DATE FROM ( select distinct customer_id , min(order_timestamp) OVER ( partition BY customer_id) ACQUISITION_DATE from Snitch_db.maplemonk.FACT_ITEMS_SNITCH B where lower(order_status) not in (\'cancelled\',\'returned\') ) AS B where A.customer_id = B.customer_id; UPDATE Snitch_db.maplemonk.FACT_ITEMS_SNITCH AS A SET A.customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id, order_timestamp, CASE WHEN order_timestamp = acquisition_Date and lower(order_status) not in (\'cancelled\', \'returned\') THEN \'New\' when ORDER_TIMESTAMP < acquisition_Date then \'Yet to make completed order\' else \'Repeated\' END AS Flag FROM Snitch_db.maplemonk.FACT_ITEMS_SNITCH )AS B WHERE A.order_id = B.order_id AND A.customer_id = B.customer_id and A.order_timestamp::date = b.order_timestamp::date ; UPDATE Snitch_db.maplemonk.FACT_ITEMS_SNITCH SET customer_flag = CASE WHEN customer_flag IS NULL and lower(order_status) not in (\'cancelled\', \'returned\') THEN \'New\' when customer_flag IS NULL and lower(order_status) in (\'cancelled\', \'returned\') THEN \'Yet to make completed order\' ELSE customer_flag END; UPDATE Snitch_db.maplemonk.FACT_ITEMS_SNITCH AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id, order_timestamp, CASE WHEN Last_day(order_timestamp, \'month\') = last_day(acquisition_Date, \'month\') then \'New\' when last_Day(order_timestamp, \'month\') < last_Day(acquisition_date, \'month\') then \'Yet to make completed order\' ELSE \'Repeated\' END AS Flag FROM Snitch_db.maplemonk.FACT_ITEMS_SNITCH)AS B WHERE A.order_id = B.order_id AND A.customer_id = B.customer_id; UPDATE Snitch_db.maplemonk.FACT_ITEMS_SNITCH SET new_customer_flag = CASE WHEN new_customer_flag IS NULL and lower(order_Status) not in (\'cancelled\',\'returned\') THEN \'New\' ELSE new_customer_flag END; CREATE OR replace temporary TABLE Snitch_db.maplemonk.temp_source AS SELECT DISTINCT customer_id, source FROM ( SELECT DISTINCT customer_id, order_timestamp, source, Min(order_timestamp) OVER ( partition BY customer_id) firstOrderdate FROM Snitch_db.maplemonk.FACT_ITEMS_SNITCH where lower(order_Status) not in (\'cancelled\',\'returned\'))res WHERE order_timestamp=firstorderdate; UPDATE Snitch_db.maplemonk.FACT_ITEMS_SNITCH AS a SET a.acquisition_channel=b.source FROM Snitch_db.maplemonk.temp_source b WHERE a.customer_id = b.customer_id; CREATE OR replace temporary TABLE Snitch_db.maplemonk.temp_product AS SELECT DISTINCT customer_id, product_name, Row_number() OVER (partition BY customer_id ORDER BY LINE_ITEM_SALES DESC) rowid FROM ( SELECT DISTINCT customer_id, order_timestamp, product_name, LINE_ITEM_SALES , Min(order_timestamp) OVER (partition BY customer_id) firstOrderdate FROM Snitch_db.maplemonk.FACT_ITEMS_SNITCH where lower(order_Status) not in (\'cancelled\',\'returned\'))res WHERE order_timestamp=firstorderdate; UPDATE Snitch_db.maplemonk.FACT_ITEMS_SNITCH AS A SET A.acquisition_product=B.product_name FROM ( SELECT * FROM Snitch_db.maplemonk.temp_product WHERE rowid=1)B WHERE A.customer_id = B.customer_id; CREATE OR replace temporary TABLE Snitch_db.maplemonk.temp_material_colour AS SELECT DISTINCT customer_id, material_colour, Row_number() OVER (partition BY customer_id ORDER BY LINE_ITEM_SALES DESC) rowid FROM ( SELECT DISTINCT customer_id, order_timestamp, concat(material,\' \',colour) material_colour, LINE_ITEM_SALES , Min(order_timestamp) OVER (partition BY customer_id) firstOrderdate FROM Snitch_db.maplemonk.FACT_ITEMS_SNITCH where lower(order_Status) not in (\'cancelled\',\'returned\'))res WHERE order_timestamp=firstorderdate; UPDATE Snitch_db.maplemonk.FACT_ITEMS_SNITCH AS A SET A.acquisition_material_colour=B.material_colour FROM ( SELECT * FROM Snitch_db.maplemonk.temp_material_colour WHERE rowid=1)B WHERE A.customer_id = B.customer_id;",
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
                        