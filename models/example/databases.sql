{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE pomme_db.MAPLEMONK.Woocommerce_Pomme_All_products AS select * ,\'Woocommerce_EU\' AS Shop_Name from pomme_db.maplemonk.woocommerce_eu_products union all select * ,\'Woocommerce_SE\' AS Shop_Name from pomme_db.maplemonk.woocommerce_se_products union all select * ,\'Woocommerce_FR\' AS Shop_Name from pomme_db.maplemonk.woocommerce_FR_products union all select * ,\'Woocommerce_DK\' AS Shop_Name from pomme_db.maplemonk.woocommerce_dk_products union all select * ,\'Woocommerce_US\' AS Shop_Name from pomme_db.maplemonk.woocommerce_us_products ; CREATE OR REPLACE TABLE pomme_db.MAPLEMONK.Woocommerce_Pomme_All_customers AS select _AIRBYTE_UNIQUE_KEY, ID, ROLE, EMAIL, _LINKS, BILLING, SHIPPING, \'www.wearepomme.com\' as shop_url, USERNAME, LAST_NAME, META_DATA, AVATAR_URL, FIRST_NAME, DATE_CREATED, DATE_MODIFIED, DATE_CREATED_GMT, DATE_MODIFIED_GMT, IS_PAYING_CUSTOMER, _AIRBYTE_AB_ID, _AIRBYTE_EMITTED_AT, _AIRBYTE_NORMALIZED_AT, _AIRBYTE_WOOCOMMERCE_EU_CUSTOMERS_HASHID, \'Woocommerce_EU\' AS Shop_Name from pomme_db.MAPLEMONK.WOOCOMMERCE_EU_CUSTOMERS Union all select *,\'Woocommerce_SE\' AS Shop_Name from pomme_db.MAPLEMONK.WOOCOMMERCE_SE_CUSTOMERS Union all select *,\'Woocommerce_FR\' AS Shop_Name from pomme_db.MAPLEMONK.WOOCOMMERCE_FR_CUSTOMERS Union all select *,\'Woocommerce_DK\' AS Shop_Name from pomme_db.MAPLEMONK.WOOCOMMERCE_DK_CUSTOMERS Union all select *,\'Woocommerce_US\' AS Shop_Name from pomme_db.MAPLEMONK.WOOCOMMERCE_US_CUSTOMERS ; CREATE OR REPLACE TABLE pomme_db.MAPLEMONK.Woocommerce_pomme_All_orders AS select _AIRBYTE_UNIQUE_KEY, ID, TOTAL, _LINKS, NUMBER, STATUS, BILLING, REFUNDS, VERSION, CART_TAX, CURRENCY, SET_PAID, SHIPPING, \'www.wearepomme.com\' as shop_url, CART_HASH, DATE_PAID, FEE_LINES, META_DATA, ORDER_KEY, PARENT_ID, TAX_LINES, TOTAL_TAX, LINE_ITEMS, CREATED_VIA, CUSTOMER_ID, COUPON_LINES, DATE_CREATED, DISCOUNT_TAX, SHIPPING_TAX, CUSTOMER_NOTE, DATE_MODIFIED, DATE_PAID_GMT, DATE_COMPLETED, DISCOUNT_TOTAL, PAYMENT_METHOD, SHIPPING_LINES, SHIPPING_TOTAL, TRANSACTION_ID, DATE_CREATED_GMT, DATE_MODIFIED_GMT, DATE_COMPLETED_GMT, PRICES_INCLUDE_TAX, CUSTOMER_IP_ADDRESS, CUSTOMER_USER_AGENT, PAYMENT_METHOD_TITLE, _AIRBYTE_AB_ID, _AIRBYTE_EMITTED_AT, _AIRBYTE_NORMALIZED_AT, _AIRBYTE_WOOCOMMERCE_EU_ORDERS_HASHID, \'Woocommerce_EU\' AS Shop_Name from pomme_db.maplemonk.woocommerce_eu_orders Union all select *, \'Woocommerce_SE\' AS Shop_Name from pomme_db.maplemonk.woocommerce_se_orders Union all select *, \'Woocommerce_FR\' AS Shop_Name from pomme_db.maplemonk.woocommerce_FR_orders Union all select *, \'Woocommerce_DK\' AS Shop_Name from pomme_db.maplemonk.woocommerce_DK_orders Union all select *, \'Woocommerce_US\' AS Shop_Name from pomme_db.maplemonk.woocommerce_US_orders ; create or replace table pomme_db.maplemonk.woocommerce_all_orders_products_attributes as select a.id order_id, a.LINE_ITEM_ID, a.product_id, a.sku, a.grip, b.size, ifnull(p.color,\'NA\') color, ifnull(c.product_short_name,\'NA\') product_short_name1, ifnull(d.product_short_name,\'NA\') product_short_name2, ifnull(e.category,\'NA\') category1, ifnull(f.category,\'NA\') category2, a.shop_name from (select distinct id, A.VALUE:product_id::STRING product_id, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:sku::STRING AS SKU, case when lower(B.value:display_key) = \'grip\' then replace(B.value:display_value,\'\"\',\'\') end as Grip, shop_name from pomme_db.MAPLEMONK.Woocommerce_pomme_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A , lateral flatten (input => A.value:meta_data) B where Grip is not null) a left join (select distinct id, A.VALUE:product_id::STRING product_id, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:sku::STRING AS SKU, case when lower(B.value:display_key) = \'size\' then replace(B.value:display_value,\'\"\',\'\') end as Size, shop_name from pomme_db.MAPLEMONK.Woocommerce_pomme_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A , lateral flatten (input => A.value:meta_data) B where Size is not null) b on a.id=b.id and a.product_id = b.product_id and a.sku = b.sku and a.shop_name = b.shop_name and a.line_item_id = b.line_item_id left join (select * from (select id, sku, name, rtrim(ltrim(replace(A.value:options,\'\"\',\'\'),\'[\'),\']\') Color, shop_name, row_number() over(partition by id, sku, name, shop_name order by rtrim(ltrim(replace(A.value:options,\'\"\',\'\'),\'[\'),\']\')) rw from pomme_db.MAPLEMONK.Woocommerce_Pomme_All_products, lateral flatten (input => attributes) A where lower(A.value:name) = \'color\') where rw = 1 )p on a.shop_name = p.shop_name and a.product_id = p.id left join (select id, sku, name, replace(A.value:name,\'\"\',\'\') product_short_name , shop_name from pomme_db.MAPLEMONK.Woocommerce_Pomme_All_products, lateral flatten (input => tags) A where index = 0) c on a.shop_name = c.shop_name and a.product_id = c.id left join (select id, sku, name, replace(A.value:name,\'\"\',\'\') product_short_name , shop_name from pomme_db.MAPLEMONK.Woocommerce_Pomme_All_products, lateral flatten (input => tags) A where index = 1) d on a.shop_name = d.shop_name and a.product_id = d.id left join (select id, sku, name, replace(A.value:name,\'\"\',\'\') Category , shop_name from pomme_db.MAPLEMONK.Woocommerce_Pomme_All_products, lateral flatten (input => categories) A where index = 0) e on a.shop_name = e.shop_name and a.product_id = e.id left join (select id, sku, name, replace(A.value:name,\'\"\',\'\') Category , shop_name from pomme_db.MAPLEMONK.Woocommerce_Pomme_All_products, lateral flatten (input => categories) A where index = 1) f on a.shop_name = f.shop_name and a.product_id = f.id ; CREATE OR REPLACE TABLE pomme_DB.maplemonk.woocommerce_pomme_All_orders_items_discount AS SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, discount_total/count(distinct A.value:id) over (partition by id, shop_name) AS discount_allocations, shop_name FROM pomme_DB.maplemonk.WOOCOMMERCE_POMME_ALL_ORDERS, LATERAL FLATTEN (INPUT => LINE_ITEMS)A ; CREATE OR REPLACE TABLE pomme_DB.maplemonk.woocommerce_pomme_All_orders_items_tax AS SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, shop_name, replace(A.VALUE:total_tax,\'\"\',\'\') AS tax_lines FROM pomme_DB.maplemonk.WOOCOMMERCE_POMME_ALL_ORDERS, LATERAL FLATTEN (INPUT => LINE_ITEMS)A ; CREATE OR REPLACE TABLE pomme_db.maplemonk.WOOCOMMERCE_POMME_All_Refunds AS select id, replace(A.value:sku,\'\"\',\'\') SKU, replace(A.value:product_id,\'\"\',\'\') product_id, replace(A.value:total,\'\"\',\'\') +replace(A.value:total_tax,\'\"\',\'\') refund_amount, replace(A.value:quantity,\'\"\',\'\') quantity, \'Woocommerce_EU\' AS Shop_Name from pomme_db.maplemonk.woocommerce_eu_refunds, lateral flatten (input =>line_items)A union all select id, replace(A.value:sku,\'\"\',\'\') SKU, replace(A.value:product_id,\'\"\',\'\') product_id, replace(A.value:total,\'\"\',\'\') +replace(A.value:total_tax,\'\"\',\'\') refund_amount, replace(A.value:quantity,\'\"\',\'\') quantity, \'Woocommerce_SE\' AS Shop_Name from pomme_db.maplemonk.woocommerce_se_refunds, lateral flatten (input =>line_items)A union all select id, replace(A.value:sku,\'\"\',\'\') SKU, replace(A.value:product_id,\'\"\',\'\') product_id, replace(A.value:total,\'\"\',\'\') +replace(A.value:total_tax,\'\"\',\'\') refund_amount, replace(A.value:quantity,\'\"\',\'\') quantity, \'Woocommerce_US\' AS Shop_Name from pomme_db.maplemonk.woocommerce_us_refunds, lateral flatten (input =>line_items)A union all select id, replace(A.value:sku,\'\"\',\'\') SKU, replace(A.value:product_id,\'\"\',\'\') product_id, replace(A.value:total,\'\"\',\'\') +replace(A.value:total_tax,\'\"\',\'\') refund_amount, replace(A.value:quantity,\'\"\',\'\') quantity, \'Woocommerce_DK\' AS Shop_Name from pomme_db.maplemonk.woocommerce_dk_refunds, lateral flatten (input =>line_items)A union all select id, replace(A.value:sku,\'\"\',\'\') SKU, replace(A.value:product_id,\'\"\',\'\') product_id, replace(A.value:total,\'\"\',\'\') +replace(A.value:total_tax,\'\"\',\'\') refund_amount, replace(A.value:quantity,\'\"\',\'\') quantity, \'Woocommerce_FR\' AS Shop_Name from pomme_db.maplemonk.woocommerce_fr_refunds, lateral flatten (input =>line_items)A ; CREATE OR REPLACE TABLE pomme_DB.maplemonk.Woocommerce_pomme_All_orders_items AS WITH CTE AS (SELECT SHOP_NAME, ID::VARCHAR(16777216) AS ORDER_ID, order_key AS ORDER_NAME, CUSTOMER_id, concat(replace(billing:first_name,\'\"\',\'\'),\' \',replace(billing:last_name,\'\"\',\'\')) NAME, replace(billing:phone,\'\"\',\'\') PHONE, replace(billing:email,\'\"\',\'\') EMAIL, replace(shipping:country,\'\"\',\'\') Country, replace(shipping:state,\'\"\',\'\') State, replace(shipping:city,\'\"\',\'\') City, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:sku::STRING AS SKU, A.VALUE:product_id::STRING AS PRODUCT_ID, A.VALUE:name::STRING AS PRODUCT_NAME, A.VALUE:parent_name::STRING AS PRODUCT_PARENT_NAME, CURRENCY, status order_status, replace(refunds:id,\'\"\',\'\') refunds_id, DATE_CREATED_GMT::DATETIME AS order_timestamp, replace(A.VALUE:total,\'\"\',\'\') + replace(A.VALUE:total_tax,\'\"\',\'\') AS LINE_ITEM_SALES, shipping_total/ COUNT(A.VALUE:id) OVER(PARTITION BY ORDER_ID ) AS SHIPPING_PRICE, shipping_tax/ COUNT(A.VALUE:id) OVER(PARTITION BY ORDER_ID ) as shipping_tax, replace(A.VALUE:quantity,\'\"\',\'\')::FLOAT as QUANTITY, \'WooCommerce\' AS Source FROM pomme_DB.maplemonk.woocommerce_pomme_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A) SELECT CTE.*, pa.grip grip, pa.size size, pa.color color, pa.product_short_name1, pa.product_short_name2, pa.category1, pa.category2, IFNULL(D.DISCOUNT_ALLOCATIONS,0) AS DISCOUNT, CTE.LINE_ITEM_SALES AS GROSS_SALES_AFTER_TAX, CTE.LINE_ITEM_SALES - ifnull(tax_lines,0) AS GROSS_SALES_BEFORE_TAX, CTE.LINE_ITEM_SALES - ifnull(tax_lines,0) - ifnull(discount_allocations,0) AS NET_SALES_BEFORE_TAX, IFNULL(T.TAX_lines,0) AS TAX, CTE.LINE_ITEM_SALES + ifnull(shipping_price,0) + ifnull(shipping_tax,0) + ifnull(refund_amount,0) AS TOTAL_SALES, refund_amount as refund_amount, R.quantity as refunded_quantity FROM CTE LEFT JOIN pomme_db.maplemonk.woocommerce_pomme_All_orders_items_tax T ON CTE.ORDER_ID = T.ORDER_ID AND CTE.LINE_ITEM_ID = T.LINE_ITEM_ID and CTE.shop_name = T.shop_name LEFT JOIN pomme_db.maplemonk.woocommerce_pomme_All_orders_items_discount D ON CTE.ORDER_ID = D.ORDER_ID AND CTE.LINE_ITEM_ID = D.LINE_ITEM_ID and CTE.shop_name = D.shop_name LEFT JOIN pomme_db.maplemonk.woocommerce_pomme_All_Refunds R ON CTE.refunds_ID = R.ID AND CTE.product_id = R.product_id and CTE.shop_name = R.shop_name left join pomme_db.maplemonk.woocommerce_all_orders_products_attributes pa on CTE.order_id = pa.order_id and CTE.product_id = pa.product_id and CTE.shop_name = pa.shop_name and CTE.sku = pa.sku and CTE.line_item_id = pa.line_item_id ; CREATE OR REPLACE TABLE pomme_DB.maplemonk.FACT_ITEMS_woocommerce_pomme AS SELECT O.SHOP_NAME, O.ORDER_ID, O.ORDER_NAME, O.CUSTOMER_id customer_id, O.Name, O.EMAIL, O.PHONE, O.LINE_ITEM_ID, O.SKU, O.PRODUCT_ID, o.product_name, O.CURRENCY, o.city, o.State, o.country, o.grip, o.size, o.color, o.product_short_name1, o.product_short_name2, o.category1, o.category2, O.order_status, O.order_timestamp, case when o.currency = \'DKK\' then O.LINE_ITEM_SALES*DKK_SEK when o.currency = \'EUR\' then O.LINE_ITEM_SALES*EUR_SEK when o.currency = \'USD\' then O.LINE_ITEM_SALES*USD_SEK else O.line_item_sales end as line_item_sales, case when o.currency = \'DKK\' then O.SHIPPING_PRICE*DKK_SEK when o.currency = \'EUR\' then O.SHIPPING_PRICE*EUR_SEK when o.currency = \'USD\' then O.SHIPPING_PRICE*USD_SEK else O.SHIPPING_PRICE end as SHIPPING_PRICE, O.QUANTITY, o.refunded_quantity, case when o.currency = \'DKK\' then O.refund_amount*DKK_SEK when o.currency = \'EUR\' then O.refund_amount*EUR_SEK when o.currency = \'USD\' then O.refund_amount*USD_SEK else O.refund_amount end as refund_amount, case when o.currency = \'DKK\' then O.TAX*DKK_SEK when o.currency = \'EUR\' then O.TAX*EUR_SEK when o.currency = \'USD\' then O.TAX*USD_SEK else O.TAX end as TAX, case when o.currency = \'DKK\' then O.DISCOUNT*DKK_SEK when o.currency = \'EUR\' then O.DISCOUNT*EUR_SEK when o.currency = \'USD\' then O.DISCOUNT*USD_SEK else O.DISCOUNT end as DISCOUNT, case when o.currency = \'DKK\' then O.GROSS_SALES_AFTER_TAX*DKK_SEK when o.currency = \'EUR\' then O.GROSS_SALES_AFTER_TAX*EUR_SEK when o.currency = \'USD\' then O.GROSS_SALES_AFTER_TAX*USD_SEK else O.GROSS_SALES_AFTER_TAX end as GROSS_SALES_AFTER_TAX, case when o.currency = \'DKK\' then O.GROSS_SALES_BEFORE_TAX*DKK_SEK when o.currency = \'EUR\' then O.GROSS_SALES_BEFORE_TAX*EUR_SEK when o.currency = \'USD\' then O.GROSS_SALES_BEFORE_TAX*USD_SEK else O.GROSS_SALES_BEFORE_TAX end as GROSS_SALES_BEFORE_TAX, case when o.currency = \'DKK\' then O.NET_SALES_BEFORE_TAX*DKK_SEK when o.currency = \'EUR\' then O.NET_SALES_BEFORE_TAX*EUR_SEK when o.currency = \'USD\' then O.NET_SALES_BEFORE_TAX*USD_SEK else O.NET_SALES_BEFORE_TAX end as NET_SALES_BEFORE_TAX, case when o.currency = \'DKK\' then O.TOTAL_SALES*DKK_SEK when o.currency = \'EUR\' then O.TOTAL_SALES*EUR_SEK when o.currency = \'USD\' then O.TOTAL_SALES*USD_SEK else O.TOTAL_SALES end as TOTAL_SALES FROM Pomme_db.maplemonk.woocommerce_pomme_All_orders_items O left join pomme_db.maplemonk.exhange_rates e on to_date(e.\"DATE \") = o.order_timestamp::date ; ALTER TABLE pomme_DB.maplemonk.FACT_ITEMS_woocommerce_pomme ADD COLUMN new_customer_flag varchar(50); ALTER TABLE pomme_DB.maplemonk.FACT_ITEMS_woocommerce_pomme ADD COLUMN new_customer_flag_month varchar(50); ALTER TABLE pomme_DB.maplemonk.FACT_ITEMS_woocommerce_pomme ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE pomme_DB.maplemonk.FACT_ITEMS_woocommerce_pomme ADD COLUMN acquisition_product varchar(16777216); UPDATE pomme_DB.maplemonk.FACT_ITEMS_woocommerce_pomme AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id, order_timestamp, CASE WHEN order_timestamp <> Min(order_timestamp) OVER ( partition BY customer_id) THEN \'Repeat\' ELSE \'New\' END AS Flag FROM pomme_DB.maplemonk.FACT_ITEMS_woocommerce_pomme)AS B WHERE A.order_id = B.order_id AND A.customer_id = B.customer_id; UPDATE pomme_DB.maplemonk.FACT_ITEMS_woocommerce_pomme SET new_customer_flag = CASE WHEN new_customer_flag IS NULL THEN \'New\' ELSE new_customer_flag END; UPDATE pomme_DB.maplemonk.FACT_ITEMS_woocommerce_pomme AS A SET A.new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, customer_id, order_timestamp, CASE WHEN Last_day(order_timestamp, \'month\') <> Last_day(Min(order_timestamp) OVER ( partition BY customer_id)) THEN \'Repeated\' ELSE \'New\' END AS Flag FROM pomme_DB.maplemonk.FACT_ITEMS_woocommerce_pomme)AS B WHERE A.order_id = B.order_id AND A.customer_id = B.customer_id; CREATE OR replace temporary TABLE pomme_DB.maplemonk.temp_product AS SELECT DISTINCT customer_id, product_name, Row_number() OVER (partition BY customer_id ORDER BY LINE_ITEM_SALES DESC) rowid FROM ( SELECT DISTINCT customer_id, order_timestamp, product_name, LINE_ITEM_SALES , Min(order_timestamp) OVER (partition BY customer_id) firstOrderdate FROM pomme_DB.maplemonk.FACT_ITEMS_woocommerce_pomme )res WHERE order_timestamp=firstorderdate; UPDATE pomme_DB.maplemonk.FACT_ITEMS_woocommerce_pomme AS A SET A.acquisition_product=B.product_name FROM ( SELECT * FROM pomme_DB.maplemonk.temp_product WHERE rowid=1)B WHERE A.customer_id = B.customer_id;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from Pomme_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        