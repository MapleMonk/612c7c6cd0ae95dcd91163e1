{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_WOOCOMMERCE_ALL_PRODUCTS AS select * ,\'WOOCOMMERCE\' AS Shop_Name from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_WOOCOMMERCE_PRODUCTS; CREATE OR REPLACE TABLE SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_Woocommerce_All_customers AS select _AIRBYTE_UNIQUE_KEY, ID, ROLE, EMAIL, _LINKS, BILLING, SHIPPING, \'www.sleepycat.in\' as shop_url, USERNAME, LAST_NAME, META_DATA, AVATAR_URL, FIRST_NAME, DATE_CREATED, DATE_MODIFIED, DATE_CREATED_GMT, DATE_MODIFIED_GMT, IS_PAYING_CUSTOMER, _AIRBYTE_AB_ID, _AIRBYTE_EMITTED_AT, _AIRBYTE_NORMALIZED_AT, _AIRBYTE_SLEEPYCAT_WOOCOMMERCE_CUSTOMERS_HASHID, \'WOOCOMMERCE\' AS Shop_Name from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_WOOCOMMERCE_CUSTOMERS; CREATE OR REPLACE TABLE SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_Woocommerce_All_orders AS select _AIRBYTE_UNIQUE_KEY, ID, TOTAL, _LINKS, NUMBER, STATUS, BILLING, REFUNDS, VERSION, CART_TAX, CURRENCY, SET_PAID, SHIPPING, \'www.sleepycat.in\' as shop_url, CART_HASH, DATE_PAID, FEE_LINES, META_DATA, ORDER_KEY, PARENT_ID, TAX_LINES, TOTAL_TAX, LINE_ITEMS, CREATED_VIA, CUSTOMER_ID, COUPON_LINES, DATE_CREATED, DISCOUNT_TAX, SHIPPING_TAX, CUSTOMER_NOTE, DATE_MODIFIED, DATE_PAID_GMT, DATE_COMPLETED, DISCOUNT_TOTAL, PAYMENT_METHOD, SHIPPING_LINES, SHIPPING_TOTAL, TRANSACTION_ID, DATE_CREATED_GMT, DATE_MODIFIED_GMT, DATE_COMPLETED_GMT, PRICES_INCLUDE_TAX, CUSTOMER_IP_ADDRESS, CUSTOMER_USER_AGENT, PAYMENT_METHOD_TITLE, _AIRBYTE_AB_ID, _AIRBYTE_EMITTED_AT, _AIRBYTE_NORMALIZED_AT, _AIRBYTE_SLEEPYCAT_WOOCOMMERCE_ORDERS_HASHID, utm_medium, utm_source, utm_campaign, utm_content, utm_path, source as utm_mapped_source, channel as utm_mapped_channel, \'WOOCOMMERCE\' AS Shop_Name from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_WOOCOMMERCE_ORDERS woo left join ( select utm.*, UTM_MAPPING.source, UTM_MAPPING.channel from ( select order_id, b:utm_medium::string AS utm_medium, b:utm_source::string AS utm_source, b:utm_campaign :: string AS utm_campaign, b:utm_content :: string AS utm_content, b:path :: string AS utm_path from (select order_id, PARSE_JSON(utm_parameters) b from (select * from ( select ID as order_id, A.value:id as id, A.value:key as key, A.value:value as utm_parameters from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_WOOCOMMERCE_ORDERS , LATERAL FLATTEN (INPUT => META_DATA)A ) where key = \'query_params_track\' ) ) )utm left join ( select * from ( select *, row_number() over (partition by lower(concat(lower(ifnull(utm_source,\'\')),lower(ifnull(utm_medium,\'\')))) order by 1) rw from sleepycat_db.MAPLEMONK.utm_mapping ) where rw=1 and lower(concat(ifnull(utm_source,\'\'),ifnull(utm_medium,\'\'))) is not null ) UTM_MAPPING on lower(concat(ifnull(utm.UTM_SOURCE,\'\'),ifnull(utm.UTM_MEDIUM,\'\'))) = lower(concat(ifnull(UTM_MAPPING.utm_source,\'\'),ifnull(UTM_MAPPING.utm_medium,\'\'))) )utm_clmn on woo.id = utm_clmn.order_id ; CREATE OR REPLACE TABLE SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_Woocommerce_All_orders_items_discount AS SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, discount_total/count(distinct A.value:id) over (partition by id, shop_name) AS discount_allocations, shop_name FROM SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_Woocommerce_ALL_ORDERS, LATERAL FLATTEN (INPUT => LINE_ITEMS)A; CREATE OR REPLACE TABLE SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_Woocommerce_All_orders_items_tax AS SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, shop_name, replace(A.VALUE:total_tax,\'\"\',\'\') AS tax_lines FROM SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_Woocommerce_ALL_ORDERS, LATERAL FLATTEN (INPUT => LINE_ITEMS)A; CREATE OR REPLACE TABLE SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_Woocommerce_All_Refunds AS select id ,replace(A.value:id,\'\"\',\'\') Refund_ID , replace(A.value:total,\'\"\',\'\')::float refund_amount , replace(A.value:quantity,\'\"\',\'\') quantity , \'WOOCOMMERCE\' AS Shop_Name from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_Woocommerce_ALL_ORDERS, LATERAL FLATTEN (INPUT => REFUNDS)A; CREATE OR REPLACE TABLE SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_Woocommerce_All_orders_items AS WITH CTE AS (SELECT SHOP_NAME, ID::VARCHAR(16777216) AS ORDER_ID, order_key AS ORDER_NAME, CUSTOMER_id, concat(replace(billing:first_name,\'\"\',\'\'),\' \',replace(billing:last_name,\'\"\',\'\')) NAME, replace(billing:phone,\'\"\',\'\') PHONE, replace(billing:email,\'\"\',\'\') EMAIL, replace(shipping:country,\'\"\',\'\') Country, replace(shipping:state,\'\"\',\'\') State, replace(shipping:city,\'\"\',\'\') City, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:sku::STRING AS SKU, A.VALUE:product_id::STRING AS PRODUCT_ID, A.VALUE:name::STRING AS PRODUCT_NAME, A.VALUE:parent_name::STRING AS PRODUCT_PARENT_NAME, CURRENCY, status order_status, DATE_CREATED_GMT::DATETIME AS order_timestamp, replace(A.VALUE:total,\'\"\',\'\') + replace(A.VALUE:total_tax,\'\"\',\'\') AS LINE_ITEM_SALES, shipping_total/COUNT(A.VALUE:id) OVER(PARTITION BY ORDER_ID ) AS SHIPPING_PRICE, shipping_tax/COUNT(A.VALUE:id) OVER(PARTITION BY ORDER_ID ) as shipping_tax, replace(A.VALUE:quantity,\'\"\',\'\')::FLOAT as QUANTITY, utm_medium, utm_source, utm_campaign, utm_content, utm_path, utm_mapped_source, utm_mapped_channel, \'WOOCOMMERCE\' AS Source, payment_method as payment_gateway, case when lower(payment_method) like \'%cod%\' then \'COD\' else \'PREPAID\' end payment_method FROM SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_Woocommerce_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS) A ) SELECT CTE.* ,IFNULL(D.DISCOUNT_ALLOCATIONS,0) AS DISCOUNT ,CTE.LINE_ITEM_SALES AS GROSS_SALES_AFTER_TAX ,CTE.LINE_ITEM_SALES - ifnull(tax_lines,0) AS GROSS_SALES_BEFORE_TAX ,CTE.LINE_ITEM_SALES - ifnull(tax_lines,0) - ifnull(discount_allocations,0) AS NET_SALES_BEFORE_TAX ,IFNULL(T.TAX_lines,0) AS TAX ,CTE.LINE_ITEM_SALES + ifnull(shipping_price,0) + ifnull(shipping_tax,0) AS TOTAL_SALES FROM CTE LEFT JOIN SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_Woocommerce_All_orders_items_tax T ON CTE.ORDER_ID = T.ORDER_ID AND CTE.LINE_ITEM_ID = T.LINE_ITEM_ID and CTE.shop_name = T.shop_name LEFT JOIN SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_Woocommerce_All_orders_items_discount D ON CTE.ORDER_ID = D.ORDER_ID AND CTE.LINE_ITEM_ID = D.LINE_ITEM_ID and CTE.shop_name = D.shop_name ; CREATE OR REPLACE TABLE SLEEPYCAT_DB.MAPLEMONK.SleepyCat_DB_Woocommerce_FACT_ITEMS AS with meta_data as ( select distinct id as order_id ,A.VALUE:id AS LINE_ITEM_ID ,A.value:sku ::string as sku ,A.value:product_id ::string as product_id ,A.value:parent_name ::string as parent_name ,b.value:display_key::string as key ,b.value:display_value::string as value from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_WOOCOMMERCE_ORDERS ,lateral flatten(INPUT => line_items,outer => true)A,LATERAL FLATTEN (INPUT => A.value:meta_data,outer=>true)B ) SELECT O.SHOP_NAME, O.ORDER_ID, O.ORDER_NAME, O.CUSTOMER_id customer_id, O.Name, O.EMAIL, O.PHONE, O.LINE_ITEM_ID, O.SKU, O.PRODUCT_ID, o.product_name, O.CURRENCY, o.city, o.State, o.country, O.order_status, O.order_timestamp, O.line_item_sales, O.SHIPPING_PRICE, O.QUANTITY, O.TAX, O.DISCOUNT, O.GROSS_SALES_AFTER_TAX, O.GROSS_SALES_BEFORE_TAX, O.NET_SALES_BEFORE_TAX, O.TOTAL_SALES, O.payment_gateway, O.payment_method, O.utm_medium, O.utm_source, O.utm_campaign, O.utm_content, O.utm_path, O.utm_mapped_source, O.utm_mapped_channel, cs.value as cart_source, cbk.value as custom_bundle_key_value, case when lower(cbk.value) like \'%diy_move_in_bundle%\' then \'Move In bundle\' when lower(cbk.value) like \'%diy_ortho_bundle%\' then \'Ortho bundle\' when lower(cbk.value) like \'%diy_bundle%\' then \'BYOB\' when cbk.value is not null then \'others\' end as custom_bundle_key, cp.value as page_url FROM SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_Woocommerce_All_orders_items O left join (select * from meta_data where ifnull(key,\'\') = \'cart_source\')cs on o.order_id = cs.order_id and o.line_item_id = cs.line_item_id left join (select * from meta_data where ifnull(key,\'\') = \'custom_bundle_key\')cbk on o.order_id = cbk.order_id and o.line_item_id = cbk.line_item_id left join (select * from meta_data where ifnull(key,\'\') = \'current_page\')cp on o.order_id = cp.order_id and o.line_item_id = cp.line_item_id ; ALTER TABLE SLEEPYCAT_DB.MAPLEMONK.SleepyCat_DB_Woocommerce_FACT_ITEMS ADD COLUMN new_customer_flag varchar(50); ALTER TABLE SLEEPYCAT_DB.MAPLEMONK.SleepyCat_DB_Woocommerce_FACT_ITEMS ADD COLUMN new_customer_flag_month varchar(50); ALTER TABLE SLEEPYCAT_DB.MAPLEMONK.SleepyCat_DB_Woocommerce_FACT_ITEMS ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE SLEEPYCAT_DB.MAPLEMONK.SleepyCat_DB_Woocommerce_FACT_ITEMS ADD COLUMN acquisition_product varchar(16777216); UPDATE SLEEPYCAT_DB.MAPLEMONK.SleepyCat_DB_Woocommerce_FACT_ITEMS AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id, order_timestamp, CASE WHEN order_timestamp <> Min(order_timestamp) OVER ( partition BY customer_id) THEN \'Repeat\' ELSE \'New\' END AS Flag FROM SLEEPYCAT_DB.MAPLEMONK.SleepyCat_DB_Woocommerce_FACT_ITEMS)AS B WHERE A.order_id = B.order_id AND A.customer_id = B.customer_id; UPDATE SLEEPYCAT_DB.MAPLEMONK.SleepyCat_DB_Woocommerce_FACT_ITEMS SET new_customer_flag = CASE WHEN new_customer_flag IS NULL THEN \'New\' ELSE new_customer_flag END; UPDATE SLEEPYCAT_DB.MAPLEMONK.SleepyCat_DB_Woocommerce_FACT_ITEMS AS A SET A.new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, customer_id, order_timestamp, CASE WHEN Last_day(order_timestamp, \'month\') <> Last_day(Min(order_timestamp) OVER ( partition BY customer_id)) THEN \'Repeated\' ELSE \'New\' END AS Flag FROM SLEEPYCAT_DB.MAPLEMONK.SleepyCat_DB_Woocommerce_FACT_ITEMS)AS B WHERE A.order_id = B.order_id AND A.customer_id = B.customer_id; CREATE OR replace temporary TABLE SLEEPYCAT_DB.MAPLEMONK.temp_product AS SELECT DISTINCT customer_id, product_name, Row_number() OVER (partition BY customer_id ORDER BY LINE_ITEM_SALES DESC) rowid FROM ( SELECT DISTINCT customer_id, order_timestamp, product_name, LINE_ITEM_SALES , Min(order_timestamp) OVER (partition BY customer_id) firstOrderdate FROM SLEEPYCAT_DB.MAPLEMONK.SleepyCat_DB_Woocommerce_FACT_ITEMS) res WHERE order_timestamp=firstorderdate; UPDATE SLEEPYCAT_DB.MAPLEMONK.SleepyCat_DB_Woocommerce_FACT_ITEMS AS A SET A.acquisition_product=B.product_name FROM ( SELECT * FROM SLEEPYCAT_DB.MAPLEMONK.temp_product WHERE rowid=1)B WHERE A.customer_id = B.customer_id;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from SLEEPYCAT_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        