{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE ghc_db.maplemonk.Shopify_International_All_customers AS select *,\'Shopify_International_Saturn\' AS Shop_Name from ghc_db.maplemonk.shopify_saturn_international_customers UNION select *,\'Shopify_International_Mars\' AS Shop_Name from ghc_db.maplemonk.shopify_mars_international_customers; CREATE OR REPLACE TABLE GHC_DB.maplemonk.Shopify_International_All_orders AS SELECT *, iff(charindex(\'utm_medium=\', landing_site)=0,NULL,substring(landing_site, charindex(\'utm_medium=\', landing_site) + len(\'utm_medium=\'), ifnull(nullif(charindex(\'&\', insert(landing_site, 1, charindex(\'utm_medium=\', landing_site)-1, \'\')),0), len(insert(landing_site, 1, charindex(\'utm_medium=\', landing_site)-1, \'\'))+1) - len(\'utm_medium=\')-1)) as LANDING_UTM_MEDIUM, iff(charindex(\'utm_source=\', landing_site)=0,NULL,substring(landing_site, charindex(\'utm_source=\', landing_site) + len(\'utm_source=\'), ifnull(nullif(charindex(\'&\', insert(landing_site, 1, charindex(\'utm_source=\', landing_site)-1, \'\')),0), len(insert(landing_site, 1, charindex(\'utm_source=\', landing_site)-1, \'\'))+1) - len(\'utm_source=\')-1)) as LANDING_UTM_SOURCE, ltrim(left(right(landing_Site,len(landing_site)-charindex(\'utm_campaign=\',landing_site)+1), case when contains(right(landing_site,len(landing_site)-charindex(\'utm_campaign=\',landing_site)+1),\'&\') = \'TRUE\' then position(\'&\',right(landing_site,len(landing_site)-charindex(\'utm_campaign=\',landing_site)+1),1) else len(right(landing_site,len(landing_site)-charindex(\'utm_campaign=\',landing_site)+1)) +1 end -1),\'utm_campaign=\') as LANDING_UTM_CAMPAIGN, iff(charindex(\'utm_medium=\', REFERRING_SITE)=0,NULL,substring(REFERRING_SITE, charindex(\'utm_medium=\', REFERRING_SITE) + len(\'utm_medium=\'), ifnull(nullif(charindex(\'&\', insert(REFERRING_SITE, 1, charindex(\'utm_medium=\', REFERRING_SITE)-1, \'\')),0), len(insert(REFERRING_SITE, 1, charindex(\'utm_medium=\', REFERRING_SITE)-1, \'\'))+1) - len(\'utm_medium=\')-1)) as REFERRING_UTM_MEDIUM, iff(charindex(\'utm_source=\', REFERRING_SITE)=0,NULL,substring(REFERRING_SITE, charindex(\'utm_source=\', REFERRING_SITE) + len(\'utm_source=\'), ifnull(nullif(charindex(\'&\', insert(REFERRING_SITE, 1, charindex(\'utm_source=\', REFERRING_SITE)-1, \'\')),0), len(insert(REFERRING_SITE, 1, charindex(\'utm_source=\', REFERRING_SITE)-1, \'\'))+1) - len(\'utm_source=\')-1)) as REFERRING_UTM_SOURCE, ltrim(left(right(referring_Site,len(referring_Site)-charindex(\'utm_campaign=\',referring_Site)+1), case when contains(right(referring_Site,len(referring_Site)-charindex(\'utm_campaign=\',referring_Site)+1),\'&\') = \'TRUE\' then position(\'&\',right(referring_Site,len(referring_Site)-charindex(\'utm_campaign=\',referring_Site)+1),1) else len(right(referring_Site,len(referring_Site)-charindex(\'utm_campaign=\',referring_Site)+1)) +1 end -1),\'utm_campaign=\') as REFERRING_UTM_CAMPAIGN, CASE WHEN LANDING_SITE LIKE \'%facebook.com%\' THEN \'Facebook\' WHEN LANDING_SITE LIKE \'%google.com%\' THEN \'Google\' WHEN LANDING_SITE LIKE \'%google.co%\' THEN \'Google\' WHEN LANDING_SITE LIKE \'%instagram.com%\' THEN \'Facebook\' WHEN LANDING_SITE LIKE \'%com.google%\' THEN \'Google\' ELSE NULL END AS LANDING_UTM_CHANNEL, CASE WHEN REFERRING_SITE LIKE \'%facebook.com%\' THEN \'Facebook\' WHEN REFERRING_SITE LIKE \'%google.com%\' THEN \'Google\' WHEN REFERRING_SITE LIKE \'%google.co%\' THEN \'Google\' WHEN REFERRING_SITE LIKE \'%instagram.com%\' THEN \'Facebook\' WHEN REFERRING_SITE LIKE \'%com.google%\' THEN \'Google\' ELSE NULL END AS REFERRING_UTM_CHANNEL FROM (select *,\'Shopify_Saturn\' AS Shop_Name from GHC_DB.maplemonk.SHOPIFY_SATURN_International_ORDERS)X UNION SELECT *, iff(charindex(\'utm_medium=\', landing_site)=0,NULL,substring(landing_site, charindex(\'utm_medium=\', landing_site) + len(\'utm_medium=\'), ifnull(nullif(charindex(\'&\', insert(landing_site, 1, charindex(\'utm_medium=\', landing_site)-1, \'\')),0), len(insert(landing_site, 1, charindex(\'utm_medium=\', landing_site)-1, \'\'))+1) - len(\'utm_medium=\')-1)) as LANDING_UTM_MEDIUM, iff(charindex(\'utm_source=\', landing_site)=0,NULL,substring(landing_site, charindex(\'utm_source=\', landing_site) + len(\'utm_source=\'), ifnull(nullif(charindex(\'&\', insert(landing_site, 1, charindex(\'utm_source=\', landing_site)-1, \'\')),0), len(insert(landing_site, 1, charindex(\'utm_source=\', landing_site)-1, \'\'))+1) - len(\'utm_source=\')-1)) as LANDING_UTM_SOURCE, ltrim(left(right(landing_Site,len(landing_site)-charindex(\'utm_campaign=\',landing_site)+1), case when contains(right(landing_site,len(landing_site)-charindex(\'utm_campaign=\',landing_site)+1),\'&\') = \'TRUE\' then position(\'&\',right(landing_site,len(landing_site)-charindex(\'utm_campaign=\',landing_site)+1),1) else len(right(landing_site,len(landing_site)-charindex(\'utm_campaign=\',landing_site)+1)) +1 end -1),\'utm_campaign=\') as LANDING_UTM_CAMPAIGN, iff(charindex(\'utm_medium=\', REFERRING_SITE)=0,NULL,substring(REFERRING_SITE, charindex(\'utm_medium=\', REFERRING_SITE) + len(\'utm_medium=\'), ifnull(nullif(charindex(\'&\', insert(REFERRING_SITE, 1, charindex(\'utm_medium=\', REFERRING_SITE)-1, \'\')),0), len(insert(REFERRING_SITE, 1, charindex(\'utm_medium=\', REFERRING_SITE)-1, \'\'))+1) - len(\'utm_medium=\')-1)) as REFERRING_UTM_MEDIUM, iff(charindex(\'utm_source=\', REFERRING_SITE)=0,NULL,substring(REFERRING_SITE, charindex(\'utm_source=\', REFERRING_SITE) + len(\'utm_source=\'), ifnull(nullif(charindex(\'&\', insert(REFERRING_SITE, 1, charindex(\'utm_source=\', REFERRING_SITE)-1, \'\')),0), len(insert(REFERRING_SITE, 1, charindex(\'utm_source=\', REFERRING_SITE)-1, \'\'))+1) - len(\'utm_source=\')-1)) as REFERRING_UTM_SOURCE, ltrim(left(right(referring_Site,len(referring_Site)-charindex(\'utm_campaign=\',referring_Site)+1), case when contains(right(referring_Site,len(referring_Site)-charindex(\'utm_campaign=\',referring_Site)+1),\'&\') = \'TRUE\' then position(\'&\',right(referring_Site,len(referring_Site)-charindex(\'utm_campaign=\',referring_Site)+1),1) else len(right(referring_Site,len(referring_Site)-charindex(\'utm_campaign=\',referring_Site)+1)) +1 end -1),\'utm_campaign=\') as REFERRING_UTM_CAMPAIGN, CASE WHEN LANDING_SITE LIKE \'%facebook.com%\' THEN \'Facebook\' WHEN LANDING_SITE LIKE \'%google.com%\' THEN \'Google\' WHEN LANDING_SITE LIKE \'%google.co%\' THEN \'Google\' WHEN LANDING_SITE LIKE \'%instagram.com%\' THEN \'Facebook\' WHEN LANDING_SITE LIKE \'%com.google%\' THEN \'Google\' ELSE NULL END AS LANDING_UTM_CHANNEL, CASE WHEN REFERRING_SITE LIKE \'%facebook.com%\' THEN \'Facebook\' WHEN REFERRING_SITE LIKE \'%google.com%\' THEN \'Google\' WHEN REFERRING_SITE LIKE \'%google.co%\' THEN \'Google\' WHEN REFERRING_SITE LIKE \'%instagram.com%\' THEN \'Facebook\' WHEN REFERRING_SITE LIKE \'%com.google%\' THEN \'Google\' ELSE NULL END AS REFERRING_UTM_CHANNEL FROM (select *,\'Shopify_Mars\' AS Shop_Name from GHC_DB.maplemonk.SHOPIFY_MARS_International_ORDERS)X; ALTER TABLE GHC_DB.maplemonk.Shopify_International_All_orders RENAME COLUMN _AIRBYTE_SHOPIFY_saturn_International_ORDERS_HASHID to _AIRBYTE_ORDERS_HASHID; CREATE OR REPLACE TABLE ghc_db.maplemonk.Shopify_International_All_products AS select *,\'Shopify_International_Saturn\' AS Shop_Name from GHC_DB.maplemonk.SHOPIFY_SATURN_International_PRODUCTS UNION select *,\'Shopify_International_Mars\' AS Shop_Name from GHC_DB.maplemonk.SHOPIFY_MARS_International_PRODUCTS; ALTER TABLE GHC_DB.maplemonk.Shopify_International_All_products RENAME COLUMN _AIRBYTE_SHOPIFY_saturn_International_PRODUCTS_HASHID to _AIRBYTE_PRODUCTS_HASHID; CREATE OR REPLACE TABLE GHC_DB.maplemonk.Shopify_International_All_products_variants AS select *,\'Shopify_International_Saturn\' AS Shop_Name from GHC_DB.maplemonk.SHOPIFY_SATURN_International_PRODUCTS_VARIANTS UNION select *,\'Shopify_International_Mars\' AS Shop_Name from GHC_DB.maplemonk.SHOPIFY_MARS_International_PRODUCTS_VARIANTS; ALTER TABLE GHC_DB.maplemonk.Shopify_International_All_products_variants RENAME COLUMN _AIRBYTE_SHOPIFY_SATURN_International_PRODUCTS_HASHID to _AIRBYTE_PRODUCTS_HASHID; CREATE OR REPLACE TABLE GHC_DB.maplemonk.Shopify_International_All_customers_addresses AS select *,\'Shopify_International_Saturn\' AS Shop_Name from GHC_DB.maplemonk.SHOPIFY_SATURN_International_CUSTOMERS_ADDRESSES UNION select *,\'Shopify_International_Mars\' AS Shop_Name from GHC_DB.maplemonk.SHOPIFY_MARS_International_CUSTOMERS_ADDRESSES; CREATE OR REPLACE TABLE GHC_DB.maplemonk.Shopify_International_All_orders_items_discount AS SELECT order_id, LINE_ITEM_ID, SUM(B.VALUE:amount::FLOAT) AS DISCOUNT FROM( SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:discount_allocations AS discount_allocations FROM GHC_DB.maplemonk.Shopify_International_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A)x,LATERAL FLATTEN (INPUT => x.discount_allocations)B GROUP BY ORDER_ID, LINE_ITEM_ID; CREATE OR REPLACE TABLE GHC_DB.maplemonk.Shopify_International_All_orders_items_tax AS SELECT order_id, LINE_ITEM_ID, SUM(B.VALUE:price::FLOAT) AS TAX, avg(B.VALUE:rate::float) as Tax_Rate FROM( SELECT id AS order_id, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:tax_lines AS tax_lines FROM GHC_DB.maplemonk.Shopify_International_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A)x,LATERAL FLATTEN (INPUT => x.tax_lines)B GROUP BY order_id, LINE_ITEM_ID; CREATE OR REPLACE TABLE GHC_DB.maplemonk.Shopify_International_All_Refunds AS SELECT Name, ID AS Order_ID, Shop_name, C.value:line_item_id AS line_item_id, SUM(C.VALUE:quantity) AS quantity, SUM(C.VALUE:subtotal) AS subtotal FROM GHC_DB.maplemonk.Shopify_International_All_orders, LATERAL FLATTEN(INPUT => refunds)A, LATERAL FLATTEN(INPUT=>A.value)B, LATERAL FLATTEN(INPUT => B.value) C WHERE C.value:line_item_id IS NOT null GROUP BY Name, ID, Shop_name, C.value:line_item_id; CREATE OR REPLACE TABLE GHC_DB.maplemonk.Shopify_International_All_orders_items AS WITH CTE AS (SELECT SHOP_NAME, ID::VARCHAR(16777216) AS ORDER_ID, NAME AS ORDER_NAME, CUSTOMER, replace(customer:default_address:name,\'\"\',\'\') NAME, coalesce(phone, replace(customer:default_address:phone,\'\"\',\'\')) phone, coalesce(email, replace(customer:email,\'\"\',\'\')) email, replace(billing_address:\"zip\",\'\"\',\'\') as pin_code, A.VALUE:id AS LINE_ITEM_ID, A.VALUE:sku::STRING AS SKU, A.VALUE:product_id::STRING AS PRODUCT_ID, A.VALUE:title::STRING AS PRODUCT_NAME, CURRENCY, CASE WHEN cancelled_at IS NOT NULL THEN \'CANCELLED\' ELSE \'Shopify_Processed\' END AS order_status, CREATED_AT::DATETIME AS order_timestamp, A.VALUE:price::FLOAT * A.VALUE:quantity::FLOAT AS LINE_ITEM_SALES, (TOTAL_SHIPPING_PRICE_SET:presentment_money:amount::FLOAT/ COUNT(ORDER_ID) OVER(PARTITION BY ORDER_ID ORDER BY ORDER_ID)) AS SHIPPING_PRICE, A.value:price/(1+A.value:tax_lines:rate), A.VALUE:quantity::FLOAT as QUANTITY, \'Shopify\' AS Source, LANDING_UTM_MEDIUM, LANDING_UTM_SOURCE, REFERRING_UTM_MEDIUM, REFERRING_UTM_SOURCE, REFERRING_UTM_CAMPAIGN, LANDING_UTM_CAMPAIGN, updated_at, app_id FROM GHC_DB.maplemonk.Shopify_International_All_orders , LATERAL FLATTEN (INPUT => LINE_ITEMS)A) SELECT CTE.*, IFNULL(T.TAX_RATE,0) AS TAX_RATE, IFNULL(D.DISCOUNT,0) AS DISCOUNT, CTE.LINE_ITEM_SALES AS GROSS_SALES_AFTER_TAX, CTE.LINE_ITEM_SALES/(1+IFNULL(T.TAX_RATE,0)) AS GROSS_SALES_BEFORE_TAX, IFNULL(D.DISCOUNT,0)/(1+IFNULL(T.TAX_RATE,0)) AS DISCOUNT_BEFORE_TAX, CTE.LINE_ITEM_SALES/(1+IFNULL(T.TAX_RATE,0)) - IFNULL(D.DISCOUNT,0)/(1+IFNULL(T.TAX_RATE,0)) AS NET_SALES_BEFORE_TAX, IFNULL(T.TAX,0) AS TAX, (CTE.LINE_ITEM_SALES/(1+IFNULL(T.TAX_RATE,0))) - (IFNULL(D.DISCOUNT,0)/(1+IFNULL(T.TAX_RATE,0))) + IFNULL(T.TAX,0) + CTE.SHIPPING_PRICE AS TOTAL_SALES, CASE WHEN R.QUANTITY IS NOT NULL THEN 1 ELSE 0 END AS IS_REFUND FROM CTE LEFT JOIN GHC_DB.maplemonk.Shopify_International_All_orders_items_tax T ON CTE.ORDER_ID = T.ORDER_ID AND CTE.LINE_ITEM_ID = T.LINE_ITEM_ID LEFT JOIN GHC_DB.maplemonk.Shopify_International_All_orders_items_discount D ON CTE.ORDER_ID = D.ORDER_ID AND CTE.LINE_ITEM_ID = D.LINE_ITEM_ID LEFT JOIN GHC_DB.maplemonk.Shopify_International_All_Refunds R ON CTE.ORDER_ID = R.ORDER_ID AND CTE.LINE_ITEM_ID = R.LINE_ITEM_ID; CREATE OR REPLACE TABLE GHC_DB.maplemonk.FACT_ITEMS_International_intermediate AS SELECT O.SHOP_NAME, O.ORDER_ID, O.ORDER_NAME, O.CUSTOMER:id::int AS customer_id, O.name, phone, email, O.LINE_ITEM_ID, O.SKU, case when lower(left(sku,position(\'-\',o.sku,1)-1)) = \'pk\' then \'Performance\' when lower(left(sku,position(\'-\',o.sku,1)-1)) = \'shk\' then \'Saturn Hair\' when lower(left(sku,position(\'-\',o.sku,1)-1)) = \'swl\' then \'Saturn Wellness\' when lower(left(sku,position(\'-\',o.sku,1)-1)) = \'ssk\' then \'Saturn Skin\' when lower(left(sku,position(\'-\',o.sku,1)-1)) = \'sk\' then \'Skin\' when lower(left(sku,position(\'-\',o.sku,1)-1)) = \'hk\' then \'Hair\' when lower(left(sku,position(\'-\',o.sku,1)-1)) = \'wl\' then \'Wellness\' when lower(left(sku,position(\'-\',o.sku,1)-1)) = \'bk\' then \'Beard\' when lower(left(sku,position(\'-\',o.sku,1)-1)) = \'pkc\' then \'Performance\' when lower(left(sku,position(\'-\',o.sku,1)-1)) = \'shkc\' then \'Saturn Hair\' when lower(left(sku,position(\'-\',o.sku,1)-1)) = \'swmc\' then \'Saturn Wellness\' when lower(left(sku,position(\'-\',o.sku,1)-1)) = \'sskc\' then \'Saturn Skin\' when lower(left(sku,position(\'-\',o.sku,1)-1)) = \'skc\' then \'Skin\' when lower(left(sku,position(\'-\',o.sku,1)-1)) = \'hkc\' then \'Hair\' when lower(left(sku,position(\'-\',o.sku,1)-1)) = \'wlc\' then \'Wellness\' when lower(left(sku,position(\'-\',o.sku,1)-1)) = \'bkc\' then \'Beard\' else \'Others\' end as category, O.PRODUCT_ID, O.CURRENCY, O.IS_REFUND, CASE WHEN CD.city IS NULL OR CD.city = \'\' THEN \'NA\' ELSE upper(CD.city) END AS city, CASE WHEN CD.province IS NULL OR CD.province = \'\' THEN \'NA\' ELSE upper(CD.province) END AS state, O.pin_code, CASE WHEN P.product_type = \'\' THEN \'NA\' ELSE P.product_type END AS product_type, O.order_status, O.order_timestamp, O.LINE_ITEM_SALES, O.SHIPPING_PRICE, O.QUANTITY, O.Tax_Rate, O.TAX, O.DISCOUNT, O.DISCOUNT_BEFORE_TAX, O.GROSS_SALES_AFTER_TAX, O.GROSS_SALES_BEFORE_TAX, O.NET_SALES_BEFORE_TAX, O.TOTAL_SALES, O.Source, O.LANDING_UTM_MEDIUM, O.LANDING_UTM_SOURCE, O.REFERRING_UTM_MEDIUM, O.REFERRING_UTM_SOURCE, O.LANDING_UTM_CAMPAIGN, case when lower(landing_utm_campaign) like \'%whatsapp%\' then \'Whatsapp\' when lower(landing_utm_campaign) like \'%sms%\' then \'SMS\' when lower(landing_utm_campaign) like \'%email%\' then \'Email\' when lower(landing_utm_campaign) like \'%wa_%\' then \'Whatsapp\' else \'Others\' end as landing_utm_flag, O.REFERRING_UTM_CAMPAIGN, case when lower(REFERRING_UTM_CAMPAIGN) like \'%whatsapp%\' then \'Whatsapp\' when lower(REFERRING_UTM_CAMPAIGN) like \'%sms%\' then \'SMS\' when lower(REFERRING_UTM_CAMPAIGN) like \'%email%\' then \'Email\' when lower(REFERRING_UTM_CAMPAIGN) like \'%wa_%\' then \'Whatsapp\' else \'Others\' end as referring_utm_flag, case when landing_utm_flag = \'Whatsapp\' or referring_utm_flag = \'Whatsapp\' then \'Whatsapp\' when landing_utm_flag = \'SMS\' or referring_utm_flag = \'SMS\' then \'SMS\' when landing_utm_flag = \'Email\' or referring_utm_flag = \'Email\' then \'Email\' else \'Others\' end as utm_flag, O.updated_at as last_updated_date, P.title as product_name, coalesce(b.team,c.team) team, app_id FROM GHC_DB.maplemonk.Shopify_International_All_orders_items O left join ghc_db.maplemonk.order_team_mapping_mars b on o.order_name = concat(\'#\',B.ORDER_ID) AND o.SHOP_NAME = \'Shopify_Mars\' left join ghc_db.maplemonk.order_team_mapping_saturn c on o.order_name = concat(\'#\',c.ORDER_ID) AND o.SHOP_NAME = \'Shopify_Saturn\' LEFT JOIN GHC_DB.maplemonk.Shopify_International_All_products P ON O.PRODUCT_ID = P.id LEFT JOIN(SELECT customer_id, city, province, row_number() OVER ( partition BY customer_id ORDER BY id DESC) rowid FROM GHC_DB.maplemonk.Shopify_International_All_customers_addresses ) AS CD ON O.CUSTOMER:id::int = CD.customer_id AND CD.rowid = 1; CREATE OR REPLACE TABLE GHC_DB.maplemonk.FACT_ITEMS_International AS select *, case when rw=1 then 1 else 0 end mapped_order from ( select *, row_number() over (partition by shop_name, order_id order by line_item_sales desc) rw from GHC_DB.maplemonk.FACT_ITEMS_International_intermediate ) ; ALTER TABLE GHC_DB.maplemonk.FACT_ITEMS_International ADD COLUMN customer_flag varchar(50); ALTER TABLE GHC_DB.maplemonk.FACT_ITEMS_International ADD COLUMN new_customer_flag varchar(50); ALTER TABLE GHC_DB.maplemonk.FACT_ITEMS_International ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE GHC_DB.maplemonk.FACT_ITEMS_International ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE GHC_DB.maplemonk.FACT_ITEMS_International ADD COLUMN ACQUISITION_date timestamp; UPDATE GHC_DB.maplemonk.FACT_ITEMS_International AS A SET A.ACQUISITION_DATE = B.ACQUISITION_DATE FROM ( select distinct customer_id , min(order_timestamp) OVER ( partition BY customer_id) ACQUISITION_DATE from GHC_DB.maplemonk.FACT_ITEMS_International B where lower(order_status) not in (\'cancelled\',\'returned\') ) AS B where A.customer_id = B.customer_id; UPDATE GHC_DB.maplemonk.FACT_ITEMS_International AS A SET A.customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id, order_timestamp, CASE WHEN order_timestamp = acquisition_Date and lower(order_status) not in (\'cancelled\', \'returned\') THEN \'New\' when ORDER_TIMESTAMP < acquisition_Date then \'Yet to make completed order\' else \'Repeated\' END AS Flag FROM GHC_DB.maplemonk.FACT_ITEMS_International )AS B WHERE A.order_id = B.order_id AND A.customer_id = B.customer_id and A.order_timestamp::date = b.order_timestamp::date ; UPDATE GHC_DB.maplemonk.FACT_ITEMS_International SET customer_flag = CASE WHEN customer_flag IS NULL and lower(order_status) not in (\'cancelled\', \'returned\') THEN \'New\' when customer_flag IS NULL and lower(order_status) in (\'cancelled\', \'returned\') THEN \'Yet to make completed order\' ELSE customer_flag END; UPDATE GHC_DB.maplemonk.FACT_ITEMS_International AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id, order_timestamp, CASE WHEN Last_day(order_timestamp, \'month\') <> Last_day(Min(order_timestamp) OVER ( partition BY customer_id)) THEN \'Repeated\' ELSE \'New\' END AS Flag FROM GHC_DB.maplemonk.FACT_ITEMS_International)AS B WHERE A.order_id = B.order_id AND A.customer_id = B.customer_id; UPDATE GHC_DB.maplemonk.FACT_ITEMS_International SET new_customer_flag = CASE WHEN new_customer_flag IS NULL THEN \'New\' ELSE new_customer_flag END; CREATE OR replace temporary TABLE GHC_DB.maplemonk.temp_source_International AS SELECT DISTINCT customer_id, source FROM ( SELECT DISTINCT customer_id, order_timestamp, source, Min(order_timestamp) OVER ( partition BY customer_id) firstOrderdate FROM GHC_DB.maplemonk.FACT_ITEMS_International)res WHERE order_timestamp=firstorderdate; UPDATE GHC_DB.maplemonk.FACT_ITEMS_International AS a SET a.acquisition_channel=b.source FROM GHC_DB.maplemonk.temp_source_International b WHERE a.customer_id = b.customer_id; CREATE OR replace temporary TABLE GHC_DB.maplemonk.temp_product_International AS SELECT DISTINCT customer_id, product_name, Row_number() OVER (partition BY customer_id ORDER BY LINE_ITEM_SALES DESC) rowid FROM ( SELECT DISTINCT customer_id, order_timestamp, product_name, LINE_ITEM_SALES , Min(order_timestamp) OVER (partition BY customer_id) firstOrderdate FROM GHC_DB.maplemonk.FACT_ITEMS_International )res WHERE order_timestamp=firstorderdate; UPDATE GHC_DB.maplemonk.FACT_ITEMS_International AS A SET A.acquisition_product=B.product_name FROM ( SELECT * FROM GHC_DB.maplemonk.temp_product_International WHERE rowid=1)B WHERE A.customer_id = B.customer_id; create or replace table ghc_db.maplemonk.fact_items_International_ghc as select a.*, b.sku order_sku, b.product_name order_product_name from ghc_db.maplemonk.fact_items_International a left join (select order_name, shop_name, sku, product_name from ( select order_name, shop_name, sku, product_name, row_number() over (partition by order_name order by line_item_sales desc) rw from GHC_DB.maplemonk.FACT_ITEMS_International )where rw=1 ) b on a.order_name = b.order_name and a.shop_name = b.shop_name ; CREATE OR REPLACE TABLE GHC_DB.maplemonk.FACT_ITEMS_SHOPIFY_International_ghc AS SELECT * , case when app_id = \'580111\' and utm_flag = \'Others\' and (lower(team) in (\'website\') or team is null) then \'website_orders\' when app_id = \'21293662209\' and utm_flag = \'Others\' then \'app_orders\' when utm_flag = \'Others\' and lower(team) in (\'abandoned\') then \'abandoned_orders\' when utm_flag = \'Others\' and lower(team) in (\'whatsapp\') and customer_flag = \'New\' then \'web_whatsapp_orders\' end as mapped_team FROM GHC_DB.maplemonk.FACT_ITEMS_International FI where SOURCE in (\'Shopify\');",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from GHC_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        