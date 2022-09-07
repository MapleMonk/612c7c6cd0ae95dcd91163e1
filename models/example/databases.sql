{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table MM_TEST.NaveenSept7.sales_consolidated_intermediate_MM_TEST as select customer_id ,SHOP_NAME ,FINAL_UTM_CHANNEL AS SOURCE ,ORDER_ID ,PHONE ,NAME ,EMAIL ,NULL AS SHIPPING_LAST_UPDATE_DATE ,SKU ,PRODUCT_ID ,PRODUCT_NAME ,CURRENCY ,CITY ,STATE AS State ,ORDER_STATUS ,ORDER_TIMESTAMP::date AS Order_Date ,SHIPPING_PRICE ,QUANTITY ,DISCOUNT_BEFORE_TAX AS DISCOUNT ,TAX ,TOTAL_SALES AS SELLING_PRICE ,NULL AS SHIPPINGPACKAGECODE ,NULL AS SHIPPINGPACKAGESTATUS ,LINE_ITEM_ID::varchar as SALEORDERITEMCODE ,LINE_ITEM_ID as SALES_ORDER_ITEM_ID ,NULL AS COURIER ,NULL AS SHIPPING_STATUS ,NULL AS DISPATCH_DATE ,NULL AS DELIVERED_STATUS ,IS_REFUND AS RETURN_FLAG ,case when is_refund = 1 then quantity::int end returned_quantity ,case when is_refund = 0 and lower(order_status) in (\'cancelled\') then quantity::int end cancelled_quantity ,NEW_CUSTOMER_FLAG::varchar new_customer_flag ,ACQUISITION_PRODUCT ,NULL AS DAYS_IN_SHIPMENT ,NULL AS ACQUSITION_DATE ,SKU_CODE ,PRODUCT_NAME_FINAL ,PRODUCT_CATEGORY ,PRODUCT_SUB_CATEGORY from MM_TEST.NaveenSept7.SHOPIFY_FACT_ITEMS_MM_TEST b union all select Null as customer_id ,SHOP_NAME ,\'Amazon\' AS SOURCE ,ORDER_ID ,Null as PHONE ,Null as NAME ,Null as EMAIL ,NULL AS SHIPPING_LAST_UPDATE_DATE ,SKU ,PRODUCT_ID ,PRODUCT_NAME ,CURRENCY ,CITY ,STATE AS State ,ORDER_STATUS ,ORDER_TIMESTAMP::date AS Order_Date ,SHIPPING_PRICE ,QUANTITY ,DISCOUNT_BEFORE_TAX AS DISCOUNT ,TAX ,TOTAL_SALES AS SELLING_PRICE ,NULL AS SHIPPINGPACKAGECODE ,NULL AS SHIPPINGPACKAGESTATUS ,NULL as SALES_ORDER_ITEM_ID ,NULL as SALEORDERITEMCODE ,NULL AS COURIER ,NULL AS SHIPPING_STATUS ,NULL AS DISPATCH_DATE ,NULL AS DELIVERED_STATUS ,IS_REFUND AS RETURN_FLAG ,case when is_refund = 1 then quantity::int end returned_quantity ,case when is_refund = 0 and lower(order_status) in (\'cancelled\') then quantity::int end cancelled_quantity ,NULL as NEW_CUSTOMER_FLAG ,NULL as ACQUISITION_PRODUCT ,NULL AS DAYS_IN_SHIPMENT ,NULL AS ACQUSITION_DATE ,SKU as SKU_CODE ,PRODUCT_NAME_FINAL ,PRODUCT_CATEGORY ,PRODUCT_SUB_CATEGORY from MM_TEST.NaveenSept7.AMAZON_FACT_ITEMS_MM_TEST b union all select Null as customer_id ,SHOP_NAME ,marketplace AS SOURCE ,ORDER_ID ,contact_num as PHONE ,customer_name as NAME ,email as EMAIL ,shipping_last_update_date AS SHIPPING_LAST_UPDATE_DATE ,SKU ,PRODUCT_ID ,PRODUCTNAME AS PRODUCT_NAME ,CURRENCY ,CITY ,STATE AS State ,ORDER_STATUS ,ORDER_DATE::date AS Order_Date ,SHIPPING_PRICE ,SUBORDER_QUANTITY AS QUANTITY ,DISCOUNT AS DISCOUNT ,TAX ,SELLING_PRICE AS SELLING_PRICE ,NULL AS SHIPPINGPACKAGECODE ,NULL AS SHIPPINGPACKAGESTATUS ,suborder_id as SALES_ORDER_ITEM_ID ,suborder_id as SALEORDERITEMCODE ,NULL AS COURIER ,shipping_status AS SHIPPING_STATUS ,NULL AS DISPATCH_DATE ,NULL AS DELIVERED_STATUS ,IS_REFUND AS RETURN_FLAG ,case when is_refund = 1 then suborder_quantity::int end returned_quantity ,case when is_refund = 0 and lower(order_status) in (\'cancelled\') then suborder_quantity::int end cancelled_quantity ,new_customer_flag::varchar as NEW_CUSTOMER_FLAG ,NULL as ACQUISITION_PRODUCT ,Days_in_shipment AS DAYS_IN_SHIPMENT ,NULL AS ACQUSITION_DATE ,Null as SKU_CODE ,Null as PRODUCT_NAME_FINAL ,Null as PRODUCT_CATEGORY ,Null as PRODUCT_SUB_CATEGORY from MM_TEST.NaveenSept7.EasyEcom_FACT_ITEMS_MM_TEST b where lower(marketplace) not like (\'%amazon%\') and lower(marketplace) not like (\'%shopify%\'); create or replace table MM_TEST.NaveenSept7.Final_customerID as with new_phone_numbers as ( select phone, contact_num, 19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) as contact_num, phone from MM_TEST.NaveenSept7.sales_consolidated_intermediate_MM_TEST ) a ), int as ( select contact_num, email, coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email, maple_monk_id, 19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email, maple_monk_id from ( select phone as contact_num, email from MM_TEST.NaveenSept7.sales_consolidated_intermediate_MM_TEST ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table MM_TEST.NaveenSept7.sales_consolidated_MM_TEST as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as customer_id_final, min(order_date) over(partition by customer_id_final) as acquisition_date, m.* from ( select c.maple_monk_id as maple_monk_id_phone, o.* from MM_TEST.NaveenSept7.sales_consolidated_intermediate_MM_TEST o left join ( select * from ( select contact_num phone, maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from MM_TEST.NaveenSept7.Final_customerID ) where magic =1 )c on c.phone = right(regexp_replace(o.phone, \'[^a-zA-Z0-9]+\'),10) )m left join ( select distinct maple_monk_id, email from MM_TEST.NaveenSept7.Final_customerID where contact_num is null )d on d.email = m.email; ALTER TABLE MM_TEST.NaveenSept7.sales_consolidated_MM_TEST drop COLUMN new_customer_flag ; ALTER TABLE MM_TEST.NaveenSept7.sales_consolidated_MM_TEST ADD COLUMN new_customer_flag varchar(50); ALTER TABLE MM_TEST.NaveenSept7.sales_consolidated_MM_TEST drop COLUMN acquisition_product ; ALTER TABLE MM_TEST.NaveenSept7.sales_consolidated_MM_TEST ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE MM_TEST.NaveenSept7.sales_consolidated_MM_TEST ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE MM_TEST.NaveenSept7.sales_consolidated_MM_TEST ADD COLUMN acquisition_marketplace varchar(16777216); UPDATE MM_TEST.NaveenSept7.sales_consolidated_MM_TEST AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Order_Date <> Min(Order_Date) OVER ( partition BY customer_id_final) THEN \'Repeat\' ELSE \'New\' END AS Flag FROM MM_TEST.NaveenSept7.sales_consolidated_MM_TEST)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE MM_TEST.NaveenSept7.sales_consolidated_MM_TEST SET new_customer_flag = CASE WHEN new_customer_flag IS NULL THEN \'New\' ELSE new_customer_flag END; CREATE OR replace temporary TABLE MM_TEST.NaveenSept7.temp_source_1 AS SELECT DISTINCT customer_id_final, channel, marketplace FROM ( SELECT DISTINCT customer_id_final, order_date, source as channel, shop_name as marketplace, Min(order_date) OVER ( partition BY customer_id_final) firstOrderdate FROM MM_TEST.NaveenSept7.sales_consolidated_MM_TEST ) res WHERE order_date=firstorderdate; UPDATE MM_TEST.NaveenSept7.sales_consolidated_MM_TEST AS a SET a.acquisition_channel=b.channel FROM MM_TEST.NaveenSept7.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; UPDATE MM_TEST.NaveenSept7.sales_consolidated_MM_TEST AS a SET a.acquisition_marketplace=b.marketplace FROM MM_TEST.NaveenSept7.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; CREATE OR replace temporary TABLE MM_TEST.NaveenSept7.temp_product_1 AS SELECT DISTINCT customer_id_final, product_name_final, Row_number() OVER (partition BY customer_id_final ORDER BY SELLING_PRICE DESC) rowid FROM ( SELECT DISTINCT customer_id_final, order_date, product_name_final, SELLING_PRICE , Min(order_date) OVER (partition BY customer_id_final) firstOrderdate FROM MM_TEST.NaveenSept7.sales_consolidated_MM_TEST )res WHERE order_date=firstorderdate; UPDATE MM_TEST.NaveenSept7.sales_consolidated_MM_TEST AS A SET A.acquisition_product=B.product_name_final FROM ( SELECT * FROM MM_TEST.NaveenSept7.temp_product_1 WHERE rowid=1 )B WHERE A.customer_id_final = B.customer_id_final;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from MM_TEST.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        