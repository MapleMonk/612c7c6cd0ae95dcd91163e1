{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table XYXX_DB.maplemonk.sales_consolidated_intermediate_XYXX as select CUSTOMER_ID ,SHOP_NAME ,FINAL_UTM_CHANNEL AS SOURCE ,ORDER_ID ,PHONE ,NAME ,EMAIL ,NULL AS SHIPPING_LAST_UPDATE_DATE ,SKU ,PRODUCT_ID ,PRODUCT_NAME ,CURRENCY ,CITY ,STATE AS State ,ORDER_STATUS ,ORDER_TIMESTAMP::date AS Order_Date ,SHIPPING_PRICE ,QUANTITY ,DISCOUNT_BEFORE_TAX AS DISCOUNT ,TAX ,TOTAL_SALES AS SELLING_PRICE ,NULL AS SHIPPINGPACKAGECODE ,NULL AS SHIPPINGPACKAGESTATUS ,LINE_ITEM_ID::varchar as SALEORDERITEMCODE ,LINE_ITEM_ID as SALES_ORDER_ITEM_ID ,NULL AS COURIER ,NULL AS SHIPPING_STATUS ,NULL AS DISPATCH_DATE ,NULL AS DELIVERED_STATUS ,IS_REFUND AS RETURN_FLAG ,case when is_refund = 1 then quantity::int end returned_quantity ,case when is_refund = 0 and lower(order_status) in (\'cancelled\') then quantity::int end cancelled_quantity ,NEW_CUSTOMER_FLAG ,ACQUISITION_PRODUCT ,NULL AS DAYS_IN_SHIPMENT ,NULL AS ACQUSITION_DATE ,SKU_CODE ,PRODUCT_NAME_FINAL ,PRODUCT_CATEGORY ,PRODUCT_SUPER_CATEGORY from XYXX_db.maplemonk.FACT_ITEMS_XYXX b union all select * from XYXX_DB.maplemonk.UNICOMMERCE_FACT_ITEMS_XYXX_FINAL where lower(marketplace) not like (\'%amazon%\') and lower(marketplace) not like (\'%shopify%\'); create or replace table XYXX_DB.maplemonk.Final_customerID as with new_phone_numbers as ( select phone, contact_num ,19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) as contact_num, phone from XYXX_DB.maplemonk.sales_consolidated_intermediate_XYXX ) a ), int as ( select contact_num,email,coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email,maple_monk_id,19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email,maple_monk_id from ( select phone as contact_num,email from XYXX_DB.maplemonk.sales_consolidated_intermediate_XYXX ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table XYXX_DB.maplemonk.sales_consolidated_XYXX as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as customer_id_final, min(order_date) over(partition by customer_id_final) as acquisition_date, m.* from (select c.maple_monk_id as maple_monk_id_phone, o.* from XYXX_DB.maplemonk.sales_consolidated_intermediate_XYXX o left join (select * from (select contact_num phone,maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from XYXX_DB.maplemonk.Final_customerID) where magic =1 )c on c.phone = right(regexp_replace(o.phone, \'[^a-zA-Z0-9]+\'),10))m left join (select distinct maple_monk_id, email from XYXX_DB.maplemonk.Final_customerID where contact_num is null )d on d.email = m.email ; ALTER TABLE XYXX_DB.maplemonk.sales_consolidated_XYXX drop COLUMN new_customer_flag ; ALTER TABLE XYXX_DB.maplemonk.sales_consolidated_XYXX ADD COLUMN new_customer_flag varchar(50); ALTER TABLE XYXX_DB.maplemonk.sales_consolidated_XYXX drop COLUMN acquisition_product ; ALTER TABLE XYXX_DB.maplemonk.sales_consolidated_XYXX ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE XYXX_DB.maplemonk.sales_consolidated_XYXX ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE XYXX_DB.maplemonk.sales_consolidated_XYXX ADD COLUMN acquisition_marketplace varchar(16777216); UPDATE XYXX_DB.maplemonk.sales_consolidated_XYXX AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Order_Date <> Min(Order_Date) OVER ( partition BY customer_id_final) THEN \'Repeat\' ELSE \'New\' END AS Flag FROM XYXX_DB.maplemonk.sales_consolidated_XYXX)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE XYXX_DB.maplemonk.sales_consolidated_XYXX SET new_customer_flag = CASE WHEN new_customer_flag IS NULL THEN \'New\' ELSE new_customer_flag END; CREATE OR replace temporary TABLE XYXX_DB.maplemonk.temp_source_1 AS SELECT DISTINCT customer_id_final, channel , marketplace FROM ( SELECT DISTINCT customer_id_final, order_date, source as channel, shop_name as marketplace, Min(order_date) OVER ( partition BY customer_id_final) firstOrderdate FROM XYXX_DB.maplemonk.sales_consolidated_XYXX ) res WHERE order_date=firstorderdate; UPDATE XYXX_DB.maplemonk.sales_consolidated_XYXX AS a SET a.acquisition_channel=b.channel FROM XYXX_DB.maplemonk.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; UPDATE XYXX_DB.maplemonk.sales_consolidated_XYXX AS a SET a.acquisition_marketplace=b.marketplace FROM XYXX_DB.maplemonk.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; CREATE OR replace temporary TABLE XYXX_DB.maplemonk.temp_product_1 AS SELECT DISTINCT customer_id_final, product_name_final, Row_number() OVER (partition BY customer_id_final ORDER BY SELLING_PRICE DESC) rowid FROM ( SELECT DISTINCT customer_id_final, order_date, product_name_final, SELLING_PRICE , Min(order_date) OVER (partition BY customer_id_final) firstOrderdate FROM XYXX_DB.maplemonk.sales_consolidated_XYXX )res WHERE order_date=firstorderdate; UPDATE XYXX_DB.maplemonk.sales_consolidated_XYXX AS A SET A.acquisition_product=B.product_name_final FROM ( SELECT * FROM XYXX_DB.maplemonk.temp_product_1 WHERE rowid=1)B WHERE A.customer_id_final = B.customer_id_final;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from XYXX_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        