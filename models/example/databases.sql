{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table LILGOODNESS_DB.maplemonk.sales_consolidated_intermediate_LG as with a as ( select order_date, sku, max(suborder_mrp) as mrp from LILGOODNESS_DB.maplemonk.fact_items_easy_ecom_lg group by 1,2 ) select CUSTOMER_ID, \'Lilgoodness\' as SHOP_NAME, NULL as carrier_id, NULL as courier, b.name as Name, b.email as email, b.PHONE as PHONE, case when shop_name = \'Amazon\' then \'Amazon.in\' else \'Shopify\' end as MARKETPLACE, NULL as MARKETPLACE_ID, ORDER_ID, ORDER_ID as Invoice_id, NUll as shipping_last_update_date, order_status as shipping_status, b.SKU, NULL as sku_type, PRODUCT_ID, PRODUCT_NAME as PRODUCTNAME, CURRENCY, IS_REFUND as Return_Flag, CITY::varchar City, STATE:: varchar State, order_status, ORDER_TIMESTAMP::DATE as ORDER_Date, shipping_price::float as SHIPPING_PRICE, quantity::int suborder_quantity, quantity::int shipped_quantity, case when is_refund = 1 then quantity::int end returned_quantity, case when is_refund = 0 and lower(order_status) in (\'cancelled\') then quantity::int end cancelled_quantity, case when is_refund = 1 then line_item_sales end as return_sales, case when is_refund = 0 and lower(order_status) in (\'cancelled\') then line_item_sales end as cancel_sales, TAX::float Tax, a.mrp as suborder_mrp, b.category, discount::float as discount, case when line_item_sales::float is null then 0 else (case when lower(shop_name) = \'amazon\' then line_item_sales::float else line_item_sales::float - ifnull(discount::float,0) end) end selling_price, coalesce(a.mrp,0)*suborder_quantity as mrp_sales, case when (mrp_sales is null or mrp_sales<selling_price) then discount else mrp_sales-selling_price end Discount_MRP, case when new_customer_flag = \'New\' then 1 else 0 end as new_customer_flag, NULL as Days_in_Shipment, FINAL_UTM_CHANNEL as Channel from lilgoodness_db.maplemonk.FACT_ITEMS_LG b left join a on a.order_date = b.order_timestamp::date and a.sku= b.sku union all select NULL as customer_id, * from Lilgoodness_DB.maplemonk.fact_items_easy_ecom_lg where lower(marketplace) not in (\'shopify\'); create or replace table lilgoodness_db.maplemonk.Final_customerID as with new_phone_numbers as ( select phone, contact_num ,19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) as contact_num, phone from lilgoodness_db.maplemonk.SALES_CONSOLIDATED_INTERMEDIATE_LG ) a ), int as ( select contact_num,email,coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email,maple_monk_id,19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email,maple_monk_id from ( select phone as contact_num,email from lilgoodness_db.maplemonk.SALES_CONSOLIDATED_INTERMEDIATE_LG ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table lilgoodness_db.maplemonk.SALES_CONSOLIDATED_LG as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as customer_id_final, min(order_date) over(partition by customer_id_final) as acquisition_date, m.* from (select c.maple_monk_id as maple_monk_id_phone, o.* from lilgoodness_db.maplemonk.SALES_CONSOLIDATED_INTERMEDIATE_LG o left join (select * from (select contact_num phone,maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from lilgoodness_db.maplemonk.Final_customerID) where magic =1 )c on c.phone = right(regexp_replace(o.phone, \'[^a-zA-Z0-9]+\'),10))m left join (select distinct maple_monk_id, email from lilgoodness_db.maplemonk.Final_customerID where contact_num is null )d on d.email = m.email ; ALTER TABLE lilgoodness_db.maplemonk.SALES_CONSOLIDATED_LG drop COLUMN new_customer_flag ; ALTER TABLE lilgoodness_db.maplemonk.SALES_CONSOLIDATED_LG ADD COLUMN new_customer_flag varchar(50); ALTER TABLE lilgoodness_db.maplemonk.SALES_CONSOLIDATED_LG ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE lilgoodness_db.maplemonk.SALES_CONSOLIDATED_LG ADD COLUMN acquisition_channel varchar(16777216); UPDATE lilgoodness_db.maplemonk.SALES_CONSOLIDATED_LG AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Order_Date <> Min(Order_Date) OVER ( partition BY customer_id_final) THEN \'Repeat\' ELSE \'New\' END AS Flag FROM lilgoodness_db.maplemonk.SALES_CONSOLIDATED_LG)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE lilgoodness_db.maplemonk.SALES_CONSOLIDATED_LG SET new_customer_flag = CASE WHEN new_customer_flag IS NULL THEN \'New\' ELSE new_customer_flag END; CREATE OR replace temporary TABLE lilgoodness_db.maplemonk.temp_source_1 AS SELECT DISTINCT customer_id_final, shop_name FROM ( SELECT DISTINCT customer_id_final, order_date, shop_name, Min(order_date) OVER ( partition BY customer_id_final) firstOrderdate FROM lilgoodness_db.maplemonk.SALES_CONSOLIDATED_LG ) res WHERE order_date=firstorderdate; UPDATE lilgoodness_db.maplemonk.SALES_CONSOLIDATED_LG AS a SET a.acquisition_channel=b.shop_name FROM lilgoodness_db.maplemonk.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; CREATE OR replace temporary TABLE lilgoodness_db.maplemonk.temp_product_1 AS SELECT DISTINCT customer_id_final, productname, Row_number() OVER (partition BY customer_id_final ORDER BY SELLING_PRICE DESC) rowid FROM ( SELECT DISTINCT customer_id_final, order_date, productname, SELLING_PRICE , Min(order_date) OVER (partition BY customer_id_final) firstOrderdate FROM lilgoodness_db.maplemonk.SALES_CONSOLIDATED_LG )res WHERE order_date=firstorderdate; UPDATE lilgoodness_db.maplemonk.SALES_CONSOLIDATED_LG AS A SET A.acquisition_product=B.productname FROM ( SELECT * FROM lilgoodness_db.maplemonk.temp_product_1 WHERE rowid=1)B WHERE A.customer_id_final = B.customer_id_final;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from LILGOODNESS_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        