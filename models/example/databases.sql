{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table pomme_DB.maplemonk.sales_consolidated_intermediate_pomme as select b.SHOP_NAME, NULL as carrier_id, NULL as courier, coalesce(b.name ,null) as Customer_Name, coalesce(b.email ,null) as email, coalesce(b.phone ,null) as phone, b.SHOP_NAME as MARKETPLACE, NULL as MARKETPLACE_ID, b.ORDER_ID, line_item_id::varchar as Line_Item_ID, order_name as Reference_Code, NULL as manifest_date, NULL as shipping_last_update_date, NULL as shipping_status, coalesce(b.sku ,null) as sku, NULL as sku_type, b.PRODUCT_ID, b.PRODUCT_NAME as PRODUCTNAME, b.grip, b.size, b.color, concat(b.product_short_name1,\' \',b.product_short_name2) product_short_name, case when left(productname, position(\' \',productname,1)-1) in (\'Club\',\'Saddle\',\'Socks\') then \'Club\' else replace(left(productname, position(\' \',productname,1)-1),\',\',\'\') end as model, b.print, b.category1, b.category2, b.CURRENCY, upper(b.CITY::varchar) City, upper(b.STATE:: varchar) State, upper(b.country::varchar) Country, coalesce(b.order_status,null) as order_status, coalesce(b.order_status,null) as final_status, b.ORDER_TIMESTAMP::datetime as ORDER_Date, b.shipping_price::float as SHIPPING_PRICE, b.quantity::int suborder_quantity, case when lower(order_status) = \'refunded\' then 1 else 0 end as return_flag, b.quantity::int shipped_quantity, b.refunded_quantity refunded_quantity, case when lower(b.order_status) in (\'cancelled\') then quantity::int end cancelled_quantity, b.refund_amount as refunded_amount, case when lower(b.order_status) in (\'cancelled\') then total_sales end as cancel_sales, coalesce(b.TAX::float,0) Tax, b.discount::float as discount, case when b.total_sales::float is null then 0 else b.total_sales::float end selling_price, case when b.new_customer_flag = \'New\' then 1 else 0 end as new_customer_flag, case when b.new_customer_flag_month = \'New\' then 1 else 0 end as new_customer_flag_month, null as Warehouse_Name, null as Days_in_Shipment, payment_method as payment_mode, null as import_date, null as last_update_date, null as invoice_date, null as company_name from pomme_db.maplemonk.FACT_ITEMS_woocommerce_pomme b; create or replace table pomme_DB.maplemonk.Final_customerID as with new_phone_numbers as ( select phone, contact_num ,19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct replace(replace(phone,\' \',\'\'),\'-\',\'\') as contact_num, phone from pomme_DB.maplemonk.SALES_CONSOLIDATED_INTERMEDIATE_pomme ) a ), int as ( select contact_num,email,coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email,maple_monk_id,19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num, e.contact_num) as contact_num, e.email,maple_monk_id from ( select replace(replace(phone,\' \',\'\'),\'-\',\'\') as contact_num,email from pomme_DB.maplemonk.SALES_CONSOLIDATED_INTERMEDIATE_pomme ) e left join new_phone_numbers p on p.contact_num = e.contact_num ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table pomme_DB.maplemonk.SALES_CONSOLIDATED_pomme as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as customer_id_final, min(order_date) over(partition by customer_id_final) as acquisition_date, m.* from (select c.maple_monk_id as maple_monk_id_phone, o.* from pomme_DB.maplemonk.SALES_CONSOLIDATED_INTERMEDIATE_pomme o left join (select * from (select contact_num phone,maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from pomme_DB.maplemonk.Final_customerID) where magic =1 )c on c.phone = replace(replace(o.phone,\' \',\'\'),\'-\',\'\'))m left join (select distinct maple_monk_id, email from pomme_DB.maplemonk.Final_customerID where contact_num is null )d on d.email = m.email ; ALTER TABLE pomme_db.maplemonk.SALES_CONSOLIDATED_pomme drop COLUMN new_customer_flag ; ALTER TABLE pomme_db.maplemonk.SALES_CONSOLIDATED_pomme ADD COLUMN new_customer_flag varchar(16777216); ALTER TABLE pomme_db.maplemonk.SALES_CONSOLIDATED_pomme drop COLUMN new_customer_flag_month ; ALTER TABLE pomme_db.maplemonk.SALES_CONSOLIDATED_pomme ADD COLUMN new_customer_flag_month varchar(16777216); ALTER TABLE pomme_db.maplemonk.SALES_CONSOLIDATED_pomme ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE pomme_db.maplemonk.SALES_CONSOLIDATED_pomme ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE pomme_db.maplemonk.SALES_CONSOLIDATED_pomme drop COLUMN ACQUISITION_DATE ; ALTER TABLE pomme_db.maplemonk.SALES_CONSOLIDATED_pomme ADD COLUMN ACQUISITION_DATE timestamp; ALTER TABLE pomme_db.maplemonk.SALES_CONSOLIDATED_pomme ADD COLUMN SAME_DAY_ORDERNO number; UPDATE pomme_db.maplemonk.SALES_CONSOLIDATED_pomme AS A SET A.SAME_DAY_ORDERNO = B.rw FROM ( select distinct customer_id_final ,order_id ,rank() over (partition by customer_id_final, order_date order by order_date, order_id) as rw from pomme_DB.maplemonk.SALES_CONSOLIDATED_pomme ) AS B Where A.order_id = B.order_id; UPDATE pomme_DB.maplemonk.SALES_CONSOLIDATED_pomme AS A SET A.ACQUISITION_DATE = B.ACQUISITION_DATE FROM ( select distinct customer_id_final , min(order_Date) OVER ( partition BY customer_id_final) ACQUISITION_DATE from pomme_DB.maplemonk.SALES_CONSOLIDATED_pomme B where lower(order_status) not in (\'cancelled\',\'refunded\') ) AS B where A.customer_id_final = B.customer_id_final; UPDATE pomme_DB.maplemonk.SALES_CONSOLIDATED_pomme AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Order_Date = ACQUISITION_DATE and lower(order_status) not in (\'cancelled\',\'refunded\') then \'New\' WHEN Order_Date < ACQUISITION_DATE THEN \'Yet to make completed order\' ELSE \'Repeat\' END AS Flag FROM pomme_DB.maplemonk.SALES_CONSOLIDATED_pomme)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final AND A.order_date::date=B.Order_date::Date; UPDATE pomme_db.maplemonk.SALES_CONSOLIDATED_pomme SET new_customer_flag = CASE WHEN new_customer_flag IS NULL and lower(order_status) not in (\'cancelled\',\'refunded\') THEN \'New\' WHEN new_customer_flag IS NULL and lower(order_status) in (\'cancelled\',\'refunded\') THEN \'Yet to make completed order\' ELSE new_customer_flag END; UPDATE pomme_db.maplemonk.SALES_CONSOLIDATED_pomme AS A SET A.new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Last_day(order_date, \'month\') = Last_day(acquisition_date, \'month\') THEN \'New\' WHEN Last_day(order_date, \'month\') < Last_day(acquisition_date, \'month\') THEN \'Yet to make completed order\' ELSE \'Repeat\' END AS Flag FROM pomme_db.maplemonk.SALES_CONSOLIDATED_pomme)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE pomme_DB.maplemonk.SALES_CONSOLIDATED_pomme SET new_customer_flag_month = CASE WHEN new_customer_flag_month IS NULL and lower(order_status) not in (\'cancelled\',\'returned\') THEN \'New\' ELSE new_customer_flag_month END; CREATE OR replace temporary TABLE pomme_DB.maplemonk.temp_product_1 AS SELECT DISTINCT customer_id_final, productname, Row_number() OVER (partition BY customer_id_final ORDER BY SELLING_PRICE DESC) rowid FROM ( SELECT DISTINCT customer_id_final, order_date, productname, SELLING_PRICE , Min(order_date) OVER (partition BY customer_id_final) firstOrderdate FROM pomme_DB.maplemonk.SALES_CONSOLIDATED_pomme where lower(order_status) not in (\'cancelled\',\'refunded\'))res WHERE order_date=firstorderdate; UPDATE pomme_DB.maplemonk.SALES_CONSOLIDATED_pomme AS A SET A.acquisition_product=B.productname FROM ( SELECT * FROM pomme_DB.maplemonk.temp_product_1 WHERE rowid=1)B WHERE A.customer_id_final = B.customer_id_final;",
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
                        