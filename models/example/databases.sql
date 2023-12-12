{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table elcinco_db.MAPLEMONK.elcinco_db_sales_consolidated_intermediate_pre as select Null as customer_id ,upper(marketplace) shop_name ,upper(marketplace) marketplace ,upper(marketplace) AS CHANNEL ,upper(marketplace) AS SOURCE ,ORDER_ID::varchar order_id ,reference_code::varchar reference_code ,phone::varchar as PHONE ,name::varchar as NAME ,email::varchar as EMAIL ,shipping_last_update_date AS SHIPPING_LAST_UPDATE_DATE ,SKU::varchar sku ,upper(b.BRAND_FINAL::varchar) brand ,b.PRODUCT_ID::Varchar PRODUCT_ID ,PRODUCT_NAME AS PRODUCT_NAME ,CURRENCY ,upper(CITY) as city ,upper(STATE) AS State ,upper(ORDER_STATUS) order_status ,ORDER_DATE::date AS Order_Date ,SUBORDER_QUANTITY AS QUANTITY ,ifnull(SELLING_PRICE,0) - ifnull(tax,0) gross_sales_before_tax ,DISCOUNT AS DISCOUNT ,TAX ,SHIPPING_PRICE ,SELLING_PRICE AS SELLING_PRICE ,upper(ORDER_STATUS) as OMS_ORDER_STATUS ,upper(b.shipping_status) AS SHIPPING_STATUS ,upper(coalesce(shipmap.final_shipping_status,b.shipping_status)) FINAL_SHIPPING_STATUS ,saleOrderItemCode::varchar as SALEORDERITEMCODE ,SALES_ORDER_ITEM_ID::varchar as SALES_ORDER_ITEM_ID ,AWB ,null as payment_gateway ,payment_mode ,COURIER ,DISPATCH_DATE AS DISPATCH_DATE ,delivered_date as delivered_date ,case when upper(FINAL_SHIPPING_STATUS) in (\'DELIVERED\') then 1 end AS DELIVERED_STATUS ,return_flag AS RETURN_FLAG ,case when return_flag = 1 then suborder_quantity::int end returned_quantity ,case when return_flag = 1 then selling_price::float end returned_sales ,case when return_flag = 0 and lower(order_status) in (\'cancelled\') then suborder_quantity::int end cancelled_quantity ,new_customer_flag::varchar as NEW_CUSTOMER_FLAG ,NULL as ACQUISITION_PRODUCT ,case when order_status=\'COMPLETE\' then delivered_date::date-order_date::date else current_date - order_date::Date end as days_in_shipment ,NULL AS ACQUSITION_DATE ,sku_code::varchar sku_code ,upper(b.product_name_final) PRODUCT_NAME_FINAL ,upper(b.Product_Category) PRODUCT_CATEGORY ,upper(b.product_sub_category) PRODUCT_SUB_CATEGORY ,upper(warehouse_name::varchar) warehouse from elcinco_db.MapleMonk.elcinco_db_unicommerce_fact_items b left join ( select * from ( select upper(Shipping_status) shipping_status ,upper(mapped_status) final_shipping_status ,row_number() over (partition by lower(shipping_Status) order by 1) rw from elcinco_db.maplemonk.shipment_status_mapping ) where rw = 1 ) ShipMap on lower(coalesce(b.shipping_status,b.order_status)) = lower(ShipMap.shipping_status) union all Select NULL as Customer_ID ,upper(MSF.marketplace) shop_name ,upper(MSF.marketplace) marketplace ,upper(MSF.marketplace) Channel ,upper(MSF.marketplace) Sourcce ,MSF.STORE_ORDER_ID::varchar ORDER_ID ,MSF.ORDER_ID_FK::varchar ORDER_NAME ,NULL as PHONE ,NULL as NAME ,NULL as EMAIL ,coalesce(MSF.DELIVERED_ON,MSF.RETURN_CREATION_DATE,MSF.RTO_CREATION_DATE,MSF.CANCELLED_ON,MSF.SHIPPED_ON,MSF.PACKED_ON,MSF.LOST_DATE) SHIPPING_LAST_UPDATE_DATE ,MSF.myntra_sku_code::varchar myntra_sku_code ,MSF.BRAND_FINAL::varchar BRAND ,MSF.SKU_ID::varchar SKU_ID ,upper(MSF.STYLE_NAME::varchar) PRODUCT_NAME ,\'INR\' as Currency ,upper(MSF.CITY) City ,upper(MSF.STATE) State ,upper(MSF.ORDER_STATUS) Order_status ,MSF.created_on ,1 as quantity ,MSF.final_amount::float final_amount ,MSF.DISCOUNT::float discount ,MSF.TAX_RECOVERY::float TAX_RECOVERY ,MSF.SHIPPING_CHARGE::float SHIPPING_CHARGE ,MSF.final_amount::float final_amount ,NULL AS OMS_ORDER_STATUS ,upper(MSF.ORDER_STATUS) Shipping_status ,upper(MSF.ORDER_STATUS) Final_Shipping_status ,MSF.ORDER_LINE_ID::varchar ,MSF.ORDER_LINE_ID::varchar ,MSF.ORDER_TRACKING_NUMBER::varchar ,\'Myntra SJIT\' Payment_Gateway ,\'Myntra SJIT\' Payment_Mode ,\'Myntra\' Courier ,MSF.shipped_on ,MSF.DELIVERED_ON ,case when MSF.delivered_on is not null then 1 else 0 end AS DELIVERED_STATUS ,case when coalesce(MSF.RETURN_CREATION_DATE,MSF.RTO_CREATION_DATE) is null then 0 else 1 end AS RETURN_FLAG ,case when coalesce(MSF.RETURN_CREATION_DATE,MSF.RTO_CREATION_DATE) is null then 0 else 1 end returned_sales ,case when coalesce(MSF.RETURN_CREATION_DATE,MSF.RTO_CREATION_DATE) is null then 0 else ifnull(MSF.final_amount,0) end returned_sales ,case when MSF.cancelled_on is null then 0 else 1 end cancelled_quantity ,NULL as NEW_CUSTOMER_FLAG ,NULL as ACQUISTION_PRODUCT ,case when MSF.delivered_on is not null then datediff(day,date(MSF.created_on),date(MSF.delivered_on)) when coalesce(MSF.delivered_on,MSF.return_creation_date,MSF.cancelled_on,MSF.lost_date,MSF.RTO_CREATION_DATE) is null then datediff(day,date(MSF.created_on), getdate()) end::int as Days_in_Shipment ,NULL as ACQUISTION_DATE ,MSF.Myntra_SKU_code::varchar SKU_ID ,upper(MSF.STYLE_NAME::varchar) PRODUCT_NAME_FINAL ,Upper(product_category) PRODUCT_CATEGORY ,Upper(product_sub_category) PRODUCT_SUB_CATEGORY ,SELLER_WAREHOUSE_ID::Varchar from Elcinco_db.maplemonk.Elcinco_Myntra_SJIT_Fact_Items MSF ; create or replace table Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated_intermediate as with SKU_Fresh_Flag as ( select SKU ,min(order_date) First_Sale_Date ,datediff(day, First_Sale_Date, current_date()) Days_Since_First_Sale ,case when Days_Since_First_Sale < 120 then \'Fresh SKU\' else \'Old SKU\' end SKU_Fresh_Flag from Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated_intermediate_pre group by SKU) select SCIP.* ,SFF.SKU_FRESH_FLAG ,SFF.First_Sale_Date SKU_FIRST_SALE_DATE from Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated_intermediate_pre SCIP left join SKU_Fresh_Flag SFF on SCIP.SKU = SFF.SKU ; create or replace table Elcinco_db.MAPLEMONK.Final_customerID as with new_phone_numbers as ( select phone, contact_num, 19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) as contact_num, phone from Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated_intermediate ) a ), int as ( select contact_num, email, coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email, maple_monk_id, 19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email, maple_monk_id from ( select phone as contact_num, email from Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated_intermediate ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as customer_id_final, min(order_date) over(partition by customer_id_final) as acquisition_date, min(case when lower(order_status) not in (\'cancelled\') then order_date end) over(partition by customer_id_final) as first_complete_order_date, m.*, n.mrp*m.quantity mrp_sales, n.gender, n.brand brand_mapped, n.\"Style Code (Without Size)\" style from ( select c.maple_monk_id as maple_monk_id_phone, o.* from Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated_intermediate o left join ( select * from ( select contact_num phone, maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from Elcinco_db.MAPLEMONK.Final_customerID ) where magic =1 )c on c.phone = right(regexp_replace(o.phone, \'[^a-zA-Z0-9]+\'),10) )m left join ( select distinct maple_monk_id, email from Elcinco_db.MAPLEMONK.Final_customerID where contact_num is null )d on d.email = m.email left join elcinco_db.maplemonk.sku_mapping_mrp n on m.sku = n.\"Sku Code\" ; ALTER TABLE Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated drop COLUMN new_customer_flag ; ALTER TABLE Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated ADD COLUMN new_customer_flag varchar(50); ALTER TABLE Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated ADD COLUMN new_customer_flag_month varchar(50); ALTER TABLE Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated drop COLUMN acquisition_product ; ALTER TABLE Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated ADD COLUMN acquisition_marketplace varchar(16777216); UPDATE Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Order_Date = first_complete_order_date then \'New\' WHEN Order_Date < first_complete_order_date or first_complete_order_date is null THEN \'Yet to make completed order\' WHEN Order_Date > first_complete_order_date then \'Repeat\' END AS Flag FROM Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated SET new_customer_flag = CASE WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) in (\'cancelled\') end) THEN \'Yet to make completed order\' ELSE new_customer_flag END; UPDATE Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated AS A SET A.new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Last_day(order_date, \'month\') = Last_day(first_complete_order_date, \'month\') THEN \'New\' WHEN Last_day(order_date, \'month\') < Last_day(first_complete_order_date, \'month\') or acquisition_date is null THEN \'Yet to make completed order\' WHEN Last_day(order_date, \'month\') > Last_day(first_complete_order_date, \'month\') THEN \'Repeat\' END AS Flag FROM Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated SET new_customer_flag_month = CASE WHEN new_customer_flag_month IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' ELSE new_customer_flag_month END; CREATE OR replace temporary TABLE Elcinco_db.MAPLEMONK.temp_source_1 AS SELECT DISTINCT customer_id_final, channel, marketplace FROM ( SELECT DISTINCT customer_id_final, order_date, source as channel, shop_name as marketplace, Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate FROM Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated ) res WHERE order_date=firstorderdate; UPDATE Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated AS a SET a.acquisition_channel=b.channel FROM Elcinco_db.MAPLEMONK.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; UPDATE Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated AS a SET a.acquisition_marketplace=b.marketplace FROM Elcinco_db.MAPLEMONK.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; CREATE OR replace temporary TABLE elcinco_db.MAPLEMONK.temp_product_1 AS SELECT DISTINCT customer_id_final, product_name_final, Row_number() OVER (partition BY customer_id_final ORDER BY SELLING_PRICE DESC) rowid FROM ( SELECT DISTINCT customer_id_final, order_date, product_name_final, SELLING_PRICE , Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate FROM Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated )res WHERE order_date=firstorderdate; UPDATE Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated AS A SET A.acquisition_product=B.product_name_final FROM ( SELECT * FROM Elcinco_db.MAPLEMONK.temp_product_1 WHERE rowid=1 )B WHERE A.customer_id_final = B.customer_id_final; create or replace table Elcinco_db.MAPLEMONK.Elcinco_db_unicommerce_returns_detailed as select a.order_date ,a.reference_code ,b.channel marketing_channel ,case when lower(a.marketplace) like \'%shopify%\' then \'Shopify_rubansaccessories\' when lower(a.marketplace) like \'%amazon%\' then \'Amazon\' else lower(a.marketplace) end as marketplace ,sum(a.return_quantity) RETURN_QUANTITY ,sum(a.return_sales) RETURN_sales ,sum(case when return_flag = 1 then TAX end) TOTAL_RETURN_TAX ,sum(return_sales) - TOTAL_RETURN_TAX as TOTAL_RETURN_AMOUNT_EXCL_TAX from Elcinco_db.Maplemonk.Elcinco_db_unicommerce_fact_items a left join (select distinct reference_code, channel from Elcinco_db.Maplemonk.Elcinco_db_sales_consolidated) b on lower(replace(a.reference_code,\'#\',\'\')) = lower(replace(b.reference_code,\'#\',\'\')) group by 1,2,3,4; create or replace table Elcinco_db.MAPLEMONK.Elcinco_db_RETURNS_CONSOLIDATED as select upper(marketplace) Marketplace ,upper(marketing_channel) marketing_channel ,order_date ,sum(RETURN_QUANTITY) TOTAL_RETURNED_QUANTITY ,sum(RETURN_sales) TOTAL_RETURN_AMOUNT ,sum(TOTAL_RETURN_TAX) TOTAL_RETURN_TAX ,TOTAL_RETURN_AMOUNT - sum(TOTAL_RETURN_TAX) as TOTAL_RETURN_AMOUNT_EXCL_TAX from Elcinco_db.Maplemonk.Elcinco_db_unicommerce_returns_detailed group by 1,2,3 ; select product_category from Elcinco_db.MAPLEMONK.Elcinco_db_sales_consolidated where producT_name_final = \'LNDMK245WWHT\'",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from elcinco_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        