{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table bbk_db.maplemonk.fact_items_intermediate_bbk as select a.unitid ,d.city outletcity ,d.outlet outletname ,concat(a.invoiceid,\'-\',a.invoicenumber,\'-\',a.unitid,\'-\',a.saledate) order_id ,concat(a.invoiceid,\'-\',a.invoicenumber,\'-\',a.unitid,\'-\',a.saledate,\'-\',itemid) order_item_id ,a.discamt/count(1) over (partition by a.unitid, a.invoiceid, a.invoicenumber, a.saledate) order_discount ,a.discountpercent ,a.paymode ,a.saledate::date as saledate ,monthname(a.saledate::date) as month ,year(a.saledate::date) as year ,a.saletime ,hour(try_cast(a.SALETIME as Datetime)) HourOfSale ,a.totalamt/count(1) over (partition by a.unitid, a.invoiceid, a.invoicenumber, a.saledate) order_amt ,case when order_amt <= 300 then \'<300\' when order_amt <= 500 and order_amt > 300 then \'301-500\' when order_amt <= 800 and order_amt > 500 then \'501-800\' when order_amt <= 1000 and order_amt > 800 then \'801-1000\' when order_amt > 1000 then \'>1000\' end as sales_buckets ,a.invoiceid ,a.invoicenumber ,a.ordertype ,case when a.ordersource in (\'zomato\',\'Zomato6\', \'ZOMATO \') then \'Zomato\' when a.ordersource = \'Foodpanda\' then \'FoodPanda\' when a.ordersource = \'magicpin\' then \'Magicpin\' when a.ordersource = \'dotpe\' then \'Dotpe\' when a.ordersource in (\'swiggy\',\'SWIGGY\') then \'Swiggy\' else a.ordersource end as ordersource ,a.discounttype ,b.price item_price ,b.itemid ,b.itemname ,b.groupname item_category ,b.itemtype ,b.quantity ,c.guestid ,c.email ,replace(c.guestphone,\',\',\'\') guest_phone ,c.customername ,c.gender ,c.locality ,c.address ,c.address1 ,c.dispatchtime ,c.orderstatus from bbk_db.maplemonk.rezol_order_details_auditable a left join bbk_db.maplemonk.rezol_item_details_auditable b on a.unitid = b.unitid and a.invoiceid = b.invoiceid and a.invoicenumber = b.invoicenumber and a.saledate = b.saledate left join bbk_db.maplemonk.rezol_guest_details_auditable c on a.unitid = c.unitid and a.invoiceid = c.invoiceid and a.invoicenumber = c.invoicenumber and a.saledate = b.saledate left join bbk_db.maplemonk.unit_city d on a.unitid = d.unitid union all select a.unitid ,d.city outletcity ,d.outlet outletname ,concat(a.invoiceid,\'-\',a.invoicenumber,\'-\',a.unitid,\'-\',a.saledate) order_id ,concat(a.invoiceid,\'-\',a.invoicenumber,\'-\',a.unitid,\'-\',a.saledate,\'-\',itemid) order_item_id ,a.discamt/count(1) over (partition by a.unitid, a.invoiceid, a.invoicenumber, a.saledate) order_discount ,a.discountpercent ,a.paymode ,a.saledate::date as saledate ,monthname(a.saledate::date) as month ,year(a.saledate::date) as year ,a.saletime ,hour(try_cast(a.SALETIME as Datetime)) HourOfSale ,a.totalamt/count(1) over (partition by a.unitid, a.invoiceid, a.invoicenumber, a.saledate) order_amt ,case when order_amt <= 300 then \'<300\' when order_amt <= 500 and order_amt > 300 then \'301-500\' when order_amt <= 800 and order_amt > 500 then \'501-800\' when order_amt <= 1000 and order_amt > 800 then \'801-1000\' when order_amt > 1000 then \'>1000\' end as sales_buckets ,a.invoiceid ,a.invoicenumber ,a.ordertype ,case when a.ordersource in (\'zomato\',\'Zomato6\', \'ZOMATO \') then \'Zomato\' when a.ordersource = \'Foodpanda\' then \'FoodPanda\' when a.ordersource = \'magicpin\' then \'Magicpin\' when a.ordersource = \'dotpe\' then \'Dotpe\' when a.ordersource in (\'swiggy\',\'SWIGGY\') then \'Swiggy\' else a.ordersource end as ordersource ,a.discounttype ,b.price item_price ,b.itemid ,b.itemname ,b.groupname item_category ,b.itemtype ,b.quantity ,c.guestid ,c.email ,replace(c.guestphone,\',\',\'\') guest_phone ,c.customername ,c.gender ,c.locality ,c.address ,c.address1 ,c.dispatchtime ,c.orderstatus from bbk_db.maplemonk.rezol_order_details a left join bbk_db.maplemonk.rezol_item_details b on a.unitid = b.unitid and a.invoiceid = b.invoiceid and a.invoicenumber = b.invoicenumber and a.saledate = b.saledate left join bbk_db.maplemonk.rezol_guest_details c on a.unitid = c.unitid and a.invoiceid = c.invoiceid and a.invoicenumber = c.invoicenumber and a.saledate = b.saledate left join bbk_db.maplemonk.unit_city d on a.unitid = d.unitid where a.saledate > (select max(saledate) from bbk_db.maplemonk.rezol_order_details_auditable) ; create or replace table bbk_DB.maplemonk.Final_customerID as with new_phone_numbers as ( select phone, contact_num ,19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(replace(replace(guest_phone,\' \',\'\'),\'-\',\'\'),10) as contact_num, guest_phone as phone from bbk_DB.maplemonk.fact_items_intermediate_bbk ) a ), int as ( select contact_num,email,coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email,maple_monk_id,19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num, e.contact_num) as contact_num, e.email,maple_monk_id from ( select right(replace(replace(guest_phone,\' \',\'\'),\'-\',\'\'),10) as contact_num,email from bbk_DB.maplemonk.fact_items_intermediate_bbk ) e left join new_phone_numbers p on p.contact_num = e.contact_num ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table bbk_DB.maplemonk.fact_items_bbk as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as customer_id_final, min(saledate) over(partition by customer_id_final) as acquisition_date, m.* from (select c.maple_monk_id as maple_monk_id_phone, o.* from bbk_DB.maplemonk.fact_items_INTERMEDIATE_bbk o left join (select * from (select contact_num phone,maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from bbk_DB.maplemonk.Final_customerID) where magic =1 )c on c.phone = right(replace(replace(o.guest_phone,\' \',\'\'),\'-\',\'\'),10))m left join (select distinct maple_monk_id, email from bbk_DB.maplemonk.Final_customerID where contact_num is null )d on d.email = m.email; ALTER TABLE BBK_DB.maplemonk.FACT_ITEMS_BBK ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE BBK_DB.maplemonk.FACT_ITEMS_BBK ADD COLUMN acquisition_outlet varchar(16777216); ALTER TABLE BBK_DB.maplemonk.FACT_ITEMS_BBK ADD COLUMN acquisition_source varchar(16777216); ALTER TABLE BBK_DB.maplemonk.FACT_ITEMS_BBK ADD COLUMN acquisition_ordertype varchar(16777216); CREATE OR replace temporary TABLE bbk_DB.maplemonk.temp_product_1 AS SELECT DISTINCT customer_id_final, itemname, Row_number() OVER (partition BY customer_id_final ORDER BY ITEM_PRICE DESC) rowid FROM ( SELECT DISTINCT customer_id_final, saledate, itemname, ITEM_PRICE , Min(saledate) OVER (partition BY customer_id_final) firstOrderdate FROM bbk_DB.maplemonk.fact_items_bbk )res WHERE saledate=firstorderdate; UPDATE BBK_DB.maplemonk.fact_items_bbk AS A SET A.acquisition_product=B.itemname FROM ( SELECT * FROM bbk_DB.maplemonk.temp_product_1 WHERE rowid=1)B WHERE A.customer_id_final = b.customer_id_final; CREATE OR replace temporary TABLE bbk_DB.maplemonk.temp_outlet_1 AS SELECT DISTINCT customer_id_final, outletname, Row_number() OVER (partition BY customer_id_final ORDER BY saletime) rowid FROM ( SELECT DISTINCT customer_id_final, saledate, saletime, outletname, Min(saledate) OVER (partition BY customer_id_final) firstOrderdate FROM bbk_DB.maplemonk.fact_items_bbk )res WHERE saledate=firstorderdate; UPDATE BBK_DB.maplemonk.fact_items_bbk AS A SET A.acquisition_outlet=B.outletname FROM ( SELECT * FROM bbk_DB.maplemonk.temp_outlet_1 WHERE rowid=1)B WHERE A.customer_id_final = b.customer_id_final; CREATE OR replace temporary TABLE bbk_DB.maplemonk.temp_source_1 AS SELECT DISTINCT customer_id_final, ordersource, Row_number() OVER (partition BY customer_id_final ORDER BY saletime) rowid FROM ( SELECT DISTINCT customer_id_final, saledate, saletime, ordersource, Min(saledate) OVER (partition BY customer_id_final) firstOrderdate FROM bbk_DB.maplemonk.fact_items_bbk )res WHERE saledate=firstorderdate; UPDATE BBK_DB.maplemonk.fact_items_bbk AS A SET A.acquisition_source=B.ordersource FROM ( SELECT * FROM bbk_DB.maplemonk.temp_source_1 WHERE rowid=1)B WHERE A.customer_id_final = b.customer_id_final; CREATE OR replace temporary TABLE bbk_DB.maplemonk.temp_ordertype_1 AS SELECT DISTINCT customer_id_final, ordertype, Row_number() OVER (partition BY customer_id_final ORDER BY saletime) rowid FROM ( SELECT DISTINCT customer_id_final, saledate, saletime, ordertype, Min(saledate) OVER (partition BY customer_id_final) firstOrderdate FROM bbk_DB.maplemonk.fact_items_bbk )res WHERE saledate=firstorderdate; UPDATE BBK_DB.maplemonk.fact_items_bbk AS A SET A.acquisition_ordertype=B.ordertype FROM ( SELECT * FROM bbk_DB.maplemonk.temp_ordertype_1 WHERE rowid=1)B WHERE A.customer_id_final = b.customer_id_final; ALTER TABLE bbk_DB.maplemonk.FACT_ITEMS_bbk ADD COLUMN customer_flag varchar(50); ALTER TABLE bbk_db.maplemonk.FACT_ITEMS_bbk ADD COLUMN customer_flag_month varchar(50); UPDATE bbk_db.maplemonk.FACT_ITEMS_BBK AS A SET A.ACQUISITION_DATE = B.ACQUISITION_DATE FROM ( select distinct customer_id_final , min(saledate) OVER ( partition BY customer_id_final) ACQUISITION_DATE from bbk_db.maplemonk.FACT_ITEMS_bbk where lower(orderstatus) not in (\'rejected\',\'to be cancelled\') ) AS B where A.customer_id_final = B.customer_id_final; UPDATE bbk_db.maplemonk.FACT_ITEMS_BBK AS A SET A.customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, saledate, CASE WHEN saledate = acquisition_Date and lower(orderstatus) not in (\'rejected\',\'to be cancelled\') THEN \'New\' when saledate < acquisition_Date then \'Yet to make completed order\' else \'Repeated\' END AS Flag FROM bbk_db.maplemonk.FACT_ITEMS_bbk )AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final and A.saledate = b.saledate ; UPDATE bbk_db.maplemonk.FACT_ITEMS_BBK SET customer_flag = CASE WHEN customer_flag IS NULL and lower(orderstatus) not in (\'rejected\',\'to be cancelled\') THEN \'New\' when customer_flag IS NULL and lower(orderstatus) in (\'rejected\',\'to be cancelled\') THEN \'Yet to make completed order\' ELSE customer_flag END; UPDATE bbk_db.maplemonk.FACT_ITEMS_BBK AS A SET A.customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, saledate, CASE WHEN Last_day(saledate, \'month\') = last_day(acquisition_Date, \'month\') then \'New\' when last_Day(saledate, \'month\') < last_Day(acquisition_date, \'month\') then \'Yet to make completed order\' ELSE \'Repeated\' END AS Flag FROM bbk_db.maplemonk.FACT_ITEMS_BBK)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE bbk_db.maplemonk.FACT_ITEMS_bbk SET customer_flag_month = CASE WHEN customer_flag_month IS NULL and lower(orderstatus) not in (\'rejected\',\'to be cancelled\') THEN \'New\' ELSE customer_flag_month END;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from BBK_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        