{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table eggozdb.maplemonk.Date_area_dim as select cast(Date as date) Date, area_classification from eggozdb.maplemonk.date_dim cross join (select distinct area_classification from eggozdb.maplemonk.my_sql_retailer_retailer); CREATE OR REPLACE TABLE eggozdb.maplemonk.SALES_SUMMARY AS select * ,sum(Net_Sales) over (partition by area_classification,year(date), month(date) order by year (date), month(date), Date) MTD_Net_Sales from (select a.date Date ,a.area_classification Area_classification ,ifnull(b.Net_sales,0) Net_Sales from eggozdb.maplemonk.Date_area_dim a left join ( select date ,area_classification ,sum(sales_per_item) Net_sales from ( select cast(timestampadd(minute,660,o.date) as date) Date ,o.id ,ol1.id ,rr.area_classification Area_Classification ,sum(o.order_price_amount)/sum(ol2.Items) Sales_per_item from eggozdb.Maplemonk.my_sql_order_order o left join eggozdb.Maplemonk.my_sql_order_orderline ol1 ON o.id=ol1.order_id left join eggozdb.Maplemonk.my_sql_product_product pp ON ol1.product_id =pp.id left join eggozdb.Maplemonk.my_sql_retailer_retailer rr ON o.retailer_id =rr.id left join (select order_id, COUNT(1) AS Items from eggozdb.Maplemonk.my_sql_order_orderline group by order_id) ol2 ON o.id=ol2.order_id WHERE o.status in (\'delivered\', \'completed\') and o.is_trial = \'FALSE\' group by cast(timestampadd(minute,660,o.date) as date) , rr.area_classification, o.id, ol1.id ) group by date ,area_classification) b on a.area_classification = b.area_classification and a.date = b.date where year(a.date)>= 2020 and year(a.date) <= year(getdate()) ); CREATE OR REPLACE TABLE eggozdb.maplemonk.Eggs_Sold_SUMMARY AS select * ,sum(Eggs_sold) over (partition by area_classification,year(date), month(date) order by year (date), month(date), Date) MTD_Eggs_sold ,sum(Eggs_sold_white) over (partition by area_classification,year(date), month(date) order by year (date), month(date), Date) MTD_Eggs_sold_white ,sum(Eggs_sold_brown) over (partition by area_classification,year(date), month(date) order by year (date), month(date), Date) MTD_Eggs_sold_brown ,sum(Eggs_sold_nutra) over (partition by area_classification,year(date), month(date) order by year (date), month(date), Date) MTD_Eggs_sold_nutra ,sum(Eggs_sold_liquid) over (partition by area_classification,year(date), month(date) order by year (date), month(date), Date) MTD_Eggs_sold_liquid from ( select a.date Date ,a.area_classification Area_classification ,ifnull(b.Eggs_sold,0) Eggs_sold ,ifnull(b.eggs_sold_white,0) Eggs_sold_white ,ifnull(b.eggs_sold_brown,0) Eggs_sold_brown ,ifnull(b.eggs_sold_nutra,0) Eggs_sold_nutra ,ifnull(b.eggs_sold_liquid,0) Eggs_sold_liquid from eggozdb.maplemonk.Date_area_dim a left join ( select date ,area_classification ,(case when eggs_sold_white is null then 0 else eggs_sold_white end) Eggs_sold_White ,(case when eggs_sold_brown is null then 0 else eggs_sold_Brown end) Eggs_sold_Brown ,(case when eggs_sold_Nutra is null then 0 else eggs_sold_Nutra end) Eggs_sold_Nutra ,(case when eggs_sold_Liquid is null then 0 else eggs_sold_Liquid end) Eggs_sold_Liquid ,((case when eggs_sold_white is null then 0 else eggs_sold_white end) + (case when eggs_sold_brown is null then 0 else eggs_sold_Brown end) + (case when eggs_sold_Nutra is null then 0 else eggs_sold_Nutra end) + (case when eggs_sold_Liquid is null then 0 else eggs_sold_Liquid end)) as eggs_sold from ( select cast(timestampadd(minute,660,o.date) as date) Date ,rr.area_classification Area_Classification ,(case when pp.name like \'%White%\' then \'White\' when pp.name like \'%Brown%\' then \'Brown\' when pp.name like \'%Nutra%\' then \'Nutra\' else \'Liquid\' end) Egg_Name ,SUM(ol1.quantity*pp.SKU_Count) Eggs_Sold from eggozdb.Maplemonk.my_sql_order_order o left join eggozdb.Maplemonk.my_sql_order_orderline ol1 ON o.id=ol1.order_id left join eggozdb.Maplemonk.my_sql_product_product pp ON ol1.product_id =pp.id left join eggozdb.Maplemonk.my_sql_retailer_retailer rr ON o.retailer_id =rr.id left join (select order_id, COUNT(1) AS Items from eggozdb.Maplemonk.my_sql_order_orderline group by order_id) ol2 ON o.id=ol2.order_id WHERE o.status in (\'delivered\', \'completed\') and o.is_trial = \'FALSE\' group by cast(timestampadd(minute,660,o.date) as date) , rr.area_classification, (case when pp.name like \'%White%\' then \'White\' when pp.name like \'%Brown%\' then \'Brown\' when pp.name like \'%Nutra%\' then \'Nutra\' else \'Liquid\' end) ) pivot( sum(eggs_sold) for egg_name in (\'White\', \'Brown\', \'Nutra\', \'Liquid\')) as p (Date, Area_Classification, Eggs_sold_White, Eggs_sold_Brown, Eggs_sold_Nutra, Eggs_sold_Liquid) ) b on a.area_classification = b.Area_Classification and a.date = b.date where year(a.date)>= 2020 and year(a.date) <= year(getdate()) ); CREATE OR REPLACE TABLE eggozdb.maplemonk.Collection_Summary as select * ,sum(Collections) over (partition by area_classification,year(date), month(date) order by year (date), month(date), Date) MTD_Collections from ( select a.date Date ,a.area_classification Area_classification ,ifnull(b.Collections,0) Collections from eggozdb.maplemonk.Date_area_dim a left join ( select date(timestampadd(minute,660,pp.created_at)) Collection_Date, area_classification, sum(pay_amount) Collections from eggozdb.maplemonk.my_sql_payment_payment pp left join eggozdb.maplemonk.my_sql_payment_invoice pi on pi.id = pp.invoice_id left join eggozdb.maplemonk.my_sql_payment_salestransaction ps on pp.salesTransaction_id = ps.id left join eggozdb.maplemonk.my_sql_order_order oo on oo.id = pi.order_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = ps.retailer_id where ps.transaction_type = \'Credit\' and ps.is_trial = \'FALSE\' group by area_classification, date(timestampadd(minute,660,pp.created_At)) ) b on a.area_classification = b.area_classification and a.date = b.collection_date where year(a.date)>= 2020 and year(a.date) <= year(getdate()) ); create or replace table eggozdb.maplemonk.Replacement_Summary as select * ,sum(Eggs_replaced) over (partition by area_classification,year(date), month(date) order by year (date), month(date), Date) MTD_Eggs_replaced ,sum(Eggs_replaced_white) over (partition by area_classification,year(date), month(date) order by year (date), month(date), Date) MTD_Eggs_replaced_white ,sum(Eggs_replaced_brown) over (partition by area_classification,year(date), month(date) order by year (date), month(date), Date) MTD_Eggs_replaced_brown ,sum(Eggs_replaced_nutra) over (partition by area_classification,year(date), month(date) order by year (date), month(date), Date) MTD_Eggs_replaced_nutra ,sum(Eggs_replaced_liquid) over (partition by area_classification,year(date), month(date) order by year (date), month(date), Date) MTD_Eggs_replaced_liquid from ( select a.date Date ,a.area_classification Area_classification ,ifnull(b.Eggs_replaced,0) Eggs_replaced ,ifnull(b.eggs_replaced_white,0) Eggs_replaced_white ,ifnull(b.eggs_replaced_brown,0) Eggs_replaced_brown ,ifnull(b.eggs_replaced_nutra,0) Eggs_replaced_nutra ,ifnull(b.eggs_replaced_liquid,0) Eggs_replaced_liquid from eggozdb.maplemonk.Date_area_dim a left join ( select replacement_date , area_classification ,(case when eggs_replaced_white is null then 0 else eggs_replaced_white end) Eggs_Replaced_White ,(case when eggs_replaced_brown is null then 0 else eggs_replaced_Brown end) Eggs_replaced_Brown ,(case when eggs_replaced_Nutra is null then 0 else eggs_replaced_Nutra end) Eggs_replaced_Nutra ,(case when eggs_replaced_Liquid is null then 0 else eggs_replaced_Liquid end) Eggs_replaced_Liquid ,((case when eggs_replaced_white is null then 0 else eggs_replaced_white end) + (case when eggs_replaced_brown is null then 0 else eggs_replaced_Brown end) + (case when eggs_replaced_Nutra is null then 0 else eggs_replaced_Nutra end) + (case when eggs_replaced_Liquid is null then 0 else eggs_replaced_Liquid end)) as eggs_replaced from ( select date(timestampadd(minute,660,or1.date)) as Replacement_Date ,rr.area_classification ,(case when pp.name like \'%White%\' then \'White\' when pp.name like \'%Brown%\' then \'Brown\' when pp.name like \'%Nutra%\' then \'Nutra\' else \'Liquid\' end) Egg_Name ,sum(or1.quantity* pp.sku_count) Eggs_replaced from eggozdb.maplemonk.my_sql_order_orderreturnline or1 left join eggozdb.maplemonk.my_sql_retailer_retailer rr ON or1.retailer_id =rr.id left JOIN eggozdb.maplemonk.my_sql_product_product pp on pp.id = or1.product_id where line_type in (\'Replacement\') group by rr.area_classification, date(timestampadd(minute,660,or1.date)) ,(case when pp.name like \'%White%\' then \'White\' when pp.name like \'%Brown%\' then \'Brown\' when pp.name like \'%Nutra%\' then \'Nutra\' else \'Liquid\' end) ) pivot( sum(eggs_replaced) for egg_name in (\'White\', \'Brown\', \'Nutra\', \'Liquid\')) as p (Replacement_date, area_classification, Eggs_Replaced_White, Eggs_Replaced_Brown, Eggs_Replaced_Nutra, Eggs_Replaced_Liquid) ) b on a.area_classification = b.area_classification and a.date = b.replacement_date where year(a.date)>= 2020 and year(a.date) <= year(getdate()) ) ; create or replace table eggozdb.maplemonk.Retailers_onboarded_summary as select a.date date, a.area_classification Area_Classification ,sum(a.Daily_Retailers_Onboarded) over (partition by a.area_classification,year(a.date), month(a.date) order by year (a.date), month(a.date), a.Date) MTD_Retailers_Onboarded ,a.Daily_Retailers_Onboarded Daily_Retailers_Onboarded ,b.active_stores from ( select dad.date date, dad.area_classification area_classification, count(distinct rr.RETAILER_ID) Daily_Retailers_onboarded from eggozdb.maplemonk.Date_area_dim dad left join (select distinct RETAILER_ID ,ACQ_DATE,area_classification from (select o.*,MIN(date(timestampadd(minute,660,O.DATE))) OVER(PARTITION BY o.RETAILER_ID) AS ACQ_DATE,r.area_classification from eggozdb.maplemonk.my_sql_order_order o left join eggozdb.maplemonk.my_sql_retailer_retailer r on o.RETAILER_ID=r.id WHERE O.RETAILER_ID IS NOT NULL AND O.DATE::DATE>=\'2020-05-01\' AND O.STATUS IN (\'delivered\', \'completed\') AND O.IS_TRIAL = \'FALSE\' )x) rr on dad.date = date(timestampadd(minute,660,rr.ACQ_DATE)) and dad.area_classification = rr.area_classification where year(dad.date)>= 2020 and year(dad.date) <= year(getdate()) group by dad.date, dad.area_classification order by dad.date asc ) a left join ( select dad.date date, dad.area_classification area_classification, count(distinct owa.retailer_id) active_Stores from eggozdb.maplemonk.Date_area_dim dad left join (select oo.retailer_id, oo.date,oo.id, rr.area_classification ac from eggozdb.maplemonk.my_sql_order_order oo left join eggozdb.maplemonk.my_sql_retailer_retailer rr on oo.retailer_id = rr.id WHERE oo.RETAILER_ID IS NOT NULL AND oo.DATE::DATE>=\'2020-05-01\' AND oo.STATUS IN (\'delivered\', \'completed\') AND oo.IS_TRIAL = \'FALSE\' ) owa on dad.area_classification = owa.ac and date_from_parts(year(dad.date), month(dad.date), 1) <= date(timestampadd(minute,660,owa.date)) and dad.date >= date(timestampadd(minute,660,owa.date)) group by dad.date, dad.area_classification order by dad.date asc ) b on a.date=b.date and a.area_classification = b.area_classification; CREATE OR REPLACE TABLE eggozdb.maplemonk.Summary_reporting_table as select a.date ,a.area_classification as Area ,Net_Sales ,MTD_Net_Sales as MTD_Sales ,eggs_sold ,Eggs_sold_white ,Eggs_sold_brown ,Eggs_sold_nutra ,Eggs_sold_liquid ,mtd_eggs_sold ,MTD_Eggs_sold_white ,MTD_Eggs_sold_brown ,MTD_Eggs_sold_nutra ,MTD_Eggs_sold_liquid ,collections ,mtd_collections ,eggs_replaced ,eggs_replaced_white ,eggs_replaced_brown ,eggs_replaced_nutra ,eggs_replaced_liquid ,mtd_eggs_replaced ,mtd_eggs_replaced_white ,mtd_eggs_replaced_brown ,mtd_eggs_replaced_nutra ,mtd_eggs_replaced_liquid ,d.Daily_Retailers_Onboarded ,d.MTD_Retailers_Onboarded ,os.MTD_Retailers_Onboarded Previous_Month_MTD_Retailers_onboarded ,datediff(day, date(DATE_TRUNC(\'MONTH\',a.date)), a.date) + 1 as Days_in_month ,d.ACTIVE_STORES from eggozdb.maplemonk.SALES_SUMMARY a left join eggozdb.maplemonk.Collection_Summary b on a.area_classification = b.area_classification and a.date = b.date left join eggozdb.maplemonk.Replacement_Summary c on a.area_classification = c.area_classification and a.date = c.date left join eggozdb.maplemonk.Retailers_onboarded_summary d on d.area_Classification = a.area_classification and d.date = a.date left join eggozdb.maplemonk.Retailers_onboarded_summary os on os.area_Classification = d.area_classification and os.date = DATEADD(DAY,(-1), DATE_TRUNC(\'MONTH\',d.date)) left join eggozdb.maplemonk.Eggs_Sold_SUMMARY e on e.area_Classification = a.area_classification and e.date = a.date ; create or replace table eggozdb.Maplemonk.Sales_summary_beat_retailer as select date ,area_classification ,beat_name ,beat_number_operations ,beat_number_original ,code ,SKU ,name classification_name ,sum(sales_per_item) Net_sales from ( select cast(timestampadd(minute,660,o.date) as date) Date ,o.id ,ol1.id ,rr.area_classification Area_Classification ,rr.code ,ba.beat_name ,ba.beat_number beat_number_operations ,rr.beat_number beat_number_original ,rc.name ,rr.onboarding_status ,sum(o.order_price_amount)/sum(ol2.Items) Sales_per_item ,concat(pp.sku_count,left(pp.name,1)) SKU from eggozdb.Maplemonk.my_sql_order_order o left join eggozdb.Maplemonk.my_sql_order_orderline ol1 ON o.id=ol1.order_id left join eggozdb.Maplemonk.my_sql_product_product pp ON ol1.product_id =pp.id left join eggozdb.Maplemonk.my_sql_retailer_retailer rr ON o.retailer_id =rr.id left join eggozdb.Maplemonk.my_sql_retailer_classification rc ON rr.classification_id = rc.id left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment ba on ba.id = o.beat_assignment_id left join (select order_id, COUNT(1) AS Items from eggozdb.Maplemonk.my_sql_order_orderline group by order_id) ol2 ON o.id=ol2.order_id WHERE o.status in (\'delivered\', \'completed\') and o.is_trial = \'FALSE\' and o.order_brand_type = \'branded\' group by cast(timestampadd(minute,660,o.date) as date) , rr.area_classification, rr.code, ba.beat_name,ba.beat_number, o.id, ol1.id, concat(pp.sku_count,left(pp.name,1)),rc.name, rr.onboarding_status,rr.beat_number ) group by date ,area_classification, beat_name, code, SKU, name, onboarding_status,beat_number_operations, beat_number_original ; create or replace table eggozdb.Maplemonk.Eggs_Sold_SUMMARY_beat_retailer as select date ,area_classification ,beat_name ,beat_number_operations ,beat_number_original ,code ,name classification_name ,SKU ,(case when eggs_sold_white is null then 0 else eggs_sold_white end) Eggs_sold_White ,(case when eggs_sold_brown is null then 0 else eggs_sold_Brown end) Eggs_sold_Brown ,(case when eggs_sold_Nutra is null then 0 else eggs_sold_Nutra end) Eggs_sold_Nutra ,(case when eggs_sold_Liquid is null then 0 else eggs_sold_Liquid end) Eggs_sold_Liquid ,((case when eggs_sold_white is null then 0 else eggs_sold_white end) + (case when eggs_sold_brown is null then 0 else eggs_sold_Brown end) + (case when eggs_sold_Nutra is null then 0 else eggs_sold_Nutra end) + (case when eggs_sold_Liquid is null then 0 else eggs_sold_Liquid end)) as eggs_sold from ( select cast(timestampadd(minute,660,o.date) as date) Date ,rr.area_classification Area_Classification ,ba.beat_name ,ba.beat_number beat_number_operations ,rr.beat_number beat_number_original ,rr.code ,rc.name ,(case when pp.name like \'%White%\' then \'White\' when pp.name like \'%Brown%\' then \'Brown\' when pp.name like \'%Nutra%\' then \'Nutra\' else \'Liquid\' end) Egg_Name ,concat(pp.sku_count,left(pp.name,1)) SKU ,SUM(ol1.quantity*pp.SKU_Count) Eggs_Sold from eggozdb.Maplemonk.my_sql_order_order o left join eggozdb.Maplemonk.my_sql_order_orderline ol1 ON o.id=ol1.order_id left join eggozdb.Maplemonk.my_sql_product_product pp ON ol1.product_id =pp.id left join eggozdb.Maplemonk.my_sql_retailer_retailer rr ON o.retailer_id =rr.id left join eggozdb.Maplemonk.my_sql_retailer_classification rc ON rr.classification_id = rc.id left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment ba on ba.id = o.beat_assignment_id left join (select order_id, COUNT(1) AS Items from eggozdb.Maplemonk.my_sql_order_orderline group by order_id) ol2 ON o.id=ol2.order_id WHERE o.status in (\'delivered\', \'completed\') and o.is_trial = \'FALSE\' and o.order_brand_type = \'branded\' group by cast(timestampadd(minute,660,o.date) as date) , rr.area_classification, (case when pp.name like \'%White%\' then \'White\' when pp.name like \'%Brown%\' then \'Brown\' when pp.name like \'%Nutra%\' then \'Nutra\' else \'Liquid\' end), ba.beat_name, ba.beat_number, rr.code, rc.name, concat(pp.sku_count,left(pp.name,1)), rr.beat_number ) pivot( sum(eggs_sold) for egg_name in (\'White\', \'Brown\', \'Nutra\', \'Liquid\')) as p (Date, Area_Classification, beat_name, beat_number_operations, beat_number_original, code, SKU, name, Eggs_sold_White, Eggs_sold_Brown, Eggs_sold_Nutra, Eggs_sold_Liquid) ; CREATE OR REPLACE TABLE eggozdb.maplemonk.Collection_Summary_beat_retailer as select date(timestampadd(minute,660,pp.created_at)) Date ,area_classification ,sum(pay_amount) Collections ,rr.code ,ba.beat_name ,ba.beat_number beat_number_operations ,rr.beat_number beat_number_original ,rc.name Classification_name from eggozdb.maplemonk.my_sql_payment_payment pp left join eggozdb.maplemonk.my_sql_payment_invoice pi on pi.id = pp.invoice_id left join eggozdb.maplemonk.my_sql_payment_salestransaction ps on pp.salesTransaction_id = ps.id left join eggozdb.maplemonk.my_sql_order_order oo on oo.id = pi.order_id left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment ba on ba.id = oo.beat_assignment_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = ps.retailer_id left join eggozdb.Maplemonk.my_sql_retailer_classification rc ON rr.classification_id = rc.id where ps.transaction_type = \'Credit\' and ps.is_trial = \'FALSE\' group by area_classification, date(timestampadd(minute,660,pp.created_At)), rr.code, ba.beat_name, rc.name, ba.beat_number, rr.beat_number ; create or replace table eggozdb.maplemonk.Replacement_Summary_beat_retailer as select replacement_date date , area_classification ,beat_name ,beat_number_operations ,beat_number_original ,code ,name classification_name ,SKU ,(case when eggs_replaced_white is null then 0 else eggs_replaced_white end) Eggs_Replaced_White ,(case when eggs_replaced_brown is null then 0 else eggs_replaced_Brown end) Eggs_replaced_Brown ,(case when eggs_replaced_Nutra is null then 0 else eggs_replaced_Nutra end) Eggs_replaced_Nutra ,(case when eggs_replaced_Liquid is null then 0 else eggs_replaced_Liquid end) Eggs_replaced_Liquid ,((case when eggs_replaced_white is null then 0 else eggs_replaced_white end) + (case when eggs_replaced_brown is null then 0 else eggs_replaced_Brown end) + (case when eggs_replaced_Nutra is null then 0 else eggs_replaced_Nutra end) + (case when eggs_replaced_Liquid is null then 0 else eggs_replaced_Liquid end)) as eggs_replaced from ( select date(timestampadd(minute,660,or1.date)) as Replacement_Date ,rr.area_classification ,ba.beat_name ,ba.beat_number beat_number_operations ,rr.beat_number beat_number_original ,rr.code ,rc.name ,(case when pp.name like \'%White%\' then \'White\' when pp.name like \'%Brown%\' then \'Brown\' when pp.name like \'%Nutra%\' then \'Nutra\' else \'Liquid\' end) Egg_Name ,concat(pp.sku_count,left(pp.name,1)) SKU ,sum(or1.quantity* pp.sku_count) Eggs_replaced from eggozdb.maplemonk.my_sql_order_orderreturnline or1 left join eggozdb.maplemonk.my_sql_retailer_retailer rr ON or1.retailer_id =rr.id left join eggozdb.Maplemonk.my_sql_retailer_classification rc ON rr.classification_id = rc.id left JOIN eggozdb.maplemonk.my_sql_product_product pp on pp.id = or1.product_id left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment ba on ba.id = or1.beat_assignment_id where line_type in (\'Replacement\') group by rr.area_classification, date(timestampadd(minute,660,or1.date)) ,(case when pp.name like \'%White%\' then \'White\' when pp.name like \'%Brown%\' then \'Brown\' when pp.name like \'%Nutra%\' then \'Nutra\' else \'Liquid\' end),ba.beat_name, ba.beat_number, rr.beat_number ,rr.code,rc.name, concat(pp.sku_count,left(pp.name,1)) ) pivot( sum(eggs_replaced) for egg_name in (\'White\', \'Brown\', \'Nutra\', \'Liquid\')) as p (Replacement_date, area_classification, beat_name, beat_number_operations, beat_number_original, SKU, code, name, Eggs_Replaced_White, Eggs_Replaced_Brown, Eggs_Replaced_Nutra, Eggs_Replaced_Liquid) ; create or replace table eggozdb.maplemonk.Promo_Summary_beat_retailer as select Promo_date date , area_classification ,beat_name ,beat_number_operations ,beat_number_original ,code ,name classification_name ,SKU ,(case when eggs_promo_white is null then 0 else eggs_promo_white end) Eggs_promo_White ,(case when eggs_promo_brown is null then 0 else eggs_promo_Brown end) Eggs_promo_Brown ,(case when eggs_promo_Nutra is null then 0 else eggs_promo_Nutra end) Eggs_promo_Nutra ,(case when eggs_promo_Liquid is null then 0 else eggs_promo_Liquid end) Eggs_promo_Liquid ,((case when eggs_promo_white is null then 0 else eggs_promo_white end) + (case when eggs_promo_brown is null then 0 else eggs_promo_Brown end) + (case when eggs_promo_Nutra is null then 0 else eggs_promo_Nutra end) + (case when eggs_promo_Liquid is null then 0 else eggs_promo_Liquid end)) as eggs_promo from ( select date(timestampadd(minute,660,or1.date)) as Promo_Date ,rr.area_classification ,ba.beat_name ,ba.beat_number beat_number_operations ,rr.beat_number beat_number_original ,rr.code ,rc.name ,(case when pp.name like \'%White%\' then \'White\' when pp.name like \'%Brown%\' then \'Brown\' when pp.name like \'%Nutra%\' then \'Nutra\' else \'Liquid\' end) Egg_Name ,concat(pp.sku_count,left(pp.name,1)) SKU ,sum(or1.quantity* pp.sku_count) Eggs_promo from eggozdb.maplemonk.my_sql_order_orderreturnline or1 left join eggozdb.maplemonk.my_sql_retailer_retailer rr ON or1.retailer_id =rr.id left join eggozdb.Maplemonk.my_sql_retailer_classification rc ON rr.classification_id = rc.id left JOIN eggozdb.maplemonk.my_sql_product_product pp on pp.id = or1.product_id left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment ba on ba.id = or1.beat_assignment_id where line_type in (\'Promo\') group by rr.area_classification, date(timestampadd(minute,660,or1.date)) ,(case when pp.name like \'%White%\' then \'White\' when pp.name like \'%Brown%\' then \'Brown\' when pp.name like \'%Nutra%\' then \'Nutra\' else \'Liquid\' end),ba.beat_name,ba.beat_number, rr.beat_number ,rr.code, rc.name, concat(pp.sku_count,left(pp.name,1)) ) pivot( sum(eggs_promo) for egg_name in (\'White\', \'Brown\', \'Nutra\', \'Liquid\')) as p (promo_date, area_classification, beat_name,beat_number_operations,beat_number_original, SKU, code, name, Eggs_promo_White, Eggs_promo_Brown, Eggs_promo_Nutra, Eggs_promo_Liquid) ; CREATE OR REPLACE TABLE eggozdb.maplemonk.Pendency_beat_retailer as select cast(timestampadd(minute,660,oo.date) as date) Date ,area_classification ,sum(pay_amount) Collections_as_of_today ,rr.code ,ba.beat_name ,ba.beat_number beat_number_operations ,rr.beat_number beat_number_original ,rc.name Classification_name from eggozdb.maplemonk.my_sql_payment_payment pp left join eggozdb.maplemonk.my_sql_payment_invoice pi on pi.id = pp.invoice_id left join eggozdb.maplemonk.my_sql_payment_salestransaction ps on pp.salesTransaction_id = ps.id left join eggozdb.maplemonk.my_sql_order_order oo on oo.id = pi.order_id left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment ba on ba.id = oo.beat_assignment_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = ps.retailer_id left join eggozdb.Maplemonk.my_sql_retailer_classification rc ON rr.classification_id = rc.id where ps.transaction_type = \'Credit\' and ps.is_trial = \'FALSE\' group by area_classification, cast(timestampadd(minute,660,oo.date) as date), rr.code, ba.beat_name, rc.name, ba.beat_number, rr.beat_number ; CREATE OR REPLACE TABLE eggozdb.maplemonk.Summary_reporting_table_beat_retailer as WITH SALES_SUMMARY_beat_retailer_CTE AS ( select DATE, AREA_CLASSIFICATION, BEAT_NAME, beat_number_operations, beat_number_original, CODE, classification_name, SUM(NET_SALES)AS NET_SALES from eggozdb.maplemonk.SALES_SUMMARY_beat_retailer GROUP BY DATE, AREA_CLASSIFICATION, BEAT_NAME, beat_number_operations, beat_number_original, CODE, classification_name), Replacement_Summary_beat_retailer_CTE AS ( SELECT DATE, AREA_CLASSIFICATION, BEAT_NAME, beat_number_operations, beat_number_original, CODE, classification_name, sum(EGGS_REPLACED_WHITE)EGGS_REPLACED_WHITE, sum(EGGS_REPLACED_BROWN)EGGS_REPLACED_BROWN, sum(EGGS_REPLACED_NUTRA)EGGS_REPLACED_NUTRA, sum(EGGS_REPLACED_LIQUID)EGGS_REPLACED_LIQUID, sum(EGGS_REPLACED)EGGS_REPLACED FROM eggozdb.maplemonk.Replacement_Summary_beat_retailer GROUP BY DATE, AREA_CLASSIFICATION, BEAT_NAME, beat_number_operations, beat_number_original, CODE, classification_name), EGGS_SOLD_SUMMARY_BEAT_RETAILER_CTE AS ( select DATE, AREA_CLASSIFICATION, BEAT_NAME, beat_number_operations, beat_number_original, CODE, classification_name, sum(EGGS_SOLD_WHITE)EGGS_SOLD_WHITE, sum(EGGS_SOLD_BROWN)EGGS_SOLD_BROWN, sum(EGGS_SOLD_NUTRA)EGGS_SOLD_NUTRA, sum(EGGS_SOLD_LIQUID)EGGS_SOLD_LIQUID, sum(EGGS_SOLD)EGGS_SOLD FROM eggozdb.maplemonk.EGGS_SOLD_SUMMARY_BEAT_RETAILER GROUP BY DATE, AREA_CLASSIFICATION, BEAT_NAME, beat_number_operations, beat_number_original, CODE, classification_name), Promo_Summary_beat_retailer_CTE AS ( SELECT DATE, AREA_CLASSIFICATION, BEAT_NAME, beat_number_operations, beat_number_original, CODE, classification_name, sum(EGGS_PROMO_WHITE)EGGS_PROMO_WHITE, sum(EGGS_PROMO_BROWN)EGGS_PROMO_BROWN, sum(EGGS_PROMO_NUTRA)EGGS_PROMO_NUTRA, sum(EGGS_PROMO_LIQUID)EGGS_PROMO_LIQUID, sum(EGGS_PROMO)EGGS_PROMO FROM eggozdb.maplemonk.Promo_Summary_beat_retailer GROUP BY DATE, AREA_CLASSIFICATION, BEAT_NAME, beat_number_operations, beat_number_original, CODE, classification_name) select v.*,mp.name Parent_retailer_name ,case when v.beat_number_operations = 0 then v.beat_number_original else v.beat_number_operations end as beat_number_consolidated from ( select w.*,(case when k.collections_as_of_today is null then 0 else k.collections_as_of_today end) collections_as_of_today from( select (case when x.date is not null then x.date else p.date end) date ,(case when x.area_classification is not null then x.area_classification else p.area_classification end) Area ,(case when x.beat_name is not null then x.beat_name else p.beat_name end) Beat_Name ,(case when x.beat_number_operations is not null then x.beat_number_operations else p.beat_number_operations end) beat_number_operations ,(case when x.beat_number_original is not null then x.beat_number_original else p.beat_number_original end) beat_number_original ,(case when x.code is not null then x.code else p.code end) Retailer_Name ,(case when x.classification_name is not null then x.classification_name else p.classification_name end) classification_name ,(case when NET_SALES is null then 0 else NET_SALES end) Net_Sales ,(case when eggs_sold is null then 0 else eggs_sold end) eggs_sold ,(case when eggs_sold_white is null then 0 else Eggs_sold_white end) Eggs_sold_white ,(case when Eggs_sold_brown is null then 0 else Eggs_sold_brown end) Eggs_sold_brown ,(case when Eggs_sold_nutra is null then 0 else Eggs_sold_nutra end) Eggs_sold_nutra ,(case when Eggs_sold_liquid is null then 0 else Eggs_sold_liquid end) Eggs_sold_liquid ,(case when collections is null then 0 else collections end) collections ,(case when eggs_replaced is null then 0 else eggs_replaced end) eggs_replaced ,(case when eggs_replaced_white is null then 0 else eggs_replaced_white end) eggs_replaced_white ,(case when eggs_replaced_brown is null then 0 else eggs_replaced_brown end) eggs_replaced_brown ,(case when eggs_replaced_nutra is null then 0 else eggs_replaced_nutra end) eggs_replaced_nutra ,(case when eggs_replaced_liquid is null then 0 else eggs_replaced_liquid end) eggs_replaced_liquid ,(case when eggs_promo is null then 0 else eggs_promo end) eggs_promo ,(case when eggs_promo_white is null then 0 else eggs_promo_white end) eggs_promo_white ,(case when eggs_promo_brown is null then 0 else eggs_promo_brown end) eggs_promo_brown ,(case when eggs_promo_nutra is null then 0 else eggs_promo_nutra end) eggs_promo_nutra ,(case when eggs_promo_liquid is null then 0 else eggs_promo_liquid end) eggs_promo_liquid from( select (case when y.date is not null then y.date else e.date end) date ,(case when y.area_classification is not null then y.area_classification else e.area_classification end) area_classification ,(case when y.beat_name is not null then y.beat_name else e.beat_name end) Beat_Name ,(case when y.beat_number_operations is not null then y.beat_number_operations else e.beat_number_operations end) beat_number_operations ,(case when y.beat_number_original is not null then y.beat_number_original else e.beat_number_original end) beat_number_original ,(case when y.code is not null then y.code else e.code end) code ,(case when y.classification_name is not null then y.classification_name else e.classification_name end) classification_name ,NET_SALES ,collections ,eggs_replaced ,eggs_replaced_white ,eggs_replaced_brown ,eggs_replaced_nutra ,eggs_replaced_liquid ,eggs_sold ,eggs_sold_white ,eggs_sold_brown ,eggs_sold_nutra ,eggs_sold_liquid from (select (case when z.date is not null then z.date else c.date end) date ,(case when z.area_classification is not null then z.area_classification else c.area_classification end) area_classification ,(case when z.beat_name is not null then z.beat_name else c.beat_name end) beat_name ,(case when z.beat_number_operations is not null then z.beat_number_operations else c.beat_number_operations end) beat_number_operations ,(case when z.beat_number_original is not null then z.beat_number_original else c.beat_number_original end) beat_number_original ,(case when z.code is not null then z.code else c.code end) code ,(case when z.classification_name is not null then z.classification_name else c.classification_name end) classification_name ,NET_SALES ,collections ,eggs_replaced ,eggs_replaced_white ,eggs_replaced_brown ,eggs_replaced_nutra ,eggs_replaced_liquid from (select (case when a.date is not null then a.date else b.date end) date ,(case when a.area_classification is not null then a.area_classification else b.area_classification end) area_classification ,(case when a.beat_name is not null then a.beat_name else b.beat_name end) beat_name ,(case when a.beat_number_operations is not null then a.beat_number_operations else b.beat_number_operations end) beat_number_operations ,(case when a.beat_number_original is not null then a.beat_number_original else b.beat_number_original end) beat_number_original ,(case when a.code is not null then a.code else b.code end) code ,(case when a.classification_name is not null then a.classification_name else b.classification_name end) classification_name ,NET_SALES ,collections from SALES_SUMMARY_beat_retailer_CTE a full outer join eggozdb.maplemonk.Collection_Summary_beat_retailer b on a.area_classification = b.area_classification and a.date = b.date and a.code = b.code and a.beat_name = b.beat_name and a.beat_number_operations = b.beat_number_operations and a.beat_number_original = b.beat_number_original and a.classification_name = b.classification_name ) z full outer join Replacement_Summary_beat_retailer_CTE c on z.area_classification = c.area_classification and z.date = c.date and z.code = c.code and z.beat_name = c.beat_name and z.beat_number_operations = c.beat_number_operations and z.beat_number_original = c.beat_number_original and z.classification_name = c.classification_name ) y full outer join EGGS_SOLD_SUMMARY_BEAT_RETAILER_CTE e on e.area_Classification = y.area_classification and e.date = y.date and e.code = y.code and e.beat_name = y.beat_name and e.beat_number_operations = y.beat_number_operations and e.beat_number_original = y.beat_number_original and e.classification_name = y.classification_name )x full outer join Promo_Summary_beat_retailer_CTE p on p.area_Classification = x.area_classification and p.date = x.date and p.code = x.code and p.beat_name = x.beat_name and p.beat_number_operations = x.beat_number_operations and p.beat_number_original = x.beat_number_original and p.classification_name = x.classification_name )w left join eggozdb.maplemonk.Pendency_beat_retailer k on w.area = k.area_classification and w.date = k.date and w.Retailer_Name = k.code and w.beat_name = k.beat_name and w.beat_number_operations = k.beat_number_operations and w.beat_number_original = k.beat_number_original and w.classification_name = k.classification_name )v left join eggozdb.maplemonk.mapping_parent_retailers mp on mp.code = v.retailer_name ; CREATE OR REPLACE TABLE eggozdb.maplemonk.Summary_reporting_table_beat_retailer_SKU as select x.*,mp.name Parent_retailer_name ,case when x.beat_number_operations = 0 then x.beat_number_original else x.beat_number_operations end as beat_number_consolidated from (select (case when y.date is not null then y.date else e.date end) date ,(case when y.area_classification is not null then y.area_classification else e.area_classification end) Area ,(case when y.beat_name is not null then y.beat_name else e.beat_name end) Beat_Name ,(case when y.beat_number_operations is not null then y.beat_number_operations else e.beat_number_operations end) beat_number_operations ,(case when y.beat_number_original is not null then y.beat_number_original else e.beat_number_original end) beat_number_original ,(case when y.code is not null then y.code else e.code end) Retailer_Name ,(case when y.classification_name is not null then y.classification_name else e.classification_name end) classification_name ,(case when y.SKU is not null then y.SKU else e.SKU end) SKU ,(case when NET_SALES is null then 0 else NET_SALES end) Net_Sales ,(case when eggs_sold is null then 0 else eggs_sold end) eggs_sold ,(case when eggs_sold_white is null then 0 else Eggs_sold_white end) Eggs_sold_white ,(case when Eggs_sold_brown is null then 0 else Eggs_sold_brown end) Eggs_sold_brown ,(case when Eggs_sold_nutra is null then 0 else Eggs_sold_nutra end) Eggs_sold_nutra ,(case when Eggs_sold_liquid is null then 0 else Eggs_sold_liquid end) Eggs_sold_liquid ,(case when eggs_replaced is null then 0 else eggs_replaced end) eggs_replaced ,(case when eggs_replaced_white is null then 0 else eggs_replaced_white end) eggs_replaced_white ,(case when eggs_replaced_brown is null then 0 else eggs_replaced_brown end) eggs_replaced_brown ,(case when eggs_replaced_nutra is null then 0 else eggs_replaced_nutra end) eggs_replaced_nutra ,(case when eggs_replaced_liquid is null then 0 else eggs_replaced_liquid end) eggs_replaced_liquid ,(case when eggs_promo is null then 0 else eggs_promo end) eggs_promo ,(case when eggs_promo_white is null then 0 else eggs_promo_white end) eggs_promo_white ,(case when eggs_promo_brown is null then 0 else eggs_promo_brown end) eggs_promo_brown ,(case when eggs_promo_nutra is null then 0 else eggs_promo_nutra end) eggs_promo_nutra ,(case when eggs_promo_liquid is null then 0 else eggs_promo_liquid end) eggs_promo_liquid from (select (case when z.date is not null then z.date else p.date end) date ,(case when z.area_classification is not null then z.area_classification else p.area_classification end) area_classification ,(case when z.beat_name is not null then z.beat_name else p.beat_name end) beat_name ,(case when z.beat_number_operations is not null then z.beat_number_operations else p.beat_number_operations end) beat_number_operations ,(case when z.beat_number_original is not null then z.beat_number_original else p.beat_number_original end) beat_number_original ,(case when z.code is not null then z.code else p.code end) code ,(case when z.classification_name is not null then z.classification_name else p.classification_name end) classification_name ,(case when z.SKU is not null then z.SKU else p.SKU end) SKU ,NET_SALES ,eggs_replaced ,eggs_replaced_white ,eggs_replaced_brown ,eggs_replaced_nutra ,eggs_replaced_liquid ,eggs_promo ,eggs_promo_white ,eggs_promo_brown ,eggs_promo_nutra ,eggs_promo_liquid from (select (case when a.date is not null then a.date else c.date end) date ,(case when a.area_classification is not null then a.area_classification else c.area_classification end) area_classification ,(case when a.beat_name is not null then a.beat_name else c.beat_name end) beat_name ,(case when a.beat_number_operations is not null then a.beat_number_operations else c.beat_number_operations end) beat_number_operations ,(case when a.beat_number_original is not null then a.beat_number_original else c.beat_number_original end) beat_number_original ,(case when a.code is not null then a.code else c.code end) code ,(case when a.classification_name is not null then a.classification_name else c.classification_name end) classification_name ,(case when a.SKU is not null then a.SKU else c.SKU end) SKU ,NET_SALES ,eggs_replaced ,eggs_replaced_white ,eggs_replaced_brown ,eggs_replaced_nutra ,eggs_replaced_liquid from eggozdb.maplemonk.SALES_SUMMARY_beat_retailer a full outer join eggozdb.maplemonk.Replacement_Summary_beat_retailer c on a.area_classification = c.area_classification and a.date = c.date and a.code = c.code and a.beat_name = c.beat_name and a.beat_number_operations = c.beat_number_operations and a.beat_number_original = c.beat_number_original and a.SKU=c.SKU and a.classification_name=c.classification_name ) z full outer join eggozdb.maplemonk.Promo_Summary_beat_retailer p on p.area_Classification = z.area_classification and p.date = z.date and p.code = z.code and p.beat_name = z.beat_name and p.beat_number_operations = z.beat_number_operations and p.beat_number_original = z.beat_number_original and p.SKU=z.SKU and p.classification_name=z.classification_name )y full outer join eggozdb.maplemonk.EGGS_SOLD_SUMMARY_BEAT_RETAILER e on e.area_Classification = y.area_classification and e.date = y.date and e.code = y.code and e.beat_name = y.beat_name and e.beat_number_operations = y.beat_number_operations and e.beat_number_original = y.beat_number_original and e.SKU=y.SKU and e.classification_name=y.classification_name )x left join eggozdb.maplemonk.mapping_parent_retailers mp on mp.code = x.retailer_name;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        