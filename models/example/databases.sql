{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table eggozdb.Maplemonk.Sales_summary_beat_retailer_DSO as select date ,area_classification ,beat_name ,beat_number_operations ,beat_number_original ,code ,SKU ,name classification_name ,Operator ,sum(sales_per_item) Net_sales from ( select cast(timestampadd(minute,660,o.date) as date) Date ,o.id ,ol1.id ,rr.area_classification Area_Classification ,rr.code ,ba.beat_name ,ba.beat_number beat_number_operations ,rr.beat_number beat_number_original ,rc.name ,rr.onboarding_status ,case when sp.name is null then sp2.name else sp.name end as Operator ,sum(single_sku_rate*quantity + discounted_amount) Sales_per_item ,concat(pp.sku_count,left(pp.name,1)) SKU from eggozdb.Maplemonk.my_sql_order_order o left join eggozdb.Maplemonk.my_sql_order_orderline ol1 ON o.id=ol1.order_id left join eggozdb.Maplemonk.my_sql_product_product pp ON ol1.product_id =pp.id left join eggozdb.Maplemonk.my_sql_retailer_retailer rr ON o.retailer_id =rr.id left join eggozdb.Maplemonk.my_sql_retailer_classification rc ON rr.classification_id = rc.id left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment ba on ba.id = o.beat_assignment_id left join (select order_id, COUNT(1) AS Items from eggozdb.Maplemonk.my_sql_order_orderline group by order_id) ol2 ON o.id=ol2.order_id LEFT JOIN (SELECT ddp.id, cau.name FROM eggozdb.Maplemonk.my_sql_distributionchain_distributionpersonprofile ddp LEFT JOIN eggozdb.Maplemonk.my_sql_custom_auth_user cau ON cau.id = ddp.user_id) sp ON sp.id = o.distributor_id LEFT JOIN (SELECT ssp.id, cau.name FROM eggozdb.Maplemonk.my_sql_saleschain_salespersonprofile ssp LEFT JOIN eggozdb.Maplemonk.my_sql_custom_auth_user cau ON cau.id = ssp.user_id) sp2 ON sp2.id = o.salesPerson_id WHERE o.status in (\'delivered\', \'completed\') and o.is_trial = \'FALSE\' group by cast(timestampadd(minute,660,o.date) as date) , rr.area_classification, rr.code, ba.beat_name,ba.beat_number, o.id, ol1.id, concat(pp.sku_count,left(pp.name,1)),rc.name, rr.onboarding_status,rr.beat_number, case when sp.name is null then sp2.name else sp.name end ) group by date ,area_classification, beat_name, code, SKU, name, onboarding_status,beat_number_operations, beat_number_original, operator ; create or replace table eggozdb.Maplemonk.Eggs_Sold_SUMMARY_beat_retailer_DSO as select date ,area_classification ,beat_name ,beat_number_operations ,beat_number_original ,code ,name classification_name ,operator ,SKU ,(case when eggs_sold_white is null then 0 else eggs_sold_white end) Eggs_sold_White ,(case when eggs_sold_brown is null then 0 else eggs_sold_Brown end) Eggs_sold_Brown ,(case when eggs_sold_Nutra is null then 0 else eggs_sold_Nutra end) Eggs_sold_Nutra ,(case when eggs_sold_Liquid is null then 0 else eggs_sold_Liquid end) Eggs_sold_Liquid ,((case when eggs_sold_white is null then 0 else eggs_sold_white end) + (case when eggs_sold_brown is null then 0 else eggs_sold_Brown end) + (case when eggs_sold_Nutra is null then 0 else eggs_sold_Nutra end) + (case when eggs_sold_Liquid is null then 0 else eggs_sold_Liquid end)) as eggs_sold from ( select cast(timestampadd(minute,660,o.date) as date) Date ,rr.area_classification Area_Classification ,ba.beat_name ,ba.beat_number beat_number_operations ,rr.beat_number beat_number_original ,rr.code ,rc.name ,case when sp.name is null then sp2.name else sp.name end as Operator ,(case when pp.name like \'%White%\' then \'White\' when pp.name like \'%Brown%\' then \'Brown\' when pp.name like \'%Nutra%\' then \'Nutra\' else \'Liquid\' end) Egg_Name ,concat(pp.sku_count,left(pp.name,1)) SKU ,SUM(ol1.quantity*pp.SKU_Count) Eggs_Sold from eggozdb.Maplemonk.my_sql_order_order o left join eggozdb.Maplemonk.my_sql_order_orderline ol1 ON o.id=ol1.order_id left join eggozdb.Maplemonk.my_sql_product_product pp ON ol1.product_id =pp.id left join eggozdb.Maplemonk.my_sql_retailer_retailer rr ON o.retailer_id =rr.id left join eggozdb.Maplemonk.my_sql_retailer_classification rc ON rr.classification_id = rc.id left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment ba on ba.id = o.beat_assignment_id left join (select order_id, COUNT(1) AS Items from eggozdb.Maplemonk.my_sql_order_orderline group by order_id) ol2 ON o.id=ol2.order_id LEFT JOIN (SELECT ddp.id, cau.name FROM eggozdb.Maplemonk.my_sql_distributionchain_distributionpersonprofile ddp LEFT JOIN eggozdb.Maplemonk.my_sql_custom_auth_user cau ON cau.id = ddp.user_id) sp ON sp.id = o.distributor_id LEFT JOIN (SELECT ssp.id, cau.name FROM eggozdb.Maplemonk.my_sql_saleschain_salespersonprofile ssp LEFT JOIN eggozdb.Maplemonk.my_sql_custom_auth_user cau ON cau.id = ssp.user_id) sp2 ON sp2.id = o.salesPerson_id WHERE o.status in (\'delivered\', \'completed\') and o.is_trial = \'FALSE\' group by cast(timestampadd(minute,660,o.date) as date) , rr.area_classification, (case when pp.name like \'%White%\' then \'White\' when pp.name like \'%Brown%\' then \'Brown\' when pp.name like \'%Nutra%\' then \'Nutra\' else \'Liquid\' end), ba.beat_name, ba.beat_number, rr.code, rc.name, concat(pp.sku_count,left(pp.name,1)), rr.beat_number,case when sp.name is null then sp2.name else sp.name end ) pivot( sum(eggs_sold) for egg_name in (\'White\', \'Brown\', \'Nutra\', \'Liquid\')) as p (Date, Area_Classification, beat_name, beat_number_operations, beat_number_original, code, SKU, name, operator, Eggs_sold_White, Eggs_sold_Brown, Eggs_sold_Nutra, Eggs_sold_Liquid) ; CREATE OR REPLACE TABLE eggozdb.maplemonk.Collection_Summary_beat_retailer_DSO as select date(timestampadd(minute,660,ps.transaction_date)) Date ,area_classification ,sum(pay_amount) Collections ,rr.code ,ba.beat_name ,ba.beat_number beat_number_operations ,rr.beat_number beat_number_original ,rc.name Classification_name ,pc.number payment_cycle ,case when sp.name is null then sp2.name else sp.name end as Operator from eggozdb.maplemonk.my_sql_payment_payment pp left join eggozdb.maplemonk.my_sql_payment_invoice pi on pi.id = pp.invoice_id left join eggozdb.maplemonk.my_sql_payment_salestransaction ps on pp.salesTransaction_id = ps.id left join eggozdb.maplemonk.my_sql_order_order oo on oo.id = pi.order_id left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment ba on ba.id = oo.beat_assignment_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = ps.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailerpaymentcycle pc on pc.id = rr.payment_cycle_id left join eggozdb.Maplemonk.my_sql_retailer_classification rc ON rr.classification_id = rc.id LEFT JOIN (SELECT ddp.id, cau.name FROM eggozdb.Maplemonk.my_sql_distributionchain_distributionpersonprofile ddp LEFT JOIN eggozdb.Maplemonk.my_sql_custom_auth_user cau ON cau.id = ddp.user_id) sp ON sp.id = oo.distributor_id LEFT JOIN (SELECT ssp.id, cau.name FROM eggozdb.Maplemonk.my_sql_saleschain_salespersonprofile ssp LEFT JOIN eggozdb.Maplemonk.my_sql_custom_auth_user cau ON cau.id = ssp.user_id) sp2 ON sp2.id = oo.salesPerson_id where ps.transaction_type in (\'Credit\',\'Adjusted\') and ps.is_trial = \'FALSE\' group by area_classification, date(timestampadd(minute,660,ps.transaction_date)), rr.code, ba.beat_name, rc.name, ba.beat_number, rr.beat_number, pc.number, case when sp.name is null then sp2.name else sp.name end ; create or replace table eggozdb.maplemonk.Replacement_Summary_beat_retailer_DSO as select replacement_date date , area_classification ,beat_name ,beat_number_operations ,beat_number_original ,code ,name classification_name ,Operator ,SKU ,(case when eggs_replaced_white is null then 0 else eggs_replaced_white end) Eggs_Replaced_White ,(case when eggs_replaced_brown is null then 0 else eggs_replaced_Brown end) Eggs_replaced_Brown ,(case when eggs_replaced_Nutra is null then 0 else eggs_replaced_Nutra end) Eggs_replaced_Nutra ,(case when eggs_replaced_Liquid is null then 0 else eggs_replaced_Liquid end) Eggs_replaced_Liquid ,((case when eggs_replaced_white is null then 0 else eggs_replaced_white end) + (case when eggs_replaced_brown is null then 0 else eggs_replaced_Brown end) + (case when eggs_replaced_Nutra is null then 0 else eggs_replaced_Nutra end) + (case when eggs_replaced_Liquid is null then 0 else eggs_replaced_Liquid end)) as eggs_replaced from ( select date(timestampadd(minute,660,or1.date)) as Replacement_Date ,rr.area_classification ,ba.beat_name ,ba.beat_number beat_number_operations ,rr.beat_number beat_number_original ,rr.code ,rc.name ,case when sp.name is null then sp2.name else sp.name end as Operator ,(case when pp.name like \'%White%\' then \'White\' when pp.name like \'%Brown%\' then \'Brown\' when pp.name like \'%Nutra%\' then \'Nutra\' else \'Liquid\' end) Egg_Name ,concat(pp.sku_count,left(pp.name,1)) SKU ,sum(or1.quantity* pp.sku_count) Eggs_replaced from eggozdb.maplemonk.my_sql_order_orderreturnline or1 left join eggozdb.maplemonk.my_sql_retailer_retailer rr ON or1.retailer_id =rr.id left join eggozdb.Maplemonk.my_sql_retailer_classification rc ON rr.classification_id = rc.id left JOIN eggozdb.maplemonk.my_sql_product_product pp on pp.id = or1.product_id left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment ba on ba.id = or1.beat_assignment_id LEFT JOIN (SELECT ddp.id, cau.name FROM eggozdb.Maplemonk.my_sql_distributionchain_distributionpersonprofile ddp LEFT JOIN eggozdb.Maplemonk.my_sql_custom_auth_user cau ON cau.id = ddp.user_id) sp ON sp.id = or1.distributor_id LEFT JOIN (SELECT ssp.id, cau.name FROM eggozdb.Maplemonk.my_sql_saleschain_salespersonprofile ssp LEFT JOIN eggozdb.Maplemonk.my_sql_custom_auth_user cau ON cau.id = ssp.user_id) sp2 ON sp2.id = or1.salesPerson_id where line_type in (\'Replacement\') group by rr.area_classification, date(timestampadd(minute,660,or1.date)) ,(case when pp.name like \'%White%\' then \'White\' when pp.name like \'%Brown%\' then \'Brown\' when pp.name like \'%Nutra%\' then \'Nutra\' else \'Liquid\' end),ba.beat_name, ba.beat_number, rr.beat_number ,rr.code,rc.name, concat(pp.sku_count,left(pp.name,1)),case when sp.name is null then sp2.name else sp.name end ) pivot( sum(eggs_replaced) for egg_name in (\'White\', \'Brown\', \'Nutra\', \'Liquid\')) as p (Replacement_date, area_classification, beat_name, beat_number_operations, beat_number_original, SKU, code, name,operator, Eggs_Replaced_White, Eggs_Replaced_Brown, Eggs_Replaced_Nutra, Eggs_Replaced_Liquid) ; create or replace table eggozdb.maplemonk.Promo_Summary_beat_retailer_DSO as select Promo_date date , area_classification ,beat_name ,beat_number_operations ,beat_number_original ,code ,name classification_name ,operator ,SKU ,(case when eggs_promo_white is null then 0 else eggs_promo_white end) Eggs_promo_White ,(case when eggs_promo_brown is null then 0 else eggs_promo_Brown end) Eggs_promo_Brown ,(case when eggs_promo_Nutra is null then 0 else eggs_promo_Nutra end) Eggs_promo_Nutra ,(case when eggs_promo_Liquid is null then 0 else eggs_promo_Liquid end) Eggs_promo_Liquid ,((case when eggs_promo_white is null then 0 else eggs_promo_white end) + (case when eggs_promo_brown is null then 0 else eggs_promo_Brown end) + (case when eggs_promo_Nutra is null then 0 else eggs_promo_Nutra end) + (case when eggs_promo_Liquid is null then 0 else eggs_promo_Liquid end)) as eggs_promo from ( select date(timestampadd(minute,660,or1.date)) as Promo_Date ,rr.area_classification ,ba.beat_name ,ba.beat_number beat_number_operations ,rr.beat_number beat_number_original ,rr.code ,rc.name ,case when sp.name is null then sp2.name else sp.name end as Operator ,(case when pp.name like \'%White%\' then \'White\' when pp.name like \'%Brown%\' then \'Brown\' when pp.name like \'%Nutra%\' then \'Nutra\' else \'Liquid\' end) Egg_Name ,concat(pp.sku_count,left(pp.name,1)) SKU ,sum(or1.quantity* pp.sku_count) Eggs_promo from eggozdb.maplemonk.my_sql_order_orderreturnline or1 left join eggozdb.maplemonk.my_sql_retailer_retailer rr ON or1.retailer_id =rr.id left join eggozdb.Maplemonk.my_sql_retailer_classification rc ON rr.classification_id = rc.id left JOIN eggozdb.maplemonk.my_sql_product_product pp on pp.id = or1.product_id left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment ba on ba.id = or1.beat_assignment_id LEFT JOIN (SELECT ddp.id, cau.name FROM eggozdb.Maplemonk.my_sql_distributionchain_distributionpersonprofile ddp LEFT JOIN eggozdb.Maplemonk.my_sql_custom_auth_user cau ON cau.id = ddp.user_id) sp ON sp.id = or1.distributor_id LEFT JOIN (SELECT ssp.id, cau.name FROM eggozdb.Maplemonk.my_sql_saleschain_salespersonprofile ssp LEFT JOIN eggozdb.Maplemonk.my_sql_custom_auth_user cau ON cau.id = ssp.user_id) sp2 ON sp2.id = or1.salesPerson_id where line_type in (\'Promo\') group by rr.area_classification, date(timestampadd(minute,660,or1.date)) ,(case when pp.name like \'%White%\' then \'White\' when pp.name like \'%Brown%\' then \'Brown\' when pp.name like \'%Nutra%\' then \'Nutra\' else \'Liquid\' end),ba.beat_name,ba.beat_number, rr.beat_number ,rr.code, rc.name, concat(pp.sku_count,left(pp.name,1)), case when sp.name is null then sp2.name else sp.name end ) pivot( sum(eggs_promo) for egg_name in (\'White\', \'Brown\', \'Nutra\', \'Liquid\')) as p (promo_date, area_classification, beat_name,beat_number_operations,beat_number_original, SKU, code, name, operator, Eggs_promo_White, Eggs_promo_Brown, Eggs_promo_Nutra, Eggs_promo_Liquid) ; create or replace table eggozdb.maplemonk.Return_Summary_beat_retailer_DSO as select return_date date , area_classification ,beat_name ,beat_number_operations ,beat_number_original ,code ,name classification_name ,operator ,SKU ,(case when eggs_return_white is null then 0 else eggs_return_white end) Eggs_return_White ,(case when eggs_return_brown is null then 0 else eggs_return_Brown end) Eggs_return_Brown ,(case when eggs_return_Nutra is null then 0 else eggs_return_Nutra end) Eggs_return_Nutra ,(case when eggs_return_Liquid is null then 0 else eggs_return_Liquid end) Eggs_return_Liquid ,((case when eggs_return_white is null then 0 else eggs_return_white end) + (case when eggs_return_brown is null then 0 else eggs_return_Brown end) + (case when eggs_return_Nutra is null then 0 else eggs_return_Nutra end) + (case when eggs_return_Liquid is null then 0 else eggs_return_Liquid end)) as eggs_return from ( select date(timestampadd(minute,660,or1.date)) as return_Date ,rr.area_classification ,ba.beat_name ,ba.beat_number beat_number_operations ,rr.beat_number beat_number_original ,rr.code ,rc.name ,case when sp.name is null then sp2.name else sp.name end as Operator ,(case when pp.name like \'%White%\' then \'White\' when pp.name like \'%Brown%\' then \'Brown\' when pp.name like \'%Nutra%\' then \'Nutra\' else \'Liquid\' end) Egg_Name ,concat(pp.sku_count,left(pp.name,1)) SKU ,sum(or1.quantity* pp.sku_count) Eggs_return from eggozdb.maplemonk.my_sql_order_orderreturnline or1 left join eggozdb.maplemonk.my_sql_retailer_retailer rr ON or1.retailer_id =rr.id left join eggozdb.Maplemonk.my_sql_retailer_classification rc ON rr.classification_id = rc.id left JOIN eggozdb.maplemonk.my_sql_product_product pp on pp.id = or1.product_id left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment ba on ba.id = or1.beat_assignment_id LEFT JOIN (SELECT ddp.id, cau.name FROM eggozdb.Maplemonk.my_sql_distributionchain_distributionpersonprofile ddp LEFT JOIN eggozdb.Maplemonk.my_sql_custom_auth_user cau ON cau.id = ddp.user_id) sp ON sp.id = or1.distributor_id LEFT JOIN (SELECT ssp.id, cau.name FROM eggozdb.Maplemonk.my_sql_saleschain_salespersonprofile ssp LEFT JOIN eggozdb.Maplemonk.my_sql_custom_auth_user cau ON cau.id = ssp.user_id) sp2 ON sp2.id = or1.salesPerson_id where line_type in (\'Return\') group by rr.area_classification, date(timestampadd(minute,660,or1.date)) ,(case when pp.name like \'%White%\' then \'White\' when pp.name like \'%Brown%\' then \'Brown\' when pp.name like \'%Nutra%\' then \'Nutra\' else \'Liquid\' end),ba.beat_name,ba.beat_number, rr.beat_number ,rr.code, rc.name, concat(pp.sku_count,left(pp.name,1)),case when sp.name is null then sp2.name else sp.name end ) pivot( sum(eggs_return) for egg_name in (\'White\', \'Brown\', \'Nutra\', \'Liquid\')) as p (return_date, area_classification, beat_name,beat_number_operations,beat_number_original, SKU, code, name, operator, Eggs_return_White, Eggs_return_Brown, Eggs_return_Nutra, Eggs_return_Liquid) ; CREATE OR REPLACE TABLE eggozdb.maplemonk.Pendency_beat_retailer_DSO as select cast(timestampadd(minute,660,oo.date) as date) Date ,area_classification ,sum(pay_amount) Collections_as_of_today ,rr.code ,ba.beat_name ,ba.beat_number beat_number_operations ,rr.beat_number beat_number_original ,rc.name Classification_name ,case when sp.name is null then sp2.name else sp.name end as Operator from eggozdb.maplemonk.my_sql_payment_payment pp left join eggozdb.maplemonk.my_sql_payment_invoice pi on pi.id = pp.invoice_id left join eggozdb.maplemonk.my_sql_payment_salestransaction ps on pp.salesTransaction_id = ps.id left join eggozdb.maplemonk.my_sql_order_order oo on oo.id = pi.order_id left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment ba on ba.id = oo.beat_assignment_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = ps.retailer_id left join eggozdb.Maplemonk.my_sql_retailer_classification rc ON rr.classification_id = rc.id LEFT JOIN (SELECT ddp.id, cau.name FROM eggozdb.Maplemonk.my_sql_distributionchain_distributionpersonprofile ddp LEFT JOIN eggozdb.Maplemonk.my_sql_custom_auth_user cau ON cau.id = ddp.user_id) sp ON sp.id = oo.distributor_id LEFT JOIN (SELECT ssp.id, cau.name FROM eggozdb.Maplemonk.my_sql_saleschain_salespersonprofile ssp LEFT JOIN eggozdb.Maplemonk.my_sql_custom_auth_user cau ON cau.id = ssp.user_id) sp2 ON sp2.id = oo.salesPerson_id where ps.transaction_type in (\'Credit\',\'Adjusted\') and ps.is_trial = \'FALSE\' group by area_classification, cast(timestampadd(minute,660,oo.date) as date), rr.code, ba.beat_name, rc.name, ba.beat_number, rr.beat_number,case when sp.name is null then sp2.name else sp.name end ; CREATE OR REPLACE TABLE eggozdb.maplemonk.Summary_reporting_table_beat_retailer_DSO as WITH SALES_SUMMARY_beat_retailer_CTE AS ( select DATE, AREA_CLASSIFICATION, BEAT_NAME, beat_number_operations, beat_number_original, CODE, classification_name, operator, SUM(NET_SALES)AS NET_SALES from eggozdb.maplemonk.SALES_SUMMARY_BEAT_RETAILER_DSO GROUP BY DATE, AREA_CLASSIFICATION, BEAT_NAME, beat_number_operations, beat_number_original, CODE, classification_name, operator), Replacement_Summary_beat_retailer_CTE AS ( SELECT DATE, AREA_CLASSIFICATION, BEAT_NAME, beat_number_operations, beat_number_original, CODE, classification_name, operator, sum(EGGS_REPLACED_WHITE)EGGS_REPLACED_WHITE, sum(EGGS_REPLACED_BROWN)EGGS_REPLACED_BROWN, sum(EGGS_REPLACED_NUTRA)EGGS_REPLACED_NUTRA, sum(EGGS_REPLACED_LIQUID)EGGS_REPLACED_LIQUID, sum(EGGS_REPLACED)EGGS_REPLACED FROM eggozdb.maplemonk.Replacement_Summary_beat_retailer_DSO GROUP BY DATE, AREA_CLASSIFICATION, BEAT_NAME, beat_number_operations, beat_number_original, CODE, classification_name, operator), EGGS_SOLD_SUMMARY_BEAT_RETAILER_CTE AS ( select DATE, AREA_CLASSIFICATION, BEAT_NAME, beat_number_operations, beat_number_original, CODE, classification_name, operator, sum(EGGS_SOLD_WHITE)EGGS_SOLD_WHITE, sum(EGGS_SOLD_BROWN)EGGS_SOLD_BROWN, sum(EGGS_SOLD_NUTRA)EGGS_SOLD_NUTRA, sum(EGGS_SOLD_LIQUID)EGGS_SOLD_LIQUID, sum(EGGS_SOLD)EGGS_SOLD FROM eggozdb.maplemonk.EGGS_SOLD_SUMMARY_BEAT_RETAILER_DSO GROUP BY DATE, AREA_CLASSIFICATION, BEAT_NAME, beat_number_operations, beat_number_original, CODE, classification_name, operator), Promo_Summary_beat_retailer_CTE AS ( SELECT DATE, AREA_CLASSIFICATION, BEAT_NAME, beat_number_operations, beat_number_original, CODE, classification_name, operator, sum(EGGS_PROMO_WHITE)EGGS_PROMO_WHITE, sum(EGGS_PROMO_BROWN)EGGS_PROMO_BROWN, sum(EGGS_PROMO_NUTRA)EGGS_PROMO_NUTRA, sum(EGGS_PROMO_LIQUID)EGGS_PROMO_LIQUID, sum(EGGS_PROMO)EGGS_PROMO FROM eggozdb.maplemonk.Promo_Summary_beat_retailer_DSO GROUP BY DATE, AREA_CLASSIFICATION, BEAT_NAME, beat_number_operations, beat_number_original, CODE, classification_name, operator), Return_Summary_beat_retailer_CTE AS ( SELECT DATE, AREA_CLASSIFICATION, BEAT_NAME, beat_number_operations, beat_number_original, CODE, classification_name, operator, sum(EGGS_Return_WHITE)EGGS_Return_WHITE, sum(EGGS_Return_BROWN)EGGS_Return_BROWN, sum(EGGS_Return_NUTRA)EGGS_Return_NUTRA, sum(EGGS_Return_LIQUID)EGGS_Return_LIQUID, sum(EGGS_Return)EGGS_Return FROM eggozdb.maplemonk.RETURN_SUMMARY_BEAT_RETAILER_DSO GROUP BY DATE, AREA_CLASSIFICATION, BEAT_NAME, beat_number_operations, beat_number_original, CODE, classification_name, operator) select v.*,rp.name Parent_retailer_name ,case when v.beat_number_operations = 0 then v.beat_number_original else v.beat_number_operations end as beat_number_consolidated from ( select u.*,(case when k.collections_as_of_today is null then 0 else k.collections_as_of_today end) collections_as_of_today from( select (case when w.date is not null then w.date else r.date end) date ,(case when w.area_classification is not null then w.area_classification else r.area_classification end) Area ,(case when w.beat_name is not null then w.beat_name else r.beat_name end) Beat_Name ,(case when w.beat_number_operations is not null then w.beat_number_operations else r.beat_number_operations end) beat_number_operations ,(case when w.beat_number_original is not null then w.beat_number_original else r.beat_number_original end) beat_number_original ,(case when w.code is not null then w.code else r.code end) Retailer_Name ,(case when w.classification_name is not null then w.classification_name else r.classification_name end) classification_name ,(case when w.operator is not null then w.operator else r.operator end) operator ,(case when NET_SALES is null then 0 else NET_SALES end) Net_Sales ,(case when eggs_sold is null then 0 else eggs_sold end) eggs_sold ,(case when eggs_sold_white is null then 0 else Eggs_sold_white end) Eggs_sold_white ,(case when Eggs_sold_brown is null then 0 else Eggs_sold_brown end) Eggs_sold_brown ,(case when Eggs_sold_nutra is null then 0 else Eggs_sold_nutra end) Eggs_sold_nutra ,(case when Eggs_sold_liquid is null then 0 else Eggs_sold_liquid end) Eggs_sold_liquid ,(case when collections is null then 0 else collections end) collections ,(case when eggs_replaced is null then 0 else eggs_replaced end) eggs_replaced ,(case when eggs_replaced_white is null then 0 else eggs_replaced_white end) eggs_replaced_white ,(case when eggs_replaced_brown is null then 0 else eggs_replaced_brown end) eggs_replaced_brown ,(case when eggs_replaced_nutra is null then 0 else eggs_replaced_nutra end) eggs_replaced_nutra ,(case when eggs_replaced_liquid is null then 0 else eggs_replaced_liquid end) eggs_replaced_liquid ,(case when eggs_promo is null then 0 else eggs_promo end) eggs_promo ,(case when eggs_promo_white is null then 0 else eggs_promo_white end) eggs_promo_white ,(case when eggs_promo_brown is null then 0 else eggs_promo_brown end) eggs_promo_brown ,(case when eggs_promo_nutra is null then 0 else eggs_promo_nutra end) eggs_promo_nutra ,(case when eggs_promo_liquid is null then 0 else eggs_promo_liquid end) eggs_promo_liquid ,(case when eggs_return is null then 0 else eggs_return end) eggs_return ,(case when eggs_return_white is null then 0 else eggs_return_white end) eggs_return_white ,(case when eggs_return_brown is null then 0 else eggs_return_brown end) eggs_return_brown ,(case when eggs_return_nutra is null then 0 else eggs_return_nutra end) eggs_return_nutra ,(case when eggs_return_liquid is null then 0 else eggs_return_liquid end) eggs_return_liquid from ( select (case when x.date is not null then x.date else p.date end) date ,(case when x.area_classification is not null then x.area_classification else p.area_classification end) Area_classification ,(case when x.beat_name is not null then x.beat_name else p.beat_name end) Beat_Name ,(case when x.beat_number_operations is not null then x.beat_number_operations else p.beat_number_operations end) beat_number_operations ,(case when x.beat_number_original is not null then x.beat_number_original else p.beat_number_original end) beat_number_original ,(case when x.code is not null then x.code else p.code end) code ,(case when x.classification_name is not null then x.classification_name else p.classification_name end) classification_name ,(case when x.operator is not null then x.operator else p.operator end) operator ,(case when NET_SALES is null then 0 else NET_SALES end) Net_Sales ,(case when eggs_sold is null then 0 else eggs_sold end) eggs_sold ,(case when eggs_sold_white is null then 0 else Eggs_sold_white end) Eggs_sold_white ,(case when Eggs_sold_brown is null then 0 else Eggs_sold_brown end) Eggs_sold_brown ,(case when Eggs_sold_nutra is null then 0 else Eggs_sold_nutra end) Eggs_sold_nutra ,(case when Eggs_sold_liquid is null then 0 else Eggs_sold_liquid end) Eggs_sold_liquid ,(case when collections is null then 0 else collections end) collections ,(case when eggs_replaced is null then 0 else eggs_replaced end) eggs_replaced ,(case when eggs_replaced_white is null then 0 else eggs_replaced_white end) eggs_replaced_white ,(case when eggs_replaced_brown is null then 0 else eggs_replaced_brown end) eggs_replaced_brown ,(case when eggs_replaced_nutra is null then 0 else eggs_replaced_nutra end) eggs_replaced_nutra ,(case when eggs_replaced_liquid is null then 0 else eggs_replaced_liquid end) eggs_replaced_liquid ,(case when eggs_promo is null then 0 else eggs_promo end) eggs_promo ,(case when eggs_promo_white is null then 0 else eggs_promo_white end) eggs_promo_white ,(case when eggs_promo_brown is null then 0 else eggs_promo_brown end) eggs_promo_brown ,(case when eggs_promo_nutra is null then 0 else eggs_promo_nutra end) eggs_promo_nutra ,(case when eggs_promo_liquid is null then 0 else eggs_promo_liquid end) eggs_promo_liquid from( select (case when y.date is not null then y.date else e.date end) date ,(case when y.area_classification is not null then y.area_classification else e.area_classification end) area_classification ,(case when y.beat_name is not null then y.beat_name else e.beat_name end) Beat_Name ,(case when y.beat_number_operations is not null then y.beat_number_operations else e.beat_number_operations end) beat_number_operations ,(case when y.beat_number_original is not null then y.beat_number_original else e.beat_number_original end) beat_number_original ,(case when y.code is not null then y.code else e.code end) code ,(case when y.classification_name is not null then y.classification_name else e.classification_name end) classification_name ,(case when y.operator is not null then y.operator else e.operator end) operator ,NET_SALES ,collections ,eggs_replaced ,eggs_replaced_white ,eggs_replaced_brown ,eggs_replaced_nutra ,eggs_replaced_liquid ,eggs_sold ,eggs_sold_white ,eggs_sold_brown ,eggs_sold_nutra ,eggs_sold_liquid from (select (case when z.date is not null then z.date else c.date end) date ,(case when z.area_classification is not null then z.area_classification else c.area_classification end) area_classification ,(case when z.beat_name is not null then z.beat_name else c.beat_name end) beat_name ,(case when z.beat_number_operations is not null then z.beat_number_operations else c.beat_number_operations end) beat_number_operations ,(case when z.beat_number_original is not null then z.beat_number_original else c.beat_number_original end) beat_number_original ,(case when z.code is not null then z.code else c.code end) code ,(case when z.classification_name is not null then z.classification_name else c.classification_name end) classification_name ,(case when z.operator is not null then z.operator else c.operator end) operator ,NET_SALES ,collections ,eggs_replaced ,eggs_replaced_white ,eggs_replaced_brown ,eggs_replaced_nutra ,eggs_replaced_liquid from (select (case when a.date is not null then a.date else b.date end) date ,(case when a.area_classification is not null then a.area_classification else b.area_classification end) area_classification ,(case when a.beat_name is not null then a.beat_name else b.beat_name end) beat_name ,(case when a.beat_number_operations is not null then a.beat_number_operations else b.beat_number_operations end) beat_number_operations ,(case when a.beat_number_original is not null then a.beat_number_original else b.beat_number_original end) beat_number_original ,(case when a.code is not null then a.code else b.code end) code ,(case when a.classification_name is not null then a.classification_name else b.classification_name end) classification_name ,(case when a.operator is not null then a.operator else b.operator end) operator ,NET_SALES ,collections from SALES_SUMMARY_beat_retailer_CTE a full outer join eggozdb.maplemonk.Collection_Summary_beat_retailer_DSO b on a.area_classification = b.area_classification and a.date = b.date and a.code = b.code and a.beat_name = b.beat_name and a.beat_number_operations = b.beat_number_operations and a.beat_number_original = b.beat_number_original and a.classification_name = b.classification_name and a.operator = b.operator ) z full outer join Replacement_Summary_beat_retailer_CTE c on z.area_classification = c.area_classification and z.date = c.date and z.code = c.code and z.beat_name = c.beat_name and z.beat_number_operations = c.beat_number_operations and z.beat_number_original = c.beat_number_original and z.classification_name = c.classification_name and z.operator = c.operator ) y full outer join EGGS_SOLD_SUMMARY_BEAT_RETAILER_CTE e on e.area_Classification = y.area_classification and e.date = y.date and e.code = y.code and e.beat_name = y.beat_name and e.beat_number_operations = y.beat_number_operations and e.beat_number_original = y.beat_number_original and e.classification_name = y.classification_name and e.operator = y.operator )x full outer join Promo_Summary_beat_retailer_CTE p on p.area_Classification = x.area_classification and p.date = x.date and p.code = x.code and p.beat_name = x.beat_name and p.beat_number_operations = x.beat_number_operations and p.beat_number_original = x.beat_number_original and p.classification_name = x.classification_name and p.operator = x.operator )w full outer join return_Summary_beat_retailer_CTE r on r.area_Classification = w.area_classification and r.date = w.date and r.code = w.code and r.beat_name = w.beat_name and r.beat_number_operations = w.beat_number_operations and r.beat_number_original = w.beat_number_original and r.classification_name = w.classification_name and r.operator = w.operator )u left join eggozdb.maplemonk.Pendency_beat_retailer_DSO k on u.area = k.area_classification and u.date = k.date and u.Retailer_Name = k.code and u.beat_name = k.beat_name and u.beat_number_operations = k.beat_number_operations and u.beat_number_original = k.beat_number_original and u.classification_name = k.classification_name and u.operator = k.operator )v left join eggozdb.maplemonk.my_sql_retailer_retailer mp on mp.code = v.retailer_name left join eggozdb.maplemonk.my_sql_retailer_retailerparent rp on rp.id = mp.parent_id ;",
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
                        