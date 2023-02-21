{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.all_bills as select oo.name as bill_no, oo.bill_no as Manual_bill_no, cast(timestampadd(minute,660,oo.delivery_date) as date) as delivery_date, oo.order_price_amount + oo.discount_amount AS Bill_amount, oo.scheme_discount_amount, oo.discount_amount as customer_discount, oo.scheme_discount_amount + oo.discount_amount as total_discount, oo.po_no as po_no, rrrp.beat_number_original, rrrp.party_name as retailer_name, rrrp.area_classification as area, rrrp.city_name as city, rrrp.payment_cycle, rrrp.parent_name as parent_retailer_name, rrrp.category_id, case when sp1.name is null then sp2.name else sp1.name end as Operator, pi.invoice_status as paid_status, pi.invoice_due as pending, pps.paid_amount from eggozdb.maplemonk.my_sql_order_order oo left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment dba on oo.beat_assignment_id = dba.id left join (select rr.id, rr.beat_number as beat_number_original, rr.code as party_name, rrp.name as parent_name, rr.area_classification, bc.city_name, rrpc.number as payment_cycle, rr.category_id from eggozdb.maplemonk.my_sql_retailer_retailer rr left join eggozdb.maplemonk.my_sql_retailer_retailerpaymentcycle rrpc on rrpc.id = rr.payment_cycle_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rr.parent_id = rrp.id join eggozdb.maplemonk.my_sql_base_city bc on bc.id = rr.city_id )rrrp on rrrp.id = oo.retailer_id LEFT JOIN (SELECT ddp.id, cau.name FROM eggozdb.maplemonk.my_sql_distributionchain_distributionpersonprofile ddp LEFT JOIN eggozdb.maplemonk.my_sql_custom_auth_user cau ON cau.id = ddp.user_id) sp1 ON sp1.id = oo.distributor_id LEFT JOIN (SELECT ssp.id, cau.name FROM eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp LEFT JOIN eggozdb.maplemonk.my_sql_custom_auth_user cau ON cau.id = ssp.user_id) sp2 ON sp2.id = oo.salesPerson_id left join eggozdb.maplemonk.my_sql_payment_invoice pi on oo.id = pi.order_id left join ( SELECT sum(pp.pay_amount) as paid_amount, pp.invoice_id FROM eggozdb.maplemonk.my_sql_payment_payment pp left join eggozdb.maplemonk.my_sql_payment_salestransaction ps on pp.salesTransaction_id = ps.id where lower(ps.transaction_type) <> \'cancelled\' and ps.is_trial <> true and lower(pp.pay_choice) <> \'credit note\' group by pp.invoice_id ) pps on pps.invoice_id = pi.id where lower(oo.status) in (\'delivered\',\'completed\') and lower(oo.order_brand_type) = \'branded\' and oo.is_trial <> TRUE and cast(timestampadd(minute,660,oo.delivery_date) as date) >= \'2021-06-01\' ; create or replace table eggozdb.maplemonk.all_bills_sku as select oo.name as bill_no, oo.bill_no as Manual_bill_no, cast(timestampadd(minute,660,oo.delivery_date) as date) as delivery_date, oo.order_price_amount + oo.discount_amount AS Bill_amount, oo.scheme_discount_amount, oo.discount_amount as customer_discount, oo.scheme_discount_amount + oo.discount_amount as total_discount, ool.quantity * (ool.single_sku_rate + ool.single_sku_discount) AS sku_gross_sales_amount, CASE WHEN pp.name LIKE \'%liquid%\' THEN ool.quantity * 1000 / 35 ELSE ool.quantity * CASE WHEN pp.SKU_Count = 1 THEN CASE WHEN rrrp.area_classification = \'UP-UB\' THEN 1 ELSE 30 END ELSE pp.SKU_Count END END AS \"Eggs Sold\", concat(pp.sku_count, left(pp.name,1)) SKU, oo.order_brand_type, pp.slug, oo.po_no as po_no, rrrp.beat_number_original, rrrp.party_name as retailer_name, rrrp.area_classification as area, rrrp.city_name as city, rrrp.payment_cycle, rrrp.parent_name as parent_retailer_name, rrrp.category_id, case when sp1.name is null then sp2.name else sp1.name end as Operator, pi.invoice_status as paid_status, pi.invoice_due as pending, pps.paid_amount from eggozdb.maplemonk.my_sql_order_order oo left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment dba on oo.beat_assignment_id = dba.id left join (select rr.id, rr.beat_number as beat_number_original, rr.code as party_name, rrp.name as parent_name, rr.area_classification, bc.city_name, rr.category_id, rrpc.number as payment_cycle from eggozdb.maplemonk.my_sql_retailer_retailer rr left join eggozdb.maplemonk.my_sql_retailer_retailerpaymentcycle rrpc on rrpc.id = rr.payment_cycle_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rr.parent_id = rrp.id join eggozdb.maplemonk.my_sql_base_city bc on bc.id = rr.city_id ) rrrp on rrrp.id = oo.retailer_id LEFT JOIN (SELECT ddp.id, cau.name FROM eggozdb.maplemonk.my_sql_distributionchain_distributionpersonprofile ddp LEFT JOIN eggozdb.maplemonk.my_sql_custom_auth_user cau ON cau.id = ddp.user_id) sp1 ON sp1.id = oo.distributor_id LEFT JOIN (SELECT ssp.id, cau.name FROM eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp LEFT JOIN eggozdb.maplemonk.my_sql_custom_auth_user cau ON cau.id = ssp.user_id) sp2 ON sp2.id = oo.salesPerson_id left join eggozdb.maplemonk.my_sql_payment_invoice pi on oo.id = pi.order_id left join ( SELECT sum(pp.pay_amount) as paid_amount, pp.invoice_id FROM eggozdb.maplemonk.my_sql_payment_payment pp left join eggozdb.maplemonk.my_sql_payment_salestransaction ps on pp.salesTransaction_id = ps.id where lower(ps.transaction_type) <> \'cancelled\' and ps.is_trial <> true and lower(pp.pay_choice) <> \'credit note\' group by pp.invoice_id ) pps on pps.invoice_id = pi.id left join eggozdb.maplemonk.my_sql_order_orderline ool on ool.order_id = oo.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = ool.product_id where lower(oo.status) in (\'delivered\',\'completed\') and lower(oo.order_brand_type) = \'branded\' and oo.is_trial <> TRUE and cast(timestampadd(minute,660,oo.delivery_date) as date) >= \'2022-04-01\' ; create or replace table eggozdb.maplemonk.unbranded_bills as select bl.*, pps.* from ( SELECT tt.* FROM (SELECT rr.beat_number as original_beat_number, dba.beat_number as oper_beat_number, cast(timestampadd(minute,660,oo.delivery_date) as date) as delivery_date, oo.is_trial, oo.order_brand_type, rr.code AS Party_name, rrpc.number as payment_cycle, oo.order_price_amount + oo.discount_amount AS Bill_amount, oo.scheme_discount_amount, oo.discount_amount as customer_discount, oo.scheme_discount_amount + oo.discount_amount as total_discount, oo.name AS Bill_no, oo.bill_no as Manual_bill_no, pi.invoice_due AS Pending, pi.invoice_status AS Paid_status, rr.area_classification, bc.city_name as city, oo.status, oo.po_no, pi.id AS invoiceid, cast(timestampadd(minute,660,pi.modified_at) as date) as invoice_date, oo.distributor_id, rrp.name as Parent, oo.id as order_id, case when sp.name is null then sp2.name else sp.name end as Operator FROM eggozdb.maplemonk.my_sql_order_order oo LEFT JOIN eggozdb.maplemonk.my_sql_retailer_retailer rr ON rr.id = oo.retailer_id LEFT JOIN eggozdb.maplemonk.my_sql_distributionchain_beatassignment dba ON oo.beat_assignment_id = dba.id JOIN eggozdb.maplemonk.my_sql_base_city bc ON rr.city_id = bc.id LEFT JOIN eggozdb.maplemonk.my_sql_retailer_retailerpaymentcycle rrpc on rrpc.id = rr.payment_cycle_id LEFT JOIN eggozdb.maplemonk.my_sql_payment_invoice pi ON oo.id = pi.order_id LEFT JOIN eggozdb.maplemonk.my_sql_retailer_retailerparent rrp ON rr.parent_id = rrp.id LEFT JOIN (SELECT ddp.id, cau.name FROM eggozdb.maplemonk.my_sql_distributionchain_distributionpersonprofile ddp LEFT JOIN eggozdb.maplemonk.my_sql_custom_auth_user cau ON cau.id = ddp.user_id) sp ON sp.id = oo.distributor_id LEFT JOIN (SELECT ssp.id, cau.name FROM eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp LEFT JOIN eggozdb.maplemonk.my_sql_custom_auth_user cau ON cau.id = ssp.user_id) sp2 ON sp2.id = oo.salesPerson_id) tt WHERE tt.delivery_date between \'2022-02-01\' and getdate() AND lower(tt.Paid_status) <> \'cancelled\' AND tt.is_trial <> TRUE AND tt.order_brand_type = \'unbranded\' and lower(tt.status) <> \'cancelled\' ) bl left join ( SELECT sum(pp.pay_amount) as Instant_amount, pp.invoice_id FROM eggozdb.maplemonk.my_sql_payment_payment pp left join eggozdb.maplemonk.my_sql_payment_salestransaction ps on pp.salesTransaction_id = ps.id where lower(ps.transaction_type) <> \'cancelled\' and ps.is_trial <> true group by pp.invoice_id ) pps on pps.invoice_id = bl.invoiceid ; create or replace table eggozdb.maplemonk.returns_with_bill_no as select cast(timestampadd(minute,660,or1.date) as date) Date, line_type ,rr.area_classification ,ba.beat_name ,ba.beat_number beat_number_operations ,rr.beat_number beat_number_original ,rr.code ,oo.name as bill_no ,(case when pp.name like \'%White%\' then \'White\' when pp.name like \'%Brown%\' then \'Brown\' when pp.name like \'%Nutra%\' then \'Nutra\' else \'Liquid\' end) Egg_Name ,concat(pp.sku_count,left(pp.name,1)) SKU ,sum(or1.quantity* pp.sku_count) Eggs_return_replaced_promo ,sum(amount) amount from eggozdb.maplemonk.my_sql_order_orderreturnline or1 left join eggozdb.maplemonk.my_sql_retailer_retailer rr ON or1.retailer_id =rr.id left join eggozdb.maplemonk.my_sql_order_orderline ool on ool.id = or1.orderLine_id left join eggozdb.maplemonk.my_sql_order_order oo on oo.id = ool.order_id left JOIN eggozdb.maplemonk.my_sql_product_product pp on pp.id = or1.product_id left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment ba on ba.id = or1.beat_assignment_id where line_type in (\'Return\') group by rr.area_classification, date(timestampadd(minute,660,or1.date)) ,(case when pp.name like \'%White%\' then \'White\' when pp.name like \'%Brown%\' then \'Brown\' when pp.name like \'%Nutra%\' then \'Nutra\' else \'Liquid\' end),ba.beat_name,ba.beat_number, rr.beat_number, line_type, oo.name ,rr.code, concat(pp.sku_count,left(pp.name,1)) ; create or replace table eggozdb.maplemonk.collection_log as select tt.*, sum(tt.count) over (partition by tt.area_classification) as grp_count, sum(tt.amount) over (partition by tt.area_classification) as grp_amount, tt.count*100/(sum(tt.count) over (partition by tt.area_classification)) count_ratio, tt.amount*100/(sum(tt.amount) over (partition by tt.area_classification)) amount_ration from (select rr.area_classification, pp.payment_mode, count(pp.id) count, sum(pp.pay_amount) amount from eggozdb.maplemonk.my_sql_payment_payment pp join eggozdb.maplemonk.my_sql_payment_invoice pi on pi.id = pp.invoice_id join eggozdb.maplemonk.my_sql_order_order oo on pi.order_id = oo.id join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id join eggozdb.maplemonk.my_sql_payment_salestransaction ps on ps.id = pp.salesTransaction_id where lower(pi.invoice_status) <> \'cancelled\' and lower(ps.transaction_type) <> \'cancelled\' and cast(timestampadd(minute, 660, ps.transaction_date) as date) between TO_DATE(DATE_TRUNC(\'month\', dateadd(\'day\',-1,cast(timestampadd(minute,660,current_date()) as date)))) and cast(timestampadd(minute, 660, getdate()) as date) and lower(ps.transaction_type) in (\'adjusted\', \'credit\') group by pp.payment_mode, rr.area_classification ) tt ; create or replace table eggozdb.maplemonk.finance_closing as select *, debit-credit-credit_note-adjusted-debit_note-disputed-return-waive_off as closing from ( select retailer_name, area_classification, parent_name, distributor, retailer_id, case when distributor is null then case when category_id = 3 then \'distributor\' else \'non-distributor\' end else \'moved_to_distributor\' end as retailer_type, ifnull(\"\'Debit\'\",0) as debit, ifnull(\"\'Credit\'\",0) as credit, ifnull(\"\'Credit Note\'\",0) as credit_note, ifnull(\"\'Adjusted\'\",0) as adjusted, ifnull(\"\'Debit Note\'\",0) as debit_note, ifnull(\"\'Disputed\'\",0) as disputed, ifnull(\"\'Return\'\",0) as return, ifnull(\"\'WaveOff\'\",0) as waive_off from ( select rr.code as retailer_name, transaction_type, sum(transaction_amount) as transaction_amount, rr.area_classification, rrp.name as parent_name, rr2.code as distributor, rr.category_id, rr.id as retailer_id from eggozdb.maplemonk.my_sql_payment_salestransaction ps left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = ps.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr2 on rr2.id = rr.distributor_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rrp.id = rr.parent_id where rr.code is not null and lower(transaction_type) not in (\'cancelled\',\'promo\',\'replacement\',\'loss\',\'draft\') and ps.is_trial = 0 and year(cast(timestampadd(minute, 660, ps.transaction_date) as date)) >=2020 and cast(timestampadd(minute, 660, ps.transaction_date) as date)<=cast(timestampadd(minute, 660, getdate()) as date) group by rr.code, transaction_type, rr.area_classification, rrp.name, rr2.code, rr.category_id, rr.id ) pivot(sum(transaction_amount) for transaction_type in (\'Debit\',\'Credit\',\'Credit Note\',\'Adjusted\',\'Debit Note\',\'Disputed\',\'Return\',\'WaveOff\')) p (retailer_name,area_classification,parent_name,distributor,category_id,retailer_id) ) ; create or replace table eggozdb.maplemonk.finance_debtor_movement as select dm.*, fc.closing from (select transaction_date, retailer_name, area_classification, parent_name, distributor, retailer_id, case when distributor is null then case when category_id = 3 then \'distributor\' else \'non-distributor\' end else \'moved_to_distributor\' end as retailer_type, ifnull(\"\'Debit\'\",0) as debit, ifnull(\"\'Credit\'\",0) as credit, ifnull(\"\'Credit Note\'\",0) as credit_note, ifnull(\"\'Adjusted\'\",0) as adjusted, ifnull(\"\'Debit Note\'\",0) as debit_note, ifnull(\"\'Disputed\'\",0) as disputed, ifnull(\"\'Return\'\",0) as return, ifnull(\"\'WaveOff\'\",0) as waive_off from ( select cast(timestampadd(minute, 660, ps.transaction_date) as date) as transaction_date, rr.code as retailer_name, transaction_type, sum(transaction_amount) as transaction_amount, rr.area_classification, rrp.name as parent_name, rr2.code as distributor, rr.category_id, rr.id as retailer_id from eggozdb.maplemonk.my_sql_payment_salestransaction ps left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = ps.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr2 on rr2.id = rr.distributor_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rrp.id = rr.parent_id where rr.code is not null and lower(transaction_type) not in (\'cancelled\',\'promo\',\'replacement\',\'loss\',\'draft\') and ps.is_trial = 0 and year(cast(timestampadd(minute, 660, ps.transaction_date) as date)) >=2020 and cast(timestampadd(minute, 660, ps.transaction_date) as date)<=cast(timestampadd(minute, 660, getdate()) as date) group by cast(timestampadd(minute, 660, ps.transaction_date) as date), rr.code, transaction_type, rr.area_classification, rrp.name, rr2.code, rr.category_id, rr.id ) pivot(sum(transaction_amount) for transaction_type in (\'Debit\',\'Credit\',\'Credit Note\',\'Adjusted\',\'Debit Note\',\'Disputed\',\'Return\',\'WaveOff\')) p (transaction_date,retailer_name,area_classification,parent_name,distributor,category_id,retailer_id) ) dm left join eggozdb.maplemonk.finance_closing fc on fc.retailer_id = dm.retailer_id ;",
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
                        