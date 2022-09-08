{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.all_bills as select oo.name as bill_no, oo.bill_no as Manual_bill_no, cast(timestampadd(minute,660,oo.delivery_date) as date) as delivery_date, oo.order_price_amount + oo.discount_amount AS Bill_amount, oo.scheme_discount_amount, oo.discount_amount as customer_discount, oo.scheme_discount_amount + oo.discount_amount as total_discount, oo.po_no as po_no, rrrp.beat_number_original, rrrp.party_name as retailer_name, rrrp.area_classification as area, rrrp.city_name, rrrp.payment_cycle, rrrp.parent_name as parent_retailer_name, case when sp1.name is null then sp2.name else sp1.name end as Operator, pi.invoice_status as paid_status, pi.invoice_due as pending, pps.paid_amount from eggozdb.maplemonk.my_sql_order_order oo left join (select rr.id, rr.beat_number as beat_number_original, rr.code as party_name, rrp.name as parent_name, rr.area_classification, bc.city_name, rrpc.number as payment_cycle from eggozdb.maplemonk.my_sql_retailer_retailer rr left join eggozdb.maplemonk.my_sql_retailer_retailerpaymentcycle rrpc on rrpc.id = rr.payment_cycle_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rr.parent_id = rrp.id join eggozdb.maplemonk.my_sql_base_city bc on bc.id = rr.city_id )rrrp on rrrp.id = oo.retailer_id LEFT JOIN (SELECT ddp.id, cau.name FROM eggozdb.maplemonk.my_sql_distributionchain_distributionpersonprofile ddp LEFT JOIN eggozdb.maplemonk.my_sql_custom_auth_user cau ON cau.id = ddp.user_id) sp1 ON sp1.id = oo.distributor_id LEFT JOIN (SELECT ssp.id, cau.name FROM eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp LEFT JOIN eggozdb.maplemonk.my_sql_custom_auth_user cau ON cau.id = ssp.user_id) sp2 ON sp2.id = oo.salesPerson_id left join eggozdb.maplemonk.my_sql_payment_invoice pi on oo.id = pi.order_id left join ( SELECT sum(pp.pay_amount) as paid_amount, pp.invoice_id FROM eggozdb.maplemonk.my_sql_payment_payment pp left join eggozdb.maplemonk.my_sql_payment_salestransaction ps on pp.salesTransaction_id = ps.id where lower(ps.transaction_type) <> \'cancelled\' and ps.is_trial <> true and lower(pp.pay_choice) <> \'credit note\' group by pp.invoice_id ) pps on pps.invoice_id = pi.id where lower(oo.status) in (\'delivered\',\'completed\') and lower(oo.order_brand_type) = \'branded\' and oo.is_trial <> TRUE ; create or replace table eggozdb.maplemonk.unbranded_bills as select bl.*, pps.* from ( SELECT tt.* FROM (SELECT rr.beat_number as original_beat_number, dba.beat_number as oper_beat_number, cast(timestampadd(minute,660,oo.delivery_date) as date) as delivery_date, oo.is_trial, oo.order_brand_type, rr.code AS Party_name, oo.order_price_amount + oo.discount_amount AS Bill_amount, oo.scheme_discount_amount, oo.discount_amount as customer_discount, oo.scheme_discount_amount + oo.discount_amount as total_discount, oo.name AS Bill_no, oo.bill_no as Manual_bill_no, pi.invoice_due AS Pending, pi.invoice_status AS Paid_status, rr.area_classification, bc.city_name, oo.status, oo.po_no, pi.id AS invoiceid, cast(timestampadd(minute,660,pi.modified_at) as date) as invoice_date, oo.distributor_id, rrp.name as Parent, oo.id as order_id, case when sp.name is null then sp2.name else sp.name end as Operator FROM eggozdb.maplemonk.my_sql_order_order oo LEFT JOIN eggozdb.maplemonk.my_sql_retailer_retailer rr ON rr.id = oo.retailer_id LEFT JOIN eggozdb.maplemonk.my_sql_distributionchain_beatassignment dba ON oo.beat_assignment_id = dba.id JOIN eggozdb.maplemonk.my_sql_base_city bc ON rr.city_id = bc.id LEFT JOIN eggozdb.maplemonk.my_sql_payment_invoice pi ON oo.id = pi.order_id LEFT JOIN eggozdb.maplemonk.my_sql_retailer_retailerparent rrp ON rr.parent_id = rrp.id LEFT JOIN (SELECT ddp.id, cau.name FROM eggozdb.maplemonk.my_sql_distributionchain_distributionpersonprofile ddp LEFT JOIN eggozdb.maplemonk.my_sql_custom_auth_user cau ON cau.id = ddp.user_id) sp ON sp.id = oo.distributor_id LEFT JOIN (SELECT ssp.id, cau.name FROM eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp LEFT JOIN eggozdb.maplemonk.my_sql_custom_auth_user cau ON cau.id = ssp.user_id) sp2 ON sp2.id = oo.salesPerson_id) tt WHERE tt.delivery_date between \'2021-06-01\' and getdate() AND tt.Paid_status <> \'cancelled\' AND tt.is_trial <> TRUE AND tt.order_brand_type = \'unbranded\' and tt.status <> \'cancelled\' ) bl left join ( SELECT sum(pp.pay_amount) as Instant_amount, pp.invoice_id FROM eggozdb.maplemonk.my_sql_payment_payment pp left join eggozdb.maplemonk.my_sql_payment_salestransaction ps on pp.salesTransaction_id = ps.id where ps.transaction_type <> \'cancelled\' and ps.is_trial <> true group by pp.invoice_id ) pps on pps.invoice_id = bl.invoiceid ; create or replace table eggozdb.maplemonk.returns_with_bill_no as select cast(timestampadd(minute,660,or1.date) as date) Date, line_type ,rr.area_classification ,ba.beat_name ,ba.beat_number beat_number_operations ,rr.beat_number beat_number_original ,rr.code ,oo.name as bill_no ,(case when pp.name like \'%White%\' then \'White\' when pp.name like \'%Brown%\' then \'Brown\' when pp.name like \'%Nutra%\' then \'Nutra\' else \'Liquid\' end) Egg_Name ,concat(pp.sku_count,left(pp.name,1)) SKU ,sum(or1.quantity* pp.sku_count) Eggs_return_replaced_promo ,sum(amount) amount from eggozdb.maplemonk.my_sql_order_orderreturnline or1 left join eggozdb.maplemonk.my_sql_retailer_retailer rr ON or1.retailer_id =rr.id left join eggozdb.maplemonk.my_sql_order_orderline ool on ool.id = or1.orderLine_id left join eggozdb.maplemonk.my_sql_order_order oo on oo.id = ool.order_id left JOIN eggozdb.maplemonk.my_sql_product_product pp on pp.id = or1.product_id left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment ba on ba.id = or1.beat_assignment_id where line_type in (\'Return\') group by rr.area_classification, date(timestampadd(minute,660,or1.date)) ,(case when pp.name like \'%White%\' then \'White\' when pp.name like \'%Brown%\' then \'Brown\' when pp.name like \'%Nutra%\' then \'Nutra\' else \'Liquid\' end),ba.beat_name,ba.beat_number, rr.beat_number, line_type, oo.name ,rr.code, concat(pp.sku_count,left(pp.name,1))",
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
                        