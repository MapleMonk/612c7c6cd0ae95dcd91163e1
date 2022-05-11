{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.all_bills as select bl.*, pps.* from ( SELECT tt.* FROM (SELECT rr.beat_number as original_beat_number, dba.beat_number as oper_beat_number, oo.delivery_date, oo.is_trial, oo.order_brand_type, rr.code AS Party_name, oo.order_price_amount AS Bill_amount, oo.name AS Bill_no, oo.bill_no as Manual_bill_no, oo.scheme_discount_amount, pi.invoice_due AS Pending, pi.invoice_status AS Paid_status, rr.area_classification, bc.city_name, oo.status, oo.po_no, pi.id AS invoiceid, pi.modified_at as invoice_date, oo.distributor_id, rrp.name as Parent, oo.id as order_id, case when sp.name is null then sp2.name else sp.name end as Operator FROM eggozdb.maplemonk.my_sql_order_order oo LEFT JOIN eggozdb.maplemonk.my_sql_retailer_retailer rr ON rr.id = oo.retailer_id LEFT JOIN eggozdb.maplemonk.my_sql_distributionchain_beatassignment dba ON oo.beat_assignment_id = dba.id JOIN eggozdb.maplemonk.my_sql_base_city bc ON rr.city_id = bc.id LEFT JOIN eggozdb.maplemonk.my_sql_payment_invoice pi ON oo.id = pi.order_id LEFT JOIN eggozdb.maplemonk.my_sql_retailer_retailerparent rrp ON rr.parent_id = rrp.id LEFT JOIN (SELECT ddp.id, cau.name FROM eggozdb.maplemonk.my_sql_distributionchain_distributionpersonprofile ddp LEFT JOIN eggozdb.maplemonk.my_sql_custom_auth_user cau ON cau.id = ddp.user_id) sp ON sp.id = oo.distributor_id LEFT JOIN (SELECT ssp.id, cau.name FROM eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp LEFT JOIN eggozdb.maplemonk.my_sql_custom_auth_user cau ON cau.id = ssp.user_id) sp2 ON sp2.id = oo.salesPerson_id) tt WHERE (tt.delivery_date BETWEEN \'2021-05-31 18:30:00 \' AND getdate()) AND tt.Paid_status <> \'Cancelled\' AND tt.is_trial <> TRUE AND tt.order_brand_type = \'branded\' ) bl left join ( SELECT sum(pp.pay_amount) as Instant_amount, pp.invoice_id FROM eggozdb.maplemonk.my_sql_payment_payment pp left join eggozdb.maplemonk.my_sql_payment_salestransaction ps on pp.salesTransaction_id = ps.id where ps.transaction_type <> \'Cancelled\' and ps.is_trial <> true and pp.pay_choice <> \'Credit Note\' group by pp.invoice_id ) pps on pps.invoice_id = bl.invoiceid",
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
                        