{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.po_list as SELECT oo.name, oo.date, oo.generation_date, oo.delivery_date, oo.order_type, rr.code, oo.order_price_amount, po.po_status, ool.quantity, oo.bill_no, pp.slug, rrp.name, rr.area_classification, rr.beat_number, case when sp.name is null then sp2.name else sp.name end as Operator FROM eggozdb.order_purchaseorder po join order_order oo on oo.id = po.order_ptr_id join retailer_retailer rr on rr.id = oo.retailer_id join order_orderline ool on oo.id = ool.order_id join product_product pp on ool.product_id = pp.id left join retailer_retailerparent rrp on rr.parent_id = rrp.id LEFT JOIN (SELECT ddp.id, cau.name FROM distributionchain_distributionpersonprofile ddp LEFT JOIN custom_auth_user cau ON cau.id = ddp.user_id) sp ON sp.id = oo.distributor_id LEFT JOIN (SELECT ssp.id, cau.name FROM saleschain_salespersonprofile ssp LEFT JOIN custom_auth_user cau ON cau.id = ssp.user_id) sp2 ON sp2.id = oo.salesPerson_id;",
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
                        