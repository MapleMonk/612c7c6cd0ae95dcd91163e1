{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table eggozdb.maplemonk.po_list as SELECT oo.name as order_name, oo.date, oo.generation_date, oo.delivery_date, oo.order_type, rr.code, oo.order_price_amount, po.po_status, ool.quantity, oo.bill_no, pp.slug, rrp.name as parent_name, rr.area_classification, rr.beat_number, case when sp.name is null then sp2.name else sp.name end as Operator FROM eggozdb.maplemonk.my_sql_order_purchaseorder po join eggozdb.maplemonk.my_sql_order_order oo on oo.id = po.order_ptr_id join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id join eggozdb.maplemonk.my_sql_order_orderline ool on oo.id = ool.order_id join eggozdb.maplemonk.my_sql_product_product pp on ool.product_id = pp.id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rr.parent_id = rrp.id LEFT JOIN (SELECT ddp.id, cau.name FROM eggozdb.maplemonk.my_sql_distributionchain_distributionpersonprofile ddp LEFT JOIN eggozdb.maplemonk.my_sql_custom_auth_user cau ON cau.id = ddp.user_id) sp ON sp.id = oo.distributor_id LEFT JOIN (SELECT ssp.id, cau.name FROM eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp LEFT JOIN eggozdb.maplemonk.my_sql_custom_auth_user cau ON cau.id = ssp.user_id) sp2 ON sp2.id = oo.salesPerson_id ; create or replace table eggozdb.maplemonk.demand_supply_po_list as select case when ll.po_no is null then ll.bill_no else ll.po_no end as purchase_id, concat(pp.sku_count,pp.name) as sku, ool.quantity, ool.single_sku_rate, pp.SKU_Count*ool.quantity as eggs_count, pp.SKU_Count*ool.single_sku_rate as amount, ool.order_id, ool.single_sku_mrp, ll.name as order_name, ll.order_type, cast(timestampadd(minute,330,ll.date) as date) as date, cast(timestampadd(minute,330,ll.generation_date) as date) as generation_date, cast(timestampadd(minute,330,ll.delivery_date) as date) as delivery_date, ll.order_price_amount, ll.retailer_id, ll.salesperson_id, ll.distributor_id, ll.beat_assignment_id, ll.bill_no, ll.po_no, ll.scheme_discount_amount, bc.city_name, rr.area_classification, rr.code as retailer_name, rr.beat_number as beat_original, db.beat_number as beat_operational, case when sp.name is null then sp2.name else sp.name end as Operator, rrp.name as parent_name from eggozdb.maplemonk.my_sql_order_orderline ool join (select * from eggozdb.maplemonk.my_sql_order_order where po_no in (select po.purchase_id from eggozdb.maplemonk.my_sql_order_order oo join eggozdb.maplemonk.my_sql_order_purchaseorder po on oo.id = po.order_ptr_id where order_type in (\'Purchase Order\')) or bill_no in (select po.purchase_id from eggozdb.maplemonk.my_sql_order_order oo join eggozdb.maplemonk.my_sql_order_purchaseorder po on oo.id = po.order_ptr_id where order_type in (\'Purchase Order\'))) ll on ll.id = ool.order_id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = ool.product_id LEFT JOIN (SELECT ddp.id, cau.name FROM eggozdb.maplemonk.my_sql_distributionchain_distributionpersonprofile ddp LEFT JOIN eggozdb.maplemonk.my_sql_custom_auth_user cau ON cau.id = ddp.user_id) sp ON sp.id = ll.distributor_id LEFT JOIN (SELECT ssp.id, cau.name FROM eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp LEFT JOIN eggozdb.maplemonk.my_sql_custom_auth_user cau ON cau.id = ssp.user_id) sp2 ON sp2.id = ll.salesPerson_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = ll.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rr.parent_id = rrp.id join eggozdb.maplemonk.my_sql_base_city bc on bc.id = rr.city_id left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment db on db.id = ll.beat_assignment_id",
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
                        