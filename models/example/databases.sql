{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table eggozdb.maplemonk.po_list as SELECT oo.name as order_name, oo.date, oo.generation_date, oo.delivery_date, oo.order_type, rr.code, oo.order_price_amount, po.po_status, ool.quantity, oo.bill_no, pp.slug, rrp.name as parent_name, rr.area_classification, rr.beat_number, case when sp.name is null then sp2.name else sp.name end as Operator FROM eggozdb.maplemonk.my_sql_order_purchaseorder po join eggozdb.maplemonk.my_sql_order_order oo on oo.id = po.order_ptr_id join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id join eggozdb.maplemonk.my_sql_order_orderline ool on oo.id = ool.order_id join eggozdb.maplemonk.my_sql_product_product pp on ool.product_id = pp.id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rr.parent_id = rrp.id LEFT JOIN (SELECT ddp.id, cau.name FROM eggozdb.maplemonk.my_sql_distributionchain_distributionpersonprofile ddp LEFT JOIN eggozdb.maplemonk.my_sql_custom_auth_user cau ON cau.id = ddp.user_id) sp ON sp.id = oo.distributor_id LEFT JOIN (SELECT ssp.id, cau.name FROM eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp LEFT JOIN eggozdb.maplemonk.my_sql_custom_auth_user cau ON cau.id = ssp.user_id) sp2 ON sp2.id = oo.salesPerson_id ; create or replace table eggozdb.maplemonk.demand_supply_po_list as select table1.order_name, table1.code as retailer_name, table1.parent_name, table1.area_classification, table1.beat_number, table1.generation_date, table1.po_date, table2.delivery_date, table1.po_status, table2.status as supply_status, table1.bill_no, table1.order_price_amount as po_total_amount, table2.order_price_amount as supply_total_amount, table1.slug, table1.quantity as po_quantity, table2.quantity as supply_quantity, table1.egg_count as po_egg_count, table2.egg_count as supply_egg_count, table1.amount as po_amount, table2.amount as supply_amount from ( SELECT oo.name as order_name, rr.code, cast(timestampadd(minute, 660, oo.generation_date) as date) generation_date, cast(timestampadd(minute, 660, oo.date) as date) po_date, oo.order_type, sum(oo.order_price_amount) order_price_amount, po.po_status, oo.status, sum(ool.quantity) quantity, pp.slug, pp.SKU_Count, sum(ool.quantity*pp.sku_count) egg_count, sum(ool.quantity*(ool.single_sku_rate+ool.single_sku_discount)) amount, po.purchase_id as bill_no, rrp.name as parent_name, rr.area_classification, rr.beat_number FROM eggozdb.maplemonk.my_sql_order_purchaseorder po join eggozdb.maplemonk.my_sql_order_order oo on oo.id = po.order_ptr_id join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id join eggozdb.maplemonk.my_sql_order_orderline ool on oo.id = ool.order_id join eggozdb.maplemonk.my_sql_product_product pp on ool.product_id = pp.id join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rr.parent_id = rrp.id group by oo.name, rr.code, cast(timestampadd(minute, 660, oo.generation_date) as date), cast(timestampadd(minute, 660, oo.date) as date), oo.order_type, po.po_status, oo.status, pp.slug, pp.SKU_Count, po.purchase_id, rrp.name, rr.area_classification, rr.beat_number ) table1 left join ( SELECT oo.name as order_name, cast(timestampadd(minute, 660, oo.delivery_date) as date) delivery_date, oo.order_type, rr.code, sum(oo.order_price_amount) order_price_amount, oo.status, sum(ool.quantity) quantity, pp.slug, pp.sku_count, sum(ool.quantity*pp.sku_count) egg_count, sum(ool.quantity*(ool.single_sku_rate+ool.single_sku_discount)) amount, oo.po_no, rrp.name as parent_name, rr.area_classification, rr.beat_number from eggozdb.maplemonk.my_sql_order_order oo join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id join eggozdb.maplemonk.my_sql_order_orderline ool on oo.id = ool.order_id join eggozdb.maplemonk.my_sql_product_product pp on ool.product_id = pp.id join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rr.parent_id = rrp.id where oo.po_no in (select purchase_id from eggozdb.maplemonk.my_sql_order_purchaseorder) and lower(oo.order_type) = \'retailer\' and lower(oo.status) <> \'cancelled\' group by oo.name, cast(timestampadd(minute, 660, oo.delivery_date) as date), oo.order_type, rr.code, oo.status, pp.slug, pp.sku_count, oo.po_no, rrp.name, rr.area_classification, rr.beat_number ) table2 on table1.bill_no = table2.po_no and table1.slug = table2.slug ;",
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
                        