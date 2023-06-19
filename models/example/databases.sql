{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table eggozdb.maplemonk.po_list as SELECT oo.name as order_name, oo.date, oo.generation_date, oo.delivery_date, oo.order_type, rr.code, oo.order_price_amount, po.po_status, ool.quantity, oo.bill_no, pp.slug, rrp.name as parent_name, rr.area_classification, rr.beat_number, case when sp.name is null then sp2.name else sp.name end as Operator FROM eggozdb.maplemonk.my_sql_order_purchaseorder po join eggozdb.maplemonk.my_sql_order_order oo on oo.id = po.order_ptr_id join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id join eggozdb.maplemonk.my_sql_order_orderline ool on oo.id = ool.order_id join eggozdb.maplemonk.my_sql_product_product pp on ool.product_id = pp.id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rr.parent_id = rrp.id LEFT JOIN (SELECT ddp.id, cau.name FROM eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ddp LEFT JOIN eggozdb.maplemonk.my_sql_custom_auth_user cau ON cau.id = ddp.user_id) sp ON sp.id = oo.jse_id LEFT JOIN (SELECT ssp.id, cau.name FROM eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp LEFT JOIN eggozdb.maplemonk.my_sql_custom_auth_user cau ON cau.id = ssp.user_id) sp2 ON sp2.id = oo.salesPerson_id ; create or replace table eggozdb.maplemonk.demand_supply_po_list as select order_name, orderid, retailer_name, parent_name, area_classification, city, beat_number, generation_date, po_date, delivery_date, po_status, ifnull(supply_status,\'not_delivered\') supply_status, po_type, po_no, po_total_amount, ifnull(supply_total_amount,0) as supply_total_amount, slug, sku, egg_type, po_quantity/(count(po_no) over (partition by po_no, sku order by po_date)) as po_quantity, ifnull(supply_quantity,0) supply_quantity, po_egg_count/(count(po_no) over (partition by po_no, sku order by po_date)) as po_egg_count, ifnull(supply_egg_count,0) supply_egg_count, ifnull(egg_fillrate,0) egg_fillrate, po_amount/(count(po_no) over (partition by po_no, sku order by po_date)) as po_amount, ifnull(supply_amount,0) supply_amount, ifnull(amount_fillrate,0) amount_fillrate from (select table1.order_name, table1.orderid, table1.code as retailer_name, table1.parent_name, table1.area_classification, table1.city_name as city, table1.beat_number, table1.generation_date, table1.po_date, table2.delivery_date, table1.po_status, table2.status as supply_status, table1.po_type, table1.bill_no as po_no, table1.order_price_amount as po_total_amount, table2.order_price_amount as supply_total_amount, table1.slug, table1.sku, table1.egg_type, table1.quantity as po_quantity, table2.quantity as supply_quantity, table1.egg_count as po_egg_count, table2.egg_count as supply_egg_count, (table2.egg_count/table1.egg_count) as egg_fillrate, table1.amount as po_amount, table2.amount as supply_amount, (table2.amount/table1.amount) as amount_fillrate from ( SELECT oo.name as order_name, oo.orderid, rr.code, cast(timestampadd(minute, 660, oo.generation_date) as date) generation_date, cast(timestampadd(minute, 660, oo.date) as date) po_date, oo.order_type, sum(oo.order_price_amount) order_price_amount, po.po_status, oo.status, po.po_type, sum(ool.quantity) quantity, pp.slug, pp.name as egg_type, concat(pp.sku_count,pp.name) as sku, pp.SKU_Count, sum(ool.quantity*pp.sku_count) egg_count, sum(ool.quantity*(ool.single_sku_rate+ool.single_sku_discount)) amount, oo.bill_no, oo.id as bill_id, rrp.name as parent_name, rr.area_classification, bc.city_name, rr.beat_number FROM eggozdb.maplemonk.my_sql_order_purchaseorder po join eggozdb.maplemonk.my_sql_order_order oo on oo.id = po.order_ptr_id join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id join eggozdb.maplemonk.my_sql_base_city bc on rr.city_id = bc.id join eggozdb.maplemonk.my_sql_order_orderline ool on oo.id = ool.order_id join eggozdb.maplemonk.my_sql_product_product pp on ool.product_id = pp.id join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rr.parent_id = rrp.id where lower(oo.order_type) = \'purchase order\' group by oo.name, oo.orderid, rr.code, oo.bill_no, oo.id, cast(timestampadd(minute, 660, oo.generation_date) as date), cast(timestampadd(minute, 660, oo.date) as date), oo.order_type, po.po_status, oo.status, po.po_type, pp.slug, pp.SKU_Count, po.purchase_id, rrp.name, rr.area_classification, bc.city_name, rr.beat_number, pp.name ) table1 left join ( SELECT oo.name as order_name, oo.orderid, cast(timestampadd(minute, 660, oo.delivery_date) as date) delivery_date, oo.order_type, rr.code, sum(oo.order_price_amount) order_price_amount, oo.status, sum(ool.quantity) quantity, pp.slug, pp.name as egg_type, concat(pp.sku_count,pp.name) as sku, pp.sku_count, sum(ool.quantity*pp.sku_count) egg_count, sum(ool.quantity*(ool.single_sku_rate + ool.single_sku_discount)) amount, oo.po_no, oo.purchase_order_id as po_id, rrp.name as parent_name, rr.area_classification, rr.beat_number from eggozdb.maplemonk.my_sql_order_order oo join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_order_orderline ool on oo.id = ool.order_id join eggozdb.maplemonk.my_sql_product_product pp on ool.product_id = pp.id join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rr.parent_id = rrp.id where lower(oo.order_type) = \'retailer\' and lower(oo.status) in (\'delivered\', \'completed\') group by oo.name, oo.orderid, oo.purchase_order_id, cast(timestampadd(minute, 660, oo.delivery_date) as date), oo.order_type, rr.code, oo.status, pp.slug, pp.sku_count, oo.po_no, rrp.name, rr.area_classification, rr.beat_number, pp.name ) table2 on table1.bill_id = table2.po_id and table1.slug = table2.slug ) ; create or replace table eggozdb.maplemonk.order_grn as select oo.name as invoice_name, cast(timestampadd(minute, 660, oo.delivery_date) as date) delivery_date, oo.status, rrp.name as parent_name, oo.order_price_amount as Billed_Amount, rr.code as retailer_name, rr.area_classification, bc.city_name, rr.beat_number, case when oog.grn_status is null then \'unavailable\' else oog.grn_status end as grn_status, concat(\'https://eggoz-backend.s3.ap-south-1.amazonaws.com/media/\',oog.grn_file) as grn_file from eggozdb.maplemonk.my_sql_order_order oo left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rr.parent_id = rrp.id left join eggozdb.maplemonk.my_sql_base_city bc on rr.city_id = bc.id left join eggozdb.maplemonk.my_sql_order_ordergrn oog on oo.id = oog.order_id where lower(oo.status) in (\'delivered\',\'completed\') ; create or replace table eggozdb.maplemonk.repeated_po as select purchase_id, count(purchase_id) po_counts from eggozdb.maplemonk.my_sql_order_purchaseorder group by purchase_id having count(purchase_id)>1 ;",
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
                        