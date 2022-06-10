{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.missing_manual_bills as select rr.code as retailer_name, rrp.name as parent_name, oo.name as bill_no, oo.status, cast(timestampadd(minute,660,oo.delivery_date) as date) as delivery_date, oo.order_price_amount, oo.scheme_discount_amount, oo.discount_amount as customer_discount, rr.area_classification, oo.bill_no as manual_bill_no, oo.order_type from eggozdb.maplemonk.my_sql_order_order oo join eggozdb.maplemonk.my_sql_retailer_retailer rr on oo.retailer_id = rr.id join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rrp.id = rr.parent_id where lower(oo.status) in (\'delivered\',\'completed\') having ifnull(oo.bill_no,\'\')=\'\' ; create or replace table eggozdb.maplemonk.po_not_punched as SELECT rr.code as retailer_name, rrp.name as parent_name, oo.po_no, oo.order_type, cast(timestampadd(minute,660,oo.delivery_date) as date) as delivery_date, rr.area_classification FROM eggozdb.maplemonk.my_sql_order_order oo LEFT JOIN eggozdb.maplemonk.my_sql_order_purchaseorder op ON oo.po_no = op.purchase_id join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rrp.id = rr.parent_id where op.purchase_id is null and rr.area_classification in (\'NCR-MT\', \'UP-MT\', \'Bangalore-MT\', \'East-MT\', \'MP-MT\') having ifnull(oo.po_no,\'\')<>\'\' ; create or replace table eggozdb.maplemonk.sales_without_po as select rr.code as retailer_name, rrp.name as parent_name, oo.name as bill_no, oo.status, cast(timestampadd(minute,660,oo.delivery_date) as date) as delivery_date, oo.order_price_amount, oo.scheme_discount_amount, oo.discount_amount as customer_discount, oo.bill_no as manual_bill_no, rr.area_classification, oo.po_no from eggozdb.maplemonk.my_sql_order_order oo join eggozdb.maplemonk.my_sql_retailer_retailer rr on oo.retailer_id = rr.id join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rrp.id = rr.parent_id where lower(oo.order_type) = \'retailer\' and lower(oo.status) in (\'delivered\') and rr.area_classification in (\'NCR-MT\', \'UP-MT\', \'Bangalore-MT\', \'East-MT\', \'MP-MT\') having ifnull(oo.po_no,\'\')=\'\' ;",
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
                        