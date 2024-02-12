{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE eggozdb.maplemonk.order_grn as select oo.name as invoice_name, cast(timestampadd(minute, 660, oo.delivery_date) as date) delivery_date,concat(pp.SKU_Count,pp.short_name) as sku, cast(timestampadd(minute, 660, oog.created_at) as date) grn_upload_date, cast(timestampadd(minute, 660, db.beat_date) as date) beat_date, oo.status, oo.bill_no,oo.id as order_id, oo.po_no, rrp.name as parent_name, oo.order_price_amount as Billed_Amount, rr.code as retailer_name, rr.area_classification, bc.city_name, rr.beat_number, db.beat_name as BEAT_NAME, oog.grn_status as grn_status, case when oog.file_url is null then concat(\'https://eggoz-backend.s3.ap-south-1.amazonaws.com/media/\',oog.grn_file) else oog.file_url end as grn_file, oog.file_url, cau.name as sales_person from eggozdb.maplemonk.my_sql_order_order oo left join eggozdb.maplemonk.my_sql_order_orderline ol on oo.id=ol.order_id left join eggozdb.maplemonk.my_sql_product_product pp on ol.product_id=pp.id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rr.parent_id = rrp.id left join eggozdb.maplemonk.my_sql_base_city bc on rr.city_id = bc.id left join eggozdb.maplemonk.my_sql_order_ordergrn oog on oo.id = oog.order_id left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment db on oo.beat_assignment_id=db.id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp on ssp.id = oo.salesperson_id left join eggozdb.maplemonk.my_sql_custom_auth_user cau on cau.id = ssp.user_id where lower(oo.status) in (\'delivered\',\'completed\') ; CREATE OR REPLACE TABLE eggozdb.maplemonk.order_prn as select cast(timestampadd(minute, 660, oot.return_picked_date) as date) as Return_date, cast(timestampadd(minute, 660, oog.created_at) as date) prn_upload_date, oot.prn_no,oot.deviated_Amount as Return_Amount, rrp.name as parent_name, rr.code as retailer_name, rr.area_classification, bc.city_name, rr.beat_number, oog.prn_status as prn_status, case when oog.prn_file_url is null then concat(\'https://eggoz-backend.s3.ap-south-1.amazonaws.com/media/\',oog.prn_file) else oog.prn_file_url end as prn_file, cau.name as sales_person from eggozdb.maplemonk.my_sql_order_returnordertransaction oot left join eggozdb.maplemonk.my_sql_order_orderprn oog on oot.id= oog.return_order_transaction_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oot.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rr.parent_id = rrp.id left join eggozdb.maplemonk.my_sql_base_city bc on rr.city_id = bc.id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp on ssp.id = oot.salesperson_id left join eggozdb.maplemonk.my_sql_custom_auth_user cau on cau.id = ssp.user_id ; CREATE OR REPLACE TABLE eggozdb.maplemonk.order_grn_actual as select oo.name as invoice_name, cast(timestampadd(minute, 660, oo.delivery_date) as date) as delivery_date, oo.status, cast(timestampadd(minute, 660, og.created_at) as date) as grn_upload_date, rrp.name as parent_name, oo.order_price_amount as Billed_Amount, rr.code as retailer_name, rr.area_classification, bc.city_name,og.grn_amount, rr.beat_number, og.status as grn_status, og.file_url, ol.quantity, ogl.quantity as grn_quantity, pp.description, concat(pp.SKU_Count,pp.short_name) as SKU, pp.SKU_Count, (ol.quantity+ol.single_sku_discount)*ol.single_sku_rate as Single_SKU_rate, oo.po_no, oo.bill_no, cau.name as sales_person from eggozdb.maplemonk.my_sql_order_order oo left join eggozdb.maplemonk.my_sql_order_orderline ol on oo.id=ol.order_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rr.parent_id = rrp.id left join eggozdb.maplemonk.my_sql_base_city bc on rr.city_id = bc.id left join eggozdb.maplemonk.my_sql_order_grn og on oo.id = og.order_id left join eggozdb.maplemonk.my_sql_order_grnline ogl on og.id=ogl.grn_id left join eggozdb.maplemonk.my_sql_product_product pp on ol.product_id=pp.id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp on ssp.id = oo.salesperson_id left join eggozdb.maplemonk.my_sql_custom_auth_user cau on cau.id = ssp.user_id where lower(oo.status) in (\'delivered\',\'completed\')",
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
                        