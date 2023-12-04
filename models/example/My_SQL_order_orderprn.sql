{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.order_grn as select oo.name as invoice_name, cast(timestampadd(minute, 660, oo.delivery_date) as date) delivery_date, cast(timestampadd(minute, 660, oog.created_at) as date) grn_upload_date, oo.status, rrp.name as parent_name, oo.order_price_amount as Billed_Amount, rr.code as retailer_name, rr.area_classification, bc.city_name, rr.beat_number,db.beat_name as BEAT_NAME, case when oog.grn_status is null then \'unavailable\' else oog.grn_status end as grn_status, case when oog.grn_file = \'\' then oog.file_url else concat(\'https://eggoz-backend.s3.ap-south-1.amazonaws.com/media/\',oog.grn_file) end as grn_file from eggozdb.maplemonk.my_sql_order_order oo left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rr.parent_id = rrp.id left join eggozdb.maplemonk.my_sql_base_city bc on rr.city_id = bc.id left join eggozdb.maplemonk.my_sql_order_ordergrn oog on oo.id = oog.order_id left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment db on oo.beat_assignment_id=db.id where lower(oo.status) in (\'delivered\',\'completed\') ; create or replace table eggozdb.maplemonk.order_prn as select cast(timestampadd(minute, 660, oot.return_picked_date) as date) as Return_date,oot.prn_no,oot.deviated_Amount as Return_Amount, rrp.name as parent_name, rr.code as retailer_name, rr.area_classification, bc.city_name, rr.beat_number, case when oog.prn_status is null then \'unavailable\' else oog.prn_status end as prn_status, case when oog.prn_file = \'\' then oog.prn_file_url else concat(\'https://eggoz-backend.s3.ap-south-1.amazonaws.com/media/\',oog.prn_file) end as prn_file from eggozdb.maplemonk.my_sql_order_returnordertransaction oot left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oot.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rr.parent_id = rrp.id left join eggozdb.maplemonk.my_sql_base_city bc on rr.city_id = bc.id left join eggozdb.maplemonk.my_sql_order_orderprn oog on oot.id= oog.return_order_transaction_id ; create or replace table eggozdb.maplemonk.order_grn_actual as select oo.name as invoice_name, cast(timestampadd(minute, 660, oo.delivery_date) as date) delivery_date, oo.status, rrp.name as parent_name, oo.order_price_amount as Billed_Amount, rr.code as retailer_name, rr.area_classification, bc.city_name, rr.beat_number, case when og.status is null then \'unavailable\' else og.status end as grn_status, og.file_url grn_file from eggozdb.maplemonk.my_sql_order_order oo left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rr.parent_id = rrp.id left join eggozdb.maplemonk.my_sql_base_city bc on rr.city_id = bc.id left join eggozdb.maplemonk.my_sql_order_grn og on oo.id = og.order_id where lower(oo.status) in (\'delivered\',\'completed\') ;create or replace table eggozdb.maplemonk.order_grn as select oo.name as invoice_name, cast(timestampadd(minute, 660, oo.delivery_date) as date) delivery_date, cast(timestampadd(minute, 660, oog.created_at) as date) grn_upload_date, oo.status, rrp.name as parent_name, oo.order_price_amount as Billed_Amount, rr.code as retailer_name, rr.area_classification, bc.city_name, rr.beat_number,db.beat_name as BEAT_NAME, case when oog.grn_status is null then \'unavailable\' else oog.grn_status end as grn_status, case when oog.grn_file = \'\' then oog.file_url else concat(\'https://eggoz-backend.s3.ap-south-1.amazonaws.com/media/\',oog.grn_file) end as grn_file from eggozdb.maplemonk.my_sql_order_order oo left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rr.parent_id = rrp.id left join eggozdb.maplemonk.my_sql_base_city bc on rr.city_id = bc.id left join eggozdb.maplemonk.my_sql_order_ordergrn oog on oo.id = oog.order_id left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment db on oo.beat_assignment_id=db.id where lower(oo.status) in (\'delivered\',\'completed\') ; create or replace table eggozdb.maplemonk.order_prn as select cast(timestampadd(minute, 660, oot.return_picked_date) as date) as Return_date,oot.prn_no,oot.deviated_Amount as Return_Amount, rrp.name as parent_name, rr.code as retailer_name, rr.area_classification, bc.city_name, rr.beat_number, case when oog.prn_status is null then \'unavailable\' else oog.prn_status end as prn_status, case when oog.prn_file = \'\' then oog.prn_file_url else concat(\'https://eggoz-backend.s3.ap-south-1.amazonaws.com/media/\',oog.prn_file) end as prn_file from eggozdb.maplemonk.my_sql_order_returnordertransaction oot left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oot.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rr.parent_id = rrp.id left join eggozdb.maplemonk.my_sql_base_city bc on rr.city_id = bc.id left join eggozdb.maplemonk.my_sql_order_orderprn oog on oot.id= oog.return_order_transaction_id ; create or replace table eggozdb.maplemonk.order_grn_actual as select oo.name as invoice_name, cast(timestampadd(minute, 660, oo.delivery_date) as date) delivery_date, oo.status, rrp.name as parent_name, oo.order_price_amount as Billed_Amount, rr.code as retailer_name, rr.area_classification, bc.city_name, rr.beat_number, case when og.status is null then \'unavailable\' else og.status end as grn_status, og.file_url grn_file from eggozdb.maplemonk.my_sql_order_order oo left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rr.parent_id = rrp.id left join eggozdb.maplemonk.my_sql_base_city bc on rr.city_id = bc.id left join eggozdb.maplemonk.my_sql_order_grn og on oo.id = og.order_id where lower(oo.status) in (\'delivered\',\'completed\') ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.MAPLEMONK.My_SQL_order_orderprn
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        