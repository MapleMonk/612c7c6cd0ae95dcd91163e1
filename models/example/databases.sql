{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table Almowear_db.Maplemonk.Fulfillment_Report_AW as select invoice_id, marketplace, order_status, shipping_status, try_cast(order_date as datetime) Order_Date, try_cast(manifest_date as datetime) Manifest_Date, try_cast(shipping_last_update_date as datetime) Shipping_Last_Update_Date, case when Order_Status in (\'Assigned\', \'Open\', \'Confirmed\') then \'Open\' when Order_Status in (\'Cancelled\') then \'Cancelled\' when Order_Status in (\'Returned\') then \'Returned\' when shipping_status in (\'Delivered\') then \'Delivered\' when lower(shipping_status) in (\'out for delivery\',\'in transit\', \'pickup scheduled\') then \'Intransit\' when lower(shipping_status) is not NULL then \'Other shipment Statuses\' when lower(shipping_status) is NULL and manifest_date is not NULL then \'Dispatched and yet to be picked\' when lower(shipping_status) is NULL then \'No Shipping Status\' end as status, case when status = \'Delivered\' then 1 else 0 end as delivered_order, case when status = \'Returned\' then 1 else 0 end as Returned_order, case when status = \'Cancelled\' then 1 else 0 end as cancelled_order, case when status = \'Open\' then 1 else 0 end as Open_Order, case when status = \'Intransit\' then 1 else 0 end as Intransit_Order, case when status = \'No Shipping Status\' then 1 else 0 end as shipping_status_NA, case when status = \'Other shipment Statuses\' then 1 else 0 end as Other_Order, case when status = \'Dispatched and yet to be picked\' then 1 else 0 end as dispatched_yet_to_be_picked_Order, case when delivered_order = 1 then datediff(\'hour\',try_cast(manifest_date as datetime) ,try_cast(shipping_last_update_date as datetime)) end as Delivery_Speed, case when delivered_order = 1 and datediff(\'hour\',try_cast(manifest_date as datetime) ,try_cast(shipping_last_update_date as datetime)) < 24 then \'< 24 hours\' when delivered_order = 1 and datediff(\'hour\',try_cast(manifest_date as datetime) ,try_cast(shipping_last_update_date as datetime)) < 48 then \'24-48 hours\' when delivered_order = 1 and datediff(\'hour\',try_cast(manifest_date as datetime) ,try_cast(shipping_last_update_date as datetime)) < 72 then \'48-72 hours\' when delivered_order = 1 and datediff(\'hour\',try_cast(manifest_date as datetime) ,try_cast(shipping_last_update_date as datetime)) < 96 then \'72-96 hours\' when delivered_order = 1 then \'> 96 hours\' else \'Not Delivered\' end as delivery_time_flag, datediff(\'hour\',try_cast(order_date as datetime) ,try_cast(manifest_date as datetime)) Dispatch_Speed, case when datediff(\'hour\',try_cast(order_date as datetime) ,try_cast(manifest_date as datetime)) < 2 then \'< 2 hours\' when datediff(\'hour\',try_cast(order_date as datetime) ,try_cast(manifest_date as datetime)) < 6 then \'2 - 6 hours\' when datediff(\'hour\',try_cast(order_date as datetime) ,try_cast(manifest_date as datetime)) < 12 then \'6 - 12 hours\' when datediff(\'hour\',try_cast(order_date as datetime) ,try_cast(manifest_date as datetime)) < 24 then \'12 - 24 hours\' when datediff(\'hour\',try_cast(order_date as datetime) ,try_cast(manifest_date as datetime)) < 48 then \'24-48 hours\' when datediff(\'hour\',try_cast(order_date as datetime) ,try_cast(manifest_date as datetime)) < 72 then \'48-72 hours\' when datediff(\'hour\',try_cast(order_date as datetime) ,try_cast(manifest_date as datetime)) < 96 then \'72-96 hours\' when try_cast(manifest_date as datetime) is not NULL then \'> 96 hours\' when try_cast(manifest_date as datetime) is NULL then \'Not Dispatched\' end as Dispatch_Speed_flag, case when try_cast(order_date as datetime) is NULL then \'Before Dispatch\' else \'After Dispatch\' end as dispatch_status, payment_mode, courier from Almowear_db.Maplemonk.EASYECOM_IN_CUSTOMER_ORDERS ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from ALMOWEAR_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        