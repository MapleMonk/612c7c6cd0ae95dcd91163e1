{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table hilodesign_db.Maplemonk.Order_Fulfillment_Report_HILO as select invoice_id, reference_code as Shopify_id, courier, payment_mode, marketplace, warehouse_name, order_status, shipping_status, order_date::timestamp Order_Date, manifest_date Manifest_Date, shipping_last_update_date Shipping_Last_Update_Date, datediff(\'hour\',order_date,current_timestamp()) as time_from_order_placement, case when lower(Order_Status) in (\'assigned\', \'open\', \'confirmed\') then \'Open\' when lower(Order_Status) in (\'cancelled\') then \'Cancelled\' when lower(Order_Status) in (\'returned\') then \'Returned\' when lower(shipping_status) in (\'delivered\') then \'Delivered\' when lower(shipping_status) in (\'out for delivery\',\'in transit\', \'pickup scheduled\') then \'In Transit\' when lower(shipping_status) is not NULL then \'Other Shipment Statuses\' when lower(shipping_status) is NULL and manifest_date is not NULL then \'Dispatched and yet to be picked\' when lower(shipping_status) is NULL then \'No Shipping Status\' end as status, case when lower(status) in (\'open\') and time_from_order_placement > 96 then 1 else 0 end as Delayed_Dispatch, case when status = \'Delivered\' then 1 else 0 end as delivered_order, case when status = \'Returned\' then 1 else 0 end as Returned_order, case when status = \'Cancelled\' then 1 else 0 end as cancelled_order, case when status = \'Open\' then 1 else 0 end as Open_Order, case when status = \'In Transit\' then 1 else 0 end as Intransit_Order, case when status = \'No Shipping Status\' then 1 else 0 end as shipping_status_NA, case when status = \'Other shipment Statuses\' then 1 else 0 end as Other_Order, case when status = \'Dispatched and yet to be picked\' then 1 else 0 end as dispatched_yet_to_be_picked_Order, case when delivered_order = 1 then datediff(\'hour\',manifest_date,shipping_last_update_date ) end as Delivery_Speed, case when delivered_order = 1 and datediff(\'hour\',manifest_date,shipping_last_update_date ) < 24 then \'< 1 day\' when delivered_order = 1 and datediff(\'hour\',manifest_date,shipping_last_update_date ) < 48 then \'1-2 days\' when delivered_order = 1 and datediff(\'hour\',manifest_date,shipping_last_update_date ) < 72 then \'2-3 days\' when delivered_order = 1 and datediff(\'hour\',manifest_date,shipping_last_update_date ) < 96 then \'3-4 days\' when delivered_order = 1 then \'> 4 days\' else \'Not Delivered\' end as delivery_time_flag, case when delivered_order = 1 then datediff(\'hour\',manifest_date ,shipping_last_update_date ) else datediff(\'hour\',manifest_date ,current_timestamp() ) end as deliver_time, datediff(\'hour\',order_date::timestamp ,manifest_date) Dispatch_Speed, case when datediff(\'hour\',order_date::timestamp ,manifest_date) < 24 then \'< 1 day\' when datediff(\'hour\',order_date::timestamp ,manifest_date) < 48 then \'1-2 days\' when datediff(\'hour\',order_date::timestamp ,manifest_date) < 72 then \'2-3 days\' when datediff(\'hour\',order_date::timestamp ,manifest_date) < 96 then \'3-4 days\' when manifest_date is not NULL then \'> 4 days\' when manifest_date is NULL then \'Not Dispatched\' end as Dispatch_Speed_flag, case when manifest_date is null then datediff(\'hour\',order_date::timestamp ,current_timestamp() ) else datediff(\'hour\',order_date::timestamp ,manifest_date) end as dispatch_time, case when manifest_date is NULL then \'Before Dispatch\' else \'After Dispatch\' end as dispatch_status from hilodesign_db.maplemonk.easy_ecom_consolidated_hilo",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from HILODESIGN_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        