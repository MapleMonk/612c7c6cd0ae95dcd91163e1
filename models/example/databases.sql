{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table Almowear_db.Maplemonk.Fulfillment_Report_AW as select invoice_id, a.marketplace, warehouse_name, order_status, shipping_status, order_date::timestamp Order_Date, manifest_date Manifest_Date, shipping_last_update_date Shipping_Last_Update_Date, case when reference_code is NULL then 1 else 0 end as not_synced_with_easy_ecom_flag, case when reference_code is NULL then \'Not Synced With EasyEcom\' when lower(Order_Status) in (\'assigned\', \'open\', \'confirmed\') then \'Open\' when lower(Order_Status) in (\'cancelled\') then \'Cancelled\' when lower(Order_Status) in (\'returned\') then \'Returned\' when lower(shipping_status) in (\'delivered\') then \'Delivered\' when lower(shipping_status) in (\'out for delivery\',\'in transit\', \'pickup scheduled\') then \'In Transit\' when lower(shipping_status) is not NULL then \'Other Shipment Statuses\' when lower(shipping_status) is NULL and manifest_date is not NULL then \'Dispatched and yet to be picked\' when lower(shipping_status) is NULL then \'No Shipping Status\' end as status, case when status = \'Not Synced With EasyEcom\' then 1 else 0 end as Not_Synced_Order, case when status = \'Delivered\' then 1 else 0 end as delivered_order, case when status = \'Returned\' then 1 else 0 end as Returned_order, case when status = \'Cancelled\' then 1 else 0 end as cancelled_order, case when status = \'Open\' then 1 else 0 end as Open_Order, case when status = \'In Transit\' then 1 else 0 end as Intransit_Order, case when status = \'No Shipping Status\' then 1 else 0 end as shipping_status_NA, case when status = \'Other shipment Statuses\' then 1 else 0 end as Other_Order, case when status = \'Dispatched and yet to be picked\' then 1 else 0 end as dispatched_yet_to_be_picked_Order, case when delivered_order = 1 then datediff(\'hour\',manifest_date,shipping_last_update_date ) end as Delivery_Speed, case when delivered_order = 1 and datediff(\'hour\',manifest_date,shipping_last_update_date ) < 24 then \'< 24 hours\' when delivered_order = 1 and datediff(\'hour\',manifest_date,shipping_last_update_date ) < 48 then \'24-48 hours\' when delivered_order = 1 and datediff(\'hour\',manifest_date,shipping_last_update_date ) < 72 then \'48-72 hours\' when delivered_order = 1 and datediff(\'hour\',manifest_date,shipping_last_update_date ) < 96 then \'72-96 hours\' when delivered_order = 1 then \'> 96 hours\' else \'Not Delivered\' end as delivery_time_flag, case when not_synced_with_easy_ecom_flag = 1 then NULL when delivered_order = 1 then datediff(\'hour\',manifest_date ,shipping_last_update_date ) else datediff(\'hour\',manifest_date ,coalesce(order_manifest_date,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp()) ) end as deliver_time, datediff(\'hour\',order_date::timestamp ,manifest_date) Dispatch_Speed, case when datediff(\'hour\',order_date::timestamp ,manifest_date) < 2 then \'< 2 hours\' when datediff(\'hour\',order_date::timestamp ,manifest_date) < 6 then \'2 - 6 hours\' when datediff(\'hour\',order_date::timestamp ,manifest_date) < 12 then \'6 - 12 hours\' when datediff(\'hour\',order_date::timestamp ,manifest_date) < 24 then \'12 - 24 hours\' when datediff(\'hour\',order_date::timestamp ,manifest_date) < 48 then \'24-48 hours\' when datediff(\'hour\',order_date::timestamp ,manifest_date) < 72 then \'48-72 hours\' when datediff(\'hour\',order_date::timestamp ,manifest_date) < 96 then \'72-96 hours\' when manifest_date is not NULL then \'> 96 hours\' when manifest_date is NULL then \'Not Dispatched\' end as Dispatch_Speed_flag, case when manifest_date is null then datediff(\'hour\',order_date::timestamp ,coalesce(order_manifest_date,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp()) ) else datediff(\'hour\',order_date::timestamp ,manifest_date) end as dispatch_time, case when not_synced_with_easy_ecom_flag = 1 then \'Order Not available in Easy Ecom\' when manifest_date is NULL then \'Before Dispatch\' else \'After Dispatch\' end as dispatch_status, payment_mode, courier , greatest(case when hour(a.import_date)< hour(try_cast(same_day_cut_off as time)) then datediff(\'day\',a.import_date ,manifest_date) else datediff(\'day\',a.import_date ,manifest_date) -1 end,0) as fulfillment_flag from ALMOWEAR_DB.maplemonk.sales_consolidated_AW a left join almo_marketplace_fulfillment_cutoff b on b.marketplace = case when a.marketplace in (select distinct marketplace from almo_marketplace_fulfillment_cutoff) then a.marketplace else \'Others\' end; create or replace table Almowear_db.Maplemonk.Order_Fulfillment_Report_AW as select order_id, reference_code, a.marketplace, warehouse_name, order_date::timestamp Order_Date, same_day_cut_off, max(import_date) Order_Import_Date, max(manifest_date) Order_Manifest_Date, max(shipping_last_update_date ) Order_Shipping_Last_Update_Date, max(case when reference_code is NULL then 1 else 0 end) as not_synced_with_easy_ecom_flag, max(case when reference_code is NULL then \'Not Synced With EasyEcom\' when lower(Order_Status) in (\'assigned\', \'open\', \'confirmed\') then \'Open\' when lower(Order_Status) in (\'cancelled\') then \'Cancelled\' when lower(Order_Status) in (\'returned\') then \'Returned\' when lower(shipping_status) in (\'delivered\') then \'Delivered\' when lower(shipping_status) in (\'out for delivery\',\'in transit\', \'pickup scheduled\') then \'In Transit\' when lower(shipping_status) is not NULL then \'Other Shipment Statuses\' when lower(shipping_status) is NULL and manifest_date is not NULL then \'Dispatched and yet to be picked\' when lower(shipping_status) is NULL then \'No Shipping Status\' end) as status, case when status = \'Not Synced With EasyEcom\' then 1 else 0 end as Not_Synced_Order, case when status = \'Open\' then 1 else 0 end as Open_Order, case when status = \'Cancelled\' then 1 else 0 end as cancelled_order, case when status = \'Returned\' then 1 else 0 end as Returned_order, case when status = \'Delivered\' then 1 else 0 end as delivered_order, case when status = \'In Transit\' then 1 else 0 end as Intransit_Order, case when status = \'Other shipment Statuses\' then 1 else 0 end as Other_Order, case when status = \'Dispatched and yet to be picked\' then 1 else 0 end as dispatched_yet_to_be_picked_Order, case when status = \'No Shipping Status\' then 1 else 0 end as shipping_status_NA, case when delivered_order = 1 then datediff(\'hour\',order_date::timestamp ,Order_Shipping_Last_Update_Date ) else datediff(\'hour\',order_date::timestamp ,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())) end as Time_From_Order, datediff(\'hour\',order_date::timestamp ,order_manifest_date) Dispatch_Speed, case when Dispatch_Speed < 2 then \'< 2 hours\' when Dispatch_Speed < 6 then \'2 - 6 hours\' when Dispatch_Speed < 12 then \'6 - 12 hours\' when Dispatch_Speed < 24 then \'12 - 24 hours\' when Dispatch_Speed < 48 then \'24-48 hours\' when Dispatch_Speed < 72 then \'48-72 hours\' when Dispatch_Speed < 96 then \'72-96 hours\' when order_manifest_date is not NULL then \'> 96 hours\' when order_manifest_date is NULL then \'Not Dispatched\' end as Dispatch_Speed_flag, greatest(case when hour(Order_Import_Date) < hour(try_cast(same_day_cut_off as time)) then datediff(\'day\',Order_Import_Date ,coalesce(order_manifest_date,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp()) )) else datediff(\'day\',Order_Import_Date ,coalesce(order_manifest_date,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp()) )) -1 end,0) as Almo_Dispatch_Flag, Case when Almo_Dispatch_Flag=0 then \'Same Day\' when Almo_Dispatch_Flag=1 then \'Next Day\' else \'2+ Days\' end as Final_Dispatch_Bucket, case when delivered_order = 1 then datediff(\'hour\',order_manifest_date ,order_shipping_last_update_date ) end as Delivery_Speed, case when not_synced_with_easy_ecom_flag = 1 then NULL when delivered_order = 1 then datediff(\'hour\',order_manifest_date ,order_shipping_last_update_date ) else datediff(\'hour\',order_manifest_date ,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())) end as deliver_time, case when delivered_order = 1 and datediff(\'hour\',order_manifest_date,order_shipping_last_update_date ) < 24 then \'< 24 hours\' when delivered_order = 1 and datediff(\'hour\',order_manifest_date,order_shipping_last_update_date ) < 48 then \'24-48 hours\' when delivered_order = 1 and datediff(\'hour\',order_manifest_date,order_shipping_last_update_date ) < 72 then \'48-72 hours\' when delivered_order = 1 and datediff(\'hour\',order_manifest_date,order_shipping_last_update_date ) < 96 then \'72-96 hours\' when delivered_order = 1 then \'> 96 hours\' else \'Not Delivered\' end as delivery_time_flag, case when not_synced_with_easy_ecom_flag = 1 then \'Order Not available in Easy Ecom\' when order_manifest_date is NULL then \'Before Dispatch\' else \'After Dispatch\' end as dispatch_status from ALMOWEAR_DB.maplemonk.sales_consolidated_AW a left join almo_marketplace_fulfillment_cutoff b on b.marketplace = case when a.marketplace in (select distinct marketplace from almo_marketplace_fulfillment_cutoff) then a.marketplace else \'Others\' end group by order_id, warehouse_name, reference_code, a.marketplace, Order_Date, same_day_cut_off;",
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
                        