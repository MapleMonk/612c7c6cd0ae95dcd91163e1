{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table RPSG_DB.Maplemonk.Order_Fulfillment_Report_RPSG as select order_id ,invoice_id ,line_item_id ,reference_code ,a.marketplace ,warehouse_name ,order_date ,same_day_cut_off ,Import_Date as Order_Import_Date ,max(Manifest_Date) as Order_Manifest_date ,easyecom_sync_flag ,shipping_last_update_date as Order_Shipping_Last_Update_Date ,shipping_status ,order_status ,COURIER ,channel ,payment_mode ,case when easyecom_sync_flag=\'Synced\' then 0 else 1 end as not_synced_with_easy_ecom_flag, max(case when lower(Order_Status) in (\'ready to dispatch\') then am.\"Mapped Status\" when lower(shipping_status) =\'shipment created\' and lower(order_status) in (\'shipped\') then am.\"Mapped Status\" when lower(shipping_status) in (\'out for pickup\',\'rto undelivered\',\'shipment lost\',\'pickup scheduled\',\'undelivered\',\'handover\',\'delivered to origin\',\'shipment created\',\'rto initiated\',\'in transit\',\'pickup rescheduled\',\'shipment error\',\'out for delivery\',\'rto in-transit\',\'delivered\',\'shipment created\' ) then pm.\"Mapped Status\" when lower(Order_Status) in (\'assigned\', \'open\', \'confirmed\',\'cancelled\',\'printed\',\'shopify_processed\',\'ready to dispatch\',\'on hold\',\'returned\') then am.\"Mapped Status\" when lower(shipping_status) is NULL and lower(order_status) in (\'shipped\') then am.\"Mapped Status\" end) as final_status, case when final_status = \'Delivered\' then 1 else 0 end as delivered_order, case when final_status = \'Order Yet To sync\' then 1 else 0 end as Order_yet_to_Sync_Order, case when final_status = \'Confirmed\' then 1 else 0 end as Confirmed_Order, case when final_status = \'Assigned\' then 1 else 0 end as Assigned_Order, case when final_status = \'Open\' then 1 else 0 end as Open_Order, case when final_status = \'Printed\' then 1 else 0 end as Printed_Order, case when final_status = \'Pending\' then 1 else 0 end as Pending_Order, case when final_status = \'In Transit\' then 1 else 0 end as In_Transit_order, case when final_status = \'RTO\' then 1 else 0 end as RTO_Order, case when final_status = \'RTS\' then 1 else 0 end as RTS_Order, case when final_status = \'Returned\' then 1 else 0 end as Returned_order, case when final_status = \'Pickup Error\' then 1 else 0 end as Pickup_Error, case when final_status = \'Lost\' then 1 else 0 end as Lost_Order, case when final_status = \'Exception\' then 1 else 0 end as Exception_Order, case when final_status = \'Misrouted\' then 1 else 0 end as Misrouted_Order, case when final_status = \'Underprocess\' then 1 else 0 end as Underprocess_Order, case when final_status = \'Damaged\' then 1 else 0 end as Damaged_Order, case when final_status = \'Cancelled\' then 1 else 0 end as cancelled_order, case when delivered_order = 1 then datediff(\'hour\',order_date::timestamp ,Order_Shipping_Last_Update_Date ) else datediff(\'hour\',order_date::timestamp ,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())) end as Time_From_Order, case when Time_from_Order <= 24 then \'1. <24 hours\' when Time_from_Order <= 48 then \'2. 25 - 48 hours\' when Time_from_Order <= 72 then \'3. 49 - 72 hours\' when Time_from_Order <= 96 then \'4. 73 - 96 hours\' when Time_from_Order <= 120 then \'5. 97 - 120 hours\' when Time_from_Order <= 144 then \'6. 121 - 144 hours\' when Time_from_Order <= 144 then \'7. 145 - 168 hours\' else \'8. More than a week\' end as Time_From_Order_Category, datediff(\'hour\',order_date::timestamp ,order_manifest_date) Dispatch_Speed, case when Dispatch_Speed < 2 then \'0- 2 hours\' when Dispatch_Speed < 6 then \'2 - 6 hours\' when Dispatch_Speed < 12 then \'6 - 12 hours\' when Dispatch_Speed < 24 then \'12 - 24 hours\' when Dispatch_Speed < 48 then \'24-48 hours\' when Dispatch_Speed < 72 then \'48-72 hours\' when Dispatch_Speed < 96 then \'72-96 hours\' when order_manifest_date is not NULL then \'> 96 hours\' when order_manifest_date is NULL then \'Not Dispatched\' end as Dispatch_Speed_flag, greatest(case when hour(Order_Import_Date) < hour(try_cast(same_day_cut_off as time)) then datediff(\'day\',Order_Import_Date ,coalesce(order_manifest_date,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp()) )) else datediff(\'day\',Order_Import_Date ,coalesce(order_manifest_date,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp()) )) -1 end,0) as Drv_Dispatch_Flag, Case when Drv_Dispatch_Flag=0 then \'Same Day\' when Drv_Dispatch_Flag=1 then \'Next Day\' else \'2+ Days\' end as Final_Dispatch_Bucket, case when delivered_order = 1 then datediff(\'hour\',order_manifest_date , order_shipping_last_update_date ) end as Delivery_Speed, case when not_synced_with_easy_ecom_flag = 1 then NULL when delivered_order = 1 then datediff(\'hour\',order_manifest_date ,order_shipping_last_update_date ) else datediff(\'hour\',order_manifest_date ,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())) end as deliver_time, case when delivered_order = 1 and datediff(\'hour\',order_manifest_date,order_shipping_last_update_date ) < 24 then \'< 24 hours\' when delivered_order = 1 and datediff(\'hour\',order_manifest_date,order_shipping_last_update_date ) < 48 then \'24-48 hours\' when delivered_order = 1 and datediff(\'hour\',order_manifest_date,order_shipping_last_update_date ) < 72 then \'48-72 hours\' when delivered_order = 1 and datediff(\'hour\',order_manifest_date,order_shipping_last_update_date ) < 96 then \'72-96 hours\' when delivered_order = 1 then \'> 96 hours\' else \'Not Delivered\' end as delivery_time_flag, case when not_synced_with_easy_ecom_flag = 1 then \'Order Not available in Easy Ecom\' when order_manifest_date is NULL then \'Before Dispatch\' else \'After Dispatch\' end as dispatch_status from rpsg_db.maplemonk.sales_consolidated_drv a left join rpsg_db.maplemonk.marketplace_fulfilment_cutoff b on b.marketplace = case when a.marketplace in (select distinct m.marketplace from rpsg_db.maplemonk.marketplace_fulfilment_cutoff m) then a.marketplace else \'Others\' end left join rpsg_db.maplemonk.shipment_status_mapping am on lower(a.order_status) = lower(am.status) left join rpsg_db.maplemonk.shipment_status_mapping pm on lower(a.shipping_status)= lower(pm.status) group by order_id ,invoice_id ,line_item_id ,reference_code ,a.marketplace ,warehouse_name ,order_date ,same_day_cut_off ,Import_Date ,easyecom_sync_flag ,Order_Shipping_Last_Update_Date ,shipping_status ,order_status ,COURIER ,channel ,payment_mode;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from RPSG_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        