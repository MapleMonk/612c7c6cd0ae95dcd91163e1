{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table Perfora_DB.Maplemonk.Order_Fulfillment_Report_Perfora as select order_id ,invoice_id ,reference_code ,a.shop_name marketplace ,warehouse_name ,order_date ,same_day_cut_off ,max(DISPATCH_DATE) as Order_Manifest_date ,shipping_last_update_date as Order_Shipping_Last_Update_Date ,shipping_status ,order_status ,COURIER ,Source ,payment_mode ,max(c.\"Mapped Status\") final_status ,case when final_status = \'Delivered\' then 1 else 0 end as delivered_order, case when final_status = \'Order Yet To sync\' then 1 else 0 end as Order_yet_to_Sync_Order, case when final_status = \'Confirmed\' then 1 else 0 end as Confirmed_Order, case when final_status = \'Assigned\' then 1 else 0 end as Assigned_Order, case when final_status = \'Open\' then 1 else 0 end as Open_Order, case when final_status = \'Printed\' then 1 else 0 end as Printed_Order, case when final_status = \'Pending\' then 1 else 0 end as Pending_Order, case when final_status = \'In Transit\' then 1 else 0 end as In_Transit_order, case when final_status = \'RTO\' then 1 else 0 end as RTO_Order, case when final_status = \'RTS\' then 1 else 0 end as RTS_Order, case when final_status = \'Returned\' then 1 else 0 end as Returned_order, case when final_status = \'Pickup Error\' then 1 else 0 end as Pickup_Error, case when final_status = \'Lost\' then 1 else 0 end as Lost_Order, case when final_status = \'Exception\' then 1 else 0 end as Exception_Order, case when final_status = \'Misrouted\' then 1 else 0 end as Misrouted_Order, case when final_status = \'Underprocess\' then 1 else 0 end as Underprocess_Order, case when final_status = \'Damaged\' then 1 else 0 end as Damaged_Order, case when final_status = \'Cancelled\' then 1 else 0 end as cancelled_order, case when delivered_order = 1 then datediff(\'hour\',order_date::timestamp ,Order_Shipping_Last_Update_Date::timestamp ) else datediff(\'hour\',order_date::timestamp ,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp) end as Time_From_Order, case when Time_from_Order <= 24 then \'1. <=24 hours\' when Time_from_Order <= 48 then \'2. 25 - 48 hours\' when Time_from_Order <= 72 then \'3. 49 - 72 hours\' when Time_from_Order <= 96 then \'4. 73 - 96 hours\' when Time_from_Order <= 120 then \'5. 97 - 120 hours\' when Time_from_Order <= 144 then \'6. 121 - 144 hours\' when Time_from_Order <= 144 then \'7. 145 - 168 hours\' else \'8. More than a week\' end as Time_From_Order_Category, datediff(\'hour\',order_date::timestamp ,order_manifest_date::timestamp) Dispatch_Speed, case when Dispatch_Speed < 2 then \'0- 2 hours\' when Dispatch_Speed < 6 then \'2 - 6 hours\' when Dispatch_Speed < 12 then \'6 - 12 hours\' when Dispatch_Speed < 24 then \'12 - 24 hours\' when Dispatch_Speed < 48 then \'24-48 hours\' when Dispatch_Speed < 72 then \'48-72 hours\' when Dispatch_Speed < 96 then \'72-96 hours\' when order_manifest_date is not NULL then \'> 96 hours\' when order_manifest_date is NULL then \'Not Dispatched\' end as Dispatch_Speed_flag, greatest(case when hour(order_date::timestamp) < hour(try_cast(same_day_cut_off as time)) then datediff(\'day\',order_date::timestamp ,coalesce(order_manifest_date::timestamp,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp )) else datediff(\'day\',order_date::timestamp ,coalesce(order_manifest_date::timestamp,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp )) -1 end,0) as Dispatch_Flag, Case when Dispatch_Flag=0 then \'Same Day\' when Dispatch_Flag=1 then \'Next Day\' else \'2+ Days\' end as Final_Dispatch_Bucket, case when delivered_order = 1 then datediff(\'hour\',order_manifest_date::timestamp , order_shipping_last_update_date::timestamp ) end as Delivery_Speed, case when delivered_order = 1 then datediff(\'hour\',order_manifest_date::timestamp ,order_shipping_last_update_date::timestamp ) else datediff(\'hour\',order_manifest_date::timestamp ,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp) end as deliver_time, case when delivered_order = 1 and datediff(\'hour\',order_manifest_date::timestamp,order_shipping_last_update_date::timestamp ) < 24 then \'< 24 hours\' when delivered_order = 1 and datediff(\'hour\',order_manifest_date::timestamp,order_shipping_last_update_date::timestamp ) < 48 then \'24-48 hours\' when delivered_order = 1 and datediff(\'hour\',order_manifest_date::timestamp,order_shipping_last_update_date::timestamp ) < 72 then \'48-72 hours\' when delivered_order = 1 and datediff(\'hour\',order_manifest_date::timestamp,order_shipping_last_update_date::timestamp ) < 96 then \'72-96 hours\' when delivered_order = 1 then \'> 96 hours\' else \'Not Delivered\' end as delivery_time_flag, case when order_manifest_date is NULL then \'Before Dispatch\' else \'After Dispatch\' end as dispatch_status from Perfora_db.maplemonk.perfora_db_sales_consolidated a left join Perfora_db.maplemonk.marketplace_fulfilment_cutoff b on b.marketplace = case when a.shop_name in (select distinct m.marketplace from Perfora_db.maplemonk.marketplace_fulfilment_cutoff m) then a.shop_name else \'Others\' end left join (select * from ( select *, row_number() over (partition by lower(status) order by 1) rw from Perfora_db.maplemonk.Shipment_Status_Mapping) where rw=1) c on lower(c.status) = lower(coalesce(a.shipping_status,a.order_status)) group by order_id ,invoice_id ,reference_code ,a.shop_name ,warehouse_name ,order_date ,same_day_cut_off ,Order_Shipping_Last_Update_Date ,shipping_status ,order_status ,COURIER ,Source ,payment_mode;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from Perfora_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        