{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create table if not exists ras_db.MAPLEMONK.marketplace_fulfillment_cutoff (Marketplace varchar,Same_Day_Cut_Off varchar); Insert into ras_db.MAPLEMONK.marketplace_fulfillment_cutoff values (\'Myntra PPMP\',\'11:00 AM\'), (\'Flipkart\',\'12:00 PM\'), (\'Amazon\',\'2:00 PM\'),(\'Others\',\'4:00 PM\'); create or replace table ras_db.MAPLEMONK.ras_db_Order_Fulfillment_Report as select order_id ,a.marketplace Final_Marketplace ,reference_code ,a.shop_name marketplace ,warehouse warehouse_name ,order_date ,same_day_cut_off ,max(DISPATCH_DATE) as Order_Manifest_date ,shipping_last_update_date as Order_Shipping_Last_Update_Date ,upper(shipping_status) shipping_status ,upper(order_status) order_status ,COURIER ,Source ,payment_mode ,upper(coalesce(FINAL_SHIPPING_STATUS,shipping_status,order_status)) final_status ,case when upper(final_status) in (\'DELIVERED\') then 1 else 0 end as delivered_order ,case when upper(final_status) = \'ORDER YET TO SYNC\' then 1 else 0 end as Order_yet_to_Sync_Order ,case when upper(final_status) = \'CONFIRMED\' then 1 else 0 end as Confirmed_Order ,case when upper(final_status) = \'ASSIGNED\' then 1 else 0 end as Assigned_Order ,case when upper(final_status) = \'OPEN\' then 1 else 0 end as Open_Order ,case when upper(final_status) = \'PRINTED\' then 1 else 0 end as Printed_Order ,case when upper(final_status) = \'PENDING\' then 1 else 0 end as Pending_Order ,case when upper(final_status) = \'IN TRANSIT\' then 1 else 0 end as In_Transit_order ,case when upper(final_status) = \'RTO\' then 1 else 0 end as RTO_Order ,case when upper(final_status) = \'RTS\' then 1 else 0 end as RTS_Order ,case when upper(final_status) = \'RETURNED\' then 1 else 0 end as Returned_order ,case when upper(final_status) = \'PICKUP ERROR\' then 1 else 0 end as Pickup_Error ,case when upper(final_status) = \'LOST\' then 1 else 0 end as Lost_Order ,case when upper(final_status) =\'EXCEPTION\' then 1 else 0 end as Exception_Order ,case when upper(final_status) = \'MISROUTED\' then 1 else 0 end as Misrouted_Order ,case when upper(final_status) = \'UNDERPROCESS\' then 1 else 0 end as Underprocess_Order ,case when upper(final_status) = \'DAMAGED\' then 1 else 0 end as Damaged_Order ,case when upper(final_status) = \'CANCELLED\' then 1 else 0 end as cancelled_order ,case when delivered_order = 1 then datediff(\'hour\',order_date::timestamp ,Order_Shipping_Last_Update_Date::timestamp ) else datediff(\'hour\',order_date::timestamp ,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp) end as Time_From_Order ,case when Time_from_Order <= 24 then \'1. <=24 hours\' when Time_from_Order <= 48 then \'2. 25 - 48 hours\' when Time_from_Order <= 72 then \'3. 49 - 72 hours\' when Time_from_Order <= 96 then \'4. 73 - 96 hours\' when Time_from_Order <= 120 then \'5. 97 - 120 hours\' when Time_from_Order <= 144 then \'6. 121 - 144 hours\' when Time_from_Order <= 144 then \'7. 145 - 168 hours\' else \'8. More than a week\' end as Time_From_Order_Category, datediff(\'hour\',order_date::timestamp ,order_manifest_date::timestamp) Dispatch_Speed ,case when Dispatch_Speed < 2 then \'0- 2 hours\' when Dispatch_Speed < 6 then \'2 - 6 hours\' when Dispatch_Speed < 12 then \'6 - 12 hours\' when Dispatch_Speed < 24 then \'12 - 24 hours\' when Dispatch_Speed < 48 then \'24-48 hours\' when Dispatch_Speed < 72 then \'48-72 hours\' when Dispatch_Speed < 96 then \'72-96 hours\' when order_manifest_date is not NULL then \'> 96 hours\' when order_manifest_date is NULL then \'Not Dispatched\' end as Dispatch_Speed_flag ,greatest(case when hour(order_date::timestamp) < hour(try_cast(same_day_cut_off as time)) then datediff(\'day\',order_date::timestamp ,coalesce(order_manifest_date::timestamp,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp )) else datediff(\'day\',order_date::timestamp ,coalesce(order_manifest_date::timestamp,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp )) -1 end,0) as Dispatch_Flag ,Case when Dispatch_Flag=0 then \'Same Day\' when Dispatch_Flag=1 then \'Next Day\' else \'2+ Days\' end as Final_Dispatch_Bucket ,case when delivered_order = 1 then datediff(\'hour\',order_manifest_date::timestamp , order_shipping_last_update_date::timestamp ) end as Delivery_Speed ,case when delivered_order = 1 then datediff(\'hour\',order_manifest_date::timestamp ,order_shipping_last_update_date::timestamp ) else datediff(\'hour\',order_manifest_date::timestamp ,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp) end as deliver_time ,case when delivered_order = 1 and datediff(\'hour\',order_manifest_date::timestamp,order_shipping_last_update_date::timestamp ) < 24 then \'< 24 hours\' when delivered_order = 1 and datediff(\'hour\',order_manifest_date::timestamp,order_shipping_last_update_date::timestamp ) < 48 then \'24-48 hours\' when delivered_order = 1 and datediff(\'hour\',order_manifest_date::timestamp,order_shipping_last_update_date::timestamp ) < 72 then \'48-72 hours\' when delivered_order = 1 and datediff(\'hour\',order_manifest_date::timestamp,order_shipping_last_update_date::timestamp ) < 96 then \'72-96 hours\' when delivered_order = 1 then \'> 96 hours\' else \'Not Delivered\' end as delivery_time_flag ,case when order_manifest_date is NULL then \'Before Dispatch\' else \'After Dispatch\' end as dispatch_status ,brand from ras_db.MAPLEMONK.ras_db_sales_consolidated a left join ras_db.MAPLEMONK.marketplace_fulfillment_cutoff b on b.marketplace = case when a.shop_name in (select distinct m.marketplace from ras_db.MAPLEMONK.marketplace_fulfillment_cutoff m) then a.shop_name else \'Others\' end group by order_id ,Final_Marketplace ,reference_code ,a.shop_name ,warehouse_name ,order_date ,same_day_cut_off ,Order_Shipping_Last_Update_Date ,shipping_status ,order_status ,upper(coalesce(FINAL_SHIPPING_STATUS,shipping_status,order_status)) ,COURIER ,Source ,payment_mode ,brand;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from ras_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        