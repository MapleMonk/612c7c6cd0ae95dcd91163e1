{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table MYMUSE_DB.MAPLEMONK.MYMUSE_DB_Order_Fulfillment_Report as select order_id ,a.marketplace Final_Marketplace ,reference_code ,a.shop_name marketplace ,a.awb AWB ,warehouse warehouse_name ,dayname(order_date) order_day_name ,order_date ,same_day_cut_off ,c.zone Zone ,c.return_flag Return_Order_Flag ,max(try_to_decimal(c.agreed_tat)) agreed_tat ,max(DISPATCH_DATE) as Order_Manifest_date ,max(case when lower(coalesce(c.courier, a.COURIER)) like any (\'%we fast%\', \'%wefast%\', \'%loginext%\') then DISPATCH_DATE else c.first_out_for_delivery_date end) first_out_for_delivery_date ,max(c.pickedup_date) pickedup_date ,coalesce(c.delivered_date, case when upper(coalesce(final_shipping_status,shipping_status,order_status)) in (\'DELIVERED\') then a.shipping_last_update_date end) order_delivered_date ,coalesce(c.updated_date, a.shipping_last_update_date) as Order_Shipping_Last_Update_Date ,upper(shipping_status) shipping_status ,upper(final_shipping_status) final_shipping_status ,upper(order_status) order_status ,upper(case when lower(coalesce(c.courier, a.COURIER)) like any (\'%we fast%\', \'%wefast%\') then \'WEFAST\' else coalesce(c.courier, a.COURIER) end) courier ,Source ,coalesce(a.payment_mode, c.payment_method) payment_mode ,upper(coalesce(final_shipping_status,shipping_status,order_status)) final_status ,case when upper(final_status) in (\'DELIVERED\') then 1 else 0 end as delivered_order ,case when upper(final_status) = \'ORDER YET TO SYNC\' then 1 else 0 end as Order_yet_to_Sync_Order ,case when upper(final_status) = \'CONFIRMED\' then 1 else 0 end as Confirmed_Order ,case when upper(final_status) = \'ASSIGNED\' then 1 else 0 end as Assigned_Order ,case when upper(final_status) = \'OPEN\' then 1 else 0 end as Open_Order ,case when upper(final_status) = \'PRINTED\' then 1 else 0 end as Printed_Order ,case when upper(final_status) = \'PENDING\' then 1 else 0 end as Pending_Order ,case when upper(final_status) = \'IN TRANSIT\' then 1 else 0 end as In_Transit_order ,case when upper(final_status) = \'RTO\' then 1 else 0 end as RTO_Order ,case when upper(final_status) = \'RTS\' then 1 else 0 end as RTS_Order ,case when upper(final_status) = \'RETURNED\' then 1 else 0 end as Returned_order ,case when upper(final_status) = \'PICKUP ERROR\' then 1 else 0 end as Pickup_Error ,case when upper(final_status) = \'LOST\' then 1 else 0 end as Lost_Order ,case when upper(final_status) =\'EXCEPTION\' then 1 else 0 end as Exception_Order ,case when upper(final_status) = \'MISROUTED\' then 1 else 0 end as Misrouted_Order ,case when upper(final_status) = \'UNDERPROCESS\' then 1 else 0 end as Underprocess_Order ,case when upper(final_status) = \'DAMAGED\' then 1 else 0 end as Damaged_Order ,case when upper(final_status) = \'CANCELLED\' then 1 else 0 end as cancelled_order ,case when delivered_order + Returned_order + Lost_Order + cancelled_order >= 1 then datediff(\'hour\',order_date::timestamp ,Order_Shipping_Last_Update_Date::timestamp ) else datediff(\'hour\',order_date::timestamp ,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp) end as Time_From_Order ,case when Time_from_Order <= 24 then \'1. <=24 hours\' when Time_from_Order <= 48 then \'2. 25 - 48 hours\' when Time_from_Order <= 72 then \'3. 49 - 72 hours\' when Time_from_Order <= 96 then \'4. 73 - 96 hours\' when Time_from_Order <= 120 then \'5. 97 - 120 hours\' when Time_from_Order <= 144 then \'6. 121 - 144 hours\' when Time_from_Order <= 144 then \'7. 145 - 168 hours\' else \'8. More than a week\' end as Time_From_Order_Category ,case when lower(final_status) like any (\'%cancel%\') then 0 else datediff(\'hour\',order_date::timestamp ,coalesce(order_manifest_date::timestamp,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp)) end Dispatch_Speed ,case when Dispatch_Speed < 2 then \'0- 2 hours\' when Dispatch_Speed < 6 then \'2 - 6 hours\' when Dispatch_Speed < 12 then \'6 - 12 hours\' when Dispatch_Speed < 24 then \'12 - 24 hours\' when Dispatch_Speed < 48 then \'24-48 hours\' when Dispatch_Speed < 72 then \'48-72 hours\' when Dispatch_Speed < 96 then \'72-96 hours\' when order_manifest_date is not NULL then \'> 96 hours\' when order_manifest_date is NULL then \'Not Dispatched\' end as Dispatch_Speed_flag ,greatest(case when hour(order_date::timestamp) < hour(try_cast(same_day_cut_off as time)) then datediff(\'day\',order_date::timestamp ,coalesce(order_manifest_date::timestamp,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp )) else datediff(\'day\',order_date::timestamp ,coalesce(order_manifest_date::timestamp,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp )) -1 end,0) as Dispatch_Flag ,Case when Dispatch_Flag=0 then \'Same Day\' when Dispatch_Flag=1 then \'Next Day\' else \'2+ Days\' end as Final_Dispatch_Bucket ,case when delivered_order = 1 then datediff(\'hour\',order_manifest_date::timestamp , order_delivered_date ) end as Delivery_Speed ,case when delivered_order = 1 then datediff(\'hour\',order_manifest_date::timestamp ,order_delivered_date ) else datediff(\'hour\',order_manifest_date::timestamp ,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp) end as deliver_time ,case when delivered_order = 1 and datediff(\'hour\',order_manifest_date::timestamp,order_delivered_date ) < 24 then \'< 24 hours\' when delivered_order = 1 and datediff(\'hour\',order_manifest_date::timestamp,order_delivered_date ) < 48 then \'24-48 hours\' when delivered_order = 1 and datediff(\'hour\',order_manifest_date::timestamp,order_delivered_date ) < 72 then \'48-72 hours\' when delivered_order = 1 and datediff(\'hour\',order_manifest_date::timestamp,order_delivered_date ) < 96 then \'72-96 hours\' when delivered_order = 1 then \'> 96 hours\' else \'Not Delivered\' end as delivery_time_flag ,case when order_manifest_date is NULL then \'Before Dispatch\' else \'After Dispatch\' end as dispatch_status ,case when not(lower(final_status) like \'%cancel%\') then datediff(\'hour\',max(c.pickedup_date),ifnull(max(c.first_out_for_delivery_date), convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp)) end FIRST_OUT_FOR_DELIVERY_TAT ,case when FIRST_OUT_FOR_DELIVERY_TAT <= max(try_to_decimal(c.agreed_tat))*24 then \'ON TIME\' when FIRST_OUT_FOR_DELIVERY_TAT > max(try_to_decimal(c.agreed_tat))*24 then \'DELAYED\' end FAT_TAT_PERFORMANCE from MYMUSE_DB.MAPLEMONK.MYMUSE_DB_sales_consolidated a left join MYMUSE_DB.MAPLEMONK.marketplace_fulfilment_cutoff b on b.marketplace = case when a.shop_name in (select distinct m.marketplace from MYMUSE_DB.MAPLEMONK.marketplace_fulfilment_cutoff m) then a.shop_name else \'Others\' end left join MYMUSE_DB.MAPLEMONK.MYMUSE_LOGISTICS_CONSOLIDATED c on a.awb = c.awb group by order_id ,Final_Marketplace ,reference_code ,a.awb ,a.shop_name ,warehouse_name ,c.zone ,c.return_flag ,dayname(order_date) ,order_date ,same_day_cut_off ,coalesce(c.delivered_date, case when upper(coalesce(final_shipping_status,shipping_status,order_status)) in (\'DELIVERED\') then a.shipping_last_update_date end) ,coalesce(c.updated_date, a.shipping_last_update_date) ,shipping_status ,final_shipping_status ,order_status ,upper(coalesce(final_shipping_status,shipping_status,order_status)) ,upper(case when lower(coalesce(c.courier, a.COURIER)) like any (\'%we fast%\', \'%wefast%\') then \'WEFAST\' else coalesce(c.courier, a.COURIER) end) ,Source ,coalesce(a.payment_mode, c.payment_method);",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from MYMUSE_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        