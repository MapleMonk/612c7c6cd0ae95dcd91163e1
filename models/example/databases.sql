{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_Order_Fulfillment_Report as select a.order_id ,a.awb ,a.marketplace Final_Marketplace ,a.reference_code ,a.marketplace marketplace ,a.source_pincode ,a.destination_pincode ,warehouse warehouse_name ,b.Same_Day_Cut_Off ,case WHEN (EXTRACT(HOUR FROM order_date)) >= b.Same_Day_Cut_Off THEN DATEADD(DAY, 1, order_date) ::date ELSE order_date::date END AS Target_dispatch_Pre ,case when upper(dayname(Target_dispatch_Pre)) = \'SUN\' then DATEADD(DAY, 1, Target_dispatch_Pre) else Target_dispatch_Pre END as Target_dispatch ,a.order_date ,coalesce(cf.pickup_date,a.dispatch_date) as Order_Manifest_date ,shipping_last_update_date as Order_Shipping_Last_Update_Date ,delivery_date as order_delivered_date ,upper(shipping_status) shipping_status ,upper(order_status) order_status ,a.COURIER ,Source ,payment_mode ,upper(coalesce(vo.virtual_order_flag,\'Not Virtual Order\')) virtual_flag ,upper(case when lower(a.marketplace) like \'%flipkart%\' then \'ekart\' when lower(source) like any (\'%gofynd%\', \'%amazon smart%\') then \'amazon flex\' else \'Self Ship\' end) as courier_partner ,upper(case when lower(coalesce(order_status, oms_order_status)) like \'%cancel%\' then \'CANCELLED\' else coalesce(FINAL_SHIPPING_STATUS,shipping_status,order_status) end) final_status ,case when upper(final_status) in (\'DELIVERED\') then 1 else 0 end as delivered_order ,case when upper(final_status) = \'ORDER YET TO SYNC\' then 1 else 0 end as Order_yet_to_Sync_Order ,case when upper(final_status) = \'CONFIRMED\' then 1 else 0 end as Confirmed_Order ,case when upper(final_status) = \'ASSIGNED\' then 1 else 0 end as Assigned_Order ,case when upper(final_status) = \'OPEN\' then 1 else 0 end as Open_Order ,case when upper(final_status) = \'PRINTED\' then 1 else 0 end as Printed_Order ,case when upper(final_status) = \'PENDING\' then 1 else 0 end as Pending_Order ,case when upper(final_status) = \'IN TRANSIT\' then 1 else 0 end as In_Transit_order ,case when upper(final_status) = \'RTO\' then 1 else 0 end as RTO_Order ,case when upper(final_status) = \'RTS\' then 1 else 0 end as RTS_Order ,case when upper(final_status) = \'RETURNED\' then 1 else 0 end as Returned_order ,case when upper(final_status) = \'PICKUP ERROR\' then 1 else 0 end as Pickup_Error ,case when upper(final_status) = \'LOST\' then 1 else 0 end as Lost_Order ,case when upper(final_status) =\'EXCEPTION\' then 1 else 0 end as Exception_Order ,case when upper(final_status) = \'MISROUTED\' then 1 else 0 end as Misrouted_Order ,case when upper(final_status) = \'UNDERPROCESS\' then 1 else 0 end as Underprocess_Order ,case when upper(final_status) = \'DAMAGED\' then 1 else 0 end as Damaged_Order ,case when upper(final_status) = \'CANCELLED\' then 1 else 0 end as cancelled_order ,case when delivered_order = 1 then datediff(\'hour\',order_date::timestamp ,Order_Shipping_Last_Update_Date::timestamp ) else datediff(\'hour\',order_date::timestamp ,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp) end as Time_From_Order ,case when Time_from_Order <= 24 then \'1. <=24 hours\' when Time_from_Order <= 48 then \'2. 25 - 48 hours\' when Time_from_Order <= 72 then \'3. 49 - 72 hours\' when Time_from_Order <= 96 then \'4. 73 - 96 hours\' when Time_from_Order <= 120 then \'5. 97 - 120 hours\' when Time_from_Order <= 144 then \'6. 121 - 144 hours\' when Time_from_Order <= 144 then \'7. 145 - 168 hours\' else \'8. More than a week\' end as Time_From_Order_Category ,datediff(\'hour\',order_date::timestamp ,order_manifest_date::timestamp) Dispatch_Speed ,datediff(\'hour\',order_date::timestamp ,order_delivered_date::timestamp) o2d_delivery_Speed ,datediff(\'hour\',order_manifest_date::timestamp ,order_delivered_date::timestamp) s2d_delivery_Speed ,upper(case when Dispatch_Speed < 2 then \'0- 2 hours\' when Dispatch_Speed < 6 then \'2 - 6 hours\' when Dispatch_Speed < 12 then \'6 - 12 hours\' when Dispatch_Speed < 24 then \'12 - 24 hours\' when Dispatch_Speed < 48 then \'24-48 hours\' when Dispatch_Speed < 72 then \'48-72 hours\' when Dispatch_Speed < 96 then \'72-96 hours\' when order_manifest_date is not NULL then \'> 96 hours\' when order_manifest_date is NULL then \'Not Dispatched\' end) as Dispatch_Speed_flag ,greatest(case when hour(order_date::timestamp) < hour(try_cast(same_day_cut_off as time)) then datediff(\'day\',order_date::timestamp ,coalesce(order_manifest_date::timestamp,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp )) else datediff(\'day\',order_date::timestamp ,coalesce(order_manifest_date::timestamp,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp )) -1 end,0) as Dispatch_Flag ,Case when Dispatch_Flag=0 then \'Same Day\' when Dispatch_Flag=1 then \'Next Day\' else \'2+ Days\' end as Final_Dispatch_Bucket ,case when delivered_order = 1 then datediff(\'hour\',order_manifest_date::timestamp , order_delivered_date ) end as Delivery_Speed ,case when delivered_order = 1 then datediff(\'hour\',order_manifest_date::timestamp ,order_delivered_date ) else datediff(\'hour\',order_manifest_date::timestamp ,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp) end as deliver_time ,upper(case when delivered_order = 1 and datediff(\'hour\',order_manifest_date::timestamp,order_delivered_date ) < 24 then \'< 24 hours\' when delivered_order = 1 and datediff(\'hour\',order_manifest_date::timestamp,order_delivered_date ) < 48 then \'24-48 hours\' when delivered_order = 1 and datediff(\'hour\',order_manifest_date::timestamp,order_delivered_date ) < 72 then \'48-72 hours\' when delivered_order = 1 and datediff(\'hour\',order_manifest_date::timestamp,order_delivered_date ) < 96 then \'72-96 hours\' when delivered_order = 1 then \'> 96 hours\' else \'Not Delivered\' end) as delivery_time_flag ,upper(case when delivered_order = 1 and datediff(\'hour\',order_date::timestamp,order_delivered_date ) < 24 then \'< 24 hours\' when delivered_order = 1 and datediff(\'hour\',order_date::timestamp,order_delivered_date ) < 48 then \'24-48 hours\' when delivered_order = 1 and datediff(\'hour\',order_date::timestamp,order_delivered_date ) < 72 then \'48-72 hours\' when delivered_order = 1 and datediff(\'hour\',order_date::timestamp,order_delivered_date ) < 96 then \'72-96 hours\' when delivered_order = 1 then \'> 96 hours\' else \'Not Delivered\' end) as delivery_time_flag_o2d ,upper(case when lower(final_status) like \'%cancel%\' then \'CANCELLED\' when not(lower(final_status) like \'%cancel%\') and lower(a.source) like any (\'%gofynd%\', \'%amazon smart%\') and order_manifest_date is NULL then upper(\'amazon fulfilled - not dispatched\') when not(lower(final_status) like \'%cancel%\') and lower(a.source) like any (\'%gofynd%\', \'%amazon smart%\') and order_manifest_date is not NULL then upper(\'amazon fulfilled - dispatched\') when not(lower(final_status) like \'%cancel%\') and not (lower(a.source_pincode) like \'%amazon%\') and order_manifest_date is NULL then \'NOT DISPATCHED\' when order_manifest_date is not NULL then \'DISPATCHED\' when order_manifest_date is NULL then \'NOT DISPATCHED\' end) as dispatch_status ,max(TAT.TARGET_TAT) TARGET_TAT ,case when not(lower(final_status) like \'%cancel%\') then datediff(\'hour\',order_manifest_date::timestamp,DATEADD(DAY, 1, DATE_TRUNC(DAY,DATEADD(DAY,max(try_to_decimal(TAT.TARGET_TAT)),order_manifest_date::timestamp)))) end target_tat_hours ,case when not(lower(final_status) like \'%cancel%\') then datediff(\'hour\',order_manifest_date::timestamp,ifnull(order_delivered_date, convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp)) end DELIVERY_TAT ,case when DELIVERY_TAT <= target_tat_hours then \'ON TIME\' when DELIVERY_TAT > target_tat_hours then \'DELAYED\' end TAT_PERFORMANCE ,upper (case when (not(lower(final_status) like \'%cancel%\')) AND lower(a.source) like any (\'%gofynd%\', \'%amazon smart%\') and order_delivered_date is null then \'amazon fulfilled\' when (not(lower(final_status) like \'%cancel%\')) AND lower(a.source) like any (\'%gofynd%\', \'%amazon smart%\') and order_delivered_date is not null then \'amazon fulfilled - delivered\' when (not(lower(final_status) like \'%cancel%\')) and order_delivered_date is null and DELIVERY_TAT <= target_tat_hours then \'In Transit - On Time\' when (not(lower(final_status) like \'%cancel%\')) and order_delivered_date is null and DELIVERY_TAT > target_tat_hours then \'In Transit - Delayed\' when (not(lower(final_status) like \'%cancel%\')) and order_delivered_date is not null and DELIVERY_TAT > target_tat_hours then \'delivered - Delayed\' when (not(lower(final_status) like \'%cancel%\')) and order_delivered_date is not null and DELIVERY_TAT <= target_tat_hours then \'delivered - On Time\' else \'zone not mapped\' end) as s2d_status ,case when not(lower(final_status) like \'%cancel%\') then datediff(\'hour\',order_date::timestamp,DATEADD(DAY, 1, DATE_TRUNC(DAY,DATEADD(DAY,max(try_to_decimal(TAT.TARGET_TAT)),order_date::timestamp)))) end target_tat_hours_o2d ,case when not(lower(final_status) like \'%cancel%\') then datediff(\'hour\',order_date::timestamp,ifnull(order_delivered_date, convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp)) end DELIVERY_TAT_o2d ,upper(case when (not(lower(final_status) like \'%cancel%\') and lower(a.source_pincode) like \'%amazon%\') then \'amazon-fulfilled\' when (not(lower(final_status) like \'%cancel%\') and order_manifest_date is null) then \'Not dispatched\' when (not(lower(final_status) like \'%cancel%\') and order_delivered_date is null and DELIVERY_TAT_o2d <= target_tat_hours_o2d ) then \'In Transit On Time\' when (not(lower(final_status) like \'%cancel%\') and order_delivered_date is null and DELIVERY_TAT_o2d > target_tat_hours_o2d ) then \'In Transit Delayed\' when (not(lower(final_status) like \'%cancel%\') and order_delivered_date is not null and DELIVERY_TAT_o2d > target_tat_hours_o2d) then \'delivered Delayed\' when (not(lower(final_status) like \'%cancel%\') and order_delivered_date is not null and DELIVERY_TAT_o2d <= target_tat_hours_o2d ) then \'delivered On Time\' end) as o2d_status ,case when (not(lower(final_status) like \'%cancel%\')) then datediff(\'hour\',order_date::timestamp,ifnull(order_manifest_date, convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp)) end DELIVERY_TAT_o2s ,upper(CASE WHEN ((NOT (LOWER(final_status) LIKE \'%cancel%\')) AND lower(a.source) like any (\'%gofynd%\', \'%amazon smart%\')) and order_manifest_date is null THEN upper(\'amazon-fulfilled\') WHEN ((NOT (LOWER(final_status) LIKE \'%cancel%\')) AND lower(a.source) like any (\'%gofynd%\', \'%amazon smart%\') and order_manifest_date is not null) then upper(\'amazon-dispatched\') WHEN ( (NOT (LOWER(final_status) LIKE \'%cancel%\')) AND order_manifest_date IS NULL AND IFNULL(order_manifest_date, CONVERT_TIMEZONE(\'America/Los_Angeles\', \'Asia/Kolkata\', CURRENT_TIMESTAMP())::TIMESTAMP)::date > Target_dispatch )THEN \'Not Dispatched On Time\' WHEN ( (NOT (LOWER(final_status) LIKE \'%cancel%\')) AND order_manifest_date IS NULL AND IFNULL(order_manifest_date, CONVERT_TIMEZONE(\'America/Los_Angeles\', \'Asia/Kolkata\', CURRENT_TIMESTAMP())::TIMESTAMP)::date <= Target_dispatch ) THEN \'Not Dispatched Delayed\' WHEN ( (NOT (LOWER(final_status) LIKE \'%cancel%\')) AND order_manifest_date IS NOT NULL AND IFNULL(order_manifest_date, CONVERT_TIMEZONE(\'America/Los_Angeles\', \'Asia/Kolkata\', CURRENT_TIMESTAMP())::TIMESTAMP)::date > Target_dispatch ) THEN \'Dispatched Delayed\' WHEN ( (NOT (LOWER(final_status) LIKE \'%cancel%\')) AND order_manifest_date IS NOT NULL and IFNULL(order_manifest_date, CONVERT_TIMEZONE(\'America/Los_Angeles\', \'Asia/Kolkata\', CURRENT_TIMESTAMP())::TIMESTAMP)::date <= Target_dispatch ) THEN \'Dispatched On Time\' END) AS o2s_status, max(TARGET_TAT) as s2d_sla, max(TARGET_TAT2) as o2s_sla, max(TARGET_TAT1) as o2d_sla , upper(case when lower(final_status) like \'%cancel%\' then \'CANCELLED\' when not(lower(final_status) like \'%cancel%\') and lower(a.source) like any (\'%gofynd%\', \'%amazon smart%\') and order_delivered_date is NULL then upper(\'amazon fulfilled\') when not(lower(final_status) like \'%cancel%\') and lower(a.source) like any (\'%gofynd%\', \'%amazon smart%\') and order_delivered_date is not NULL then upper(\'amazon delivered\') when not(lower(final_status) like \'%cancel%\') and not lower(a.source) like any (\'%gofynd%\', \'%amazon smart%\') and order_delivered_date is NULL then \'NOT DELIVERED\' else \'DELIVERED\' end) as delivery_status ,dateadd(day, max(TARGET_TAT1), order_date::timestamp) as promised_delivery_date ,TAT.zone from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated a left join SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_CLICKPOST_FACT_ITEMS cf on lower(cf.awb_number) = lower(a.awb) left join SLEEPYCAT_DB.MAPLEMONK.marketplace_fulfilment_cutoff b on lower(b.marketplace) = lower(a.marketplace) and lower(b.courier) = (case when lower(a.marketplace) like \'flipkart%\' and lower(a.courier) = \'ekart\' then lower(a.courier) when lower(a.marketplace) = \'flipkart\' then lower(\'other\') else \'any\' end) left join (select * from (select marketplace ,zone ,Source_Pincode ,Destination_Pincode, \"S2D SLA\" as s2d_sla, \"O2D SLA\" as o2d_sla ,\"S2D SLA\" as TARGET_TAT, \"O2D SLA\" as TARGET_TAT1, \"O2D SLA\"::int - \"S2D SLA\"::int as TARGET_TAT2 ,row_number() over (partition by lower(marketplace), source_pincode, destination_pincode order by 1) rw from SLEEPYCAT_DB.MAPLEMONK.delivery_pin_tat ) where rw=1) TAT on lower(case when lower(a.marketplace) = \'flipkart\' then a.marketplace else \'1\' end) = lower(case when lower(TAT.marketplace)=\'flipkart\' then TAT.marketplace else \'1\' end) and a.source_pincode = TAT.source_pincode and a.destination_pincode = TAT.Destination_Pincode left join (select * from (select \"Order id\" reference_code ,\"Sku\'s\" SKU ,\'Virtual Order\' Virtual_Order_Flag ,row_number() over (partition by \"Order id\", lower(\"Sku\'s\") order by 1) rw from SLEEPYCAT_DB.MAPLEMONK.virtual_orders_sheet1 ) where rw = 1 ) vo on lower(a.reference_code) = lower(vo.reference_code) and lower(a.common_sku_code) = lower(vo.sku) group by order_id ,Final_Marketplace ,a.reference_code ,a.marketplace ,a.source_pincode ,a.destination_pincode ,warehouse_name ,a.order_date ,b.same_day_cut_off ,Order_Shipping_Last_Update_Date ,case when upper(coalesce(final_shipping_status,shipping_status,order_status)) in (\'DELIVERED\') then a.shipping_last_update_date end ,shipping_status ,order_status ,upper(case when lower(coalesce(order_status, oms_order_status)) like \'%cancel%\' then \'CANCELLED\' else coalesce(FINAL_SHIPPING_STATUS,shipping_status,order_status) end) ,a.COURIER ,Source ,payment_mode, tat.zone, awb, coalesce(cf.pickup_date,a.dispatch_date), delivery_date, upper(coalesce(vo.virtual_order_flag,\'Not Virtual Order\')) ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from SLEEPYCAT_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        