{{ config(
            materialized='table',
                post_hook={
                    "sql": "create table if not exists MINDFUL_DB.MAPLEMONK.marketplace_fulfillment_cutoff (Marketplace varchar,Same_Day_Cut_Off varchar); WITH marketplace_fulfillment_cutoff AS ( SELECT \'Myntra PPMP\' AS marketplace, \'11:00 AM\' AS cutoff_time UNION ALL SELECT \'Flipkart\', \'12:00 PM\' UNION ALL SELECT \'Amazon\', \'2:00 PM\' UNION ALL SELECT \'Others\', \'4:00 PM\' ) select * from marketplace_fulfillment_cutoff where not exists(select * from MINDFUL_DB.MAPLEMONK.marketplace_fulfillment_cutoff) union select * from MINDFUL_DB.MAPLEMONK.marketplace_fulfillment_cutoff; create table if not exists MINDFUL_DB.MAPLEMONK.MINDFUL_DB_CLICKPOST_FACT_ITEMS (awb_number varchar,pickup_date varchar,courier_partner varchar,PAYMENT_METHOD varchar,CURRENT_STATUS varchar,delivery_date varchar); create table if not exists MINDFUL_DB.MAPLEMONK.delivery_pin_tat (origin_pincode varchar, destination_pincode varchar, zone varchar,target_tat int); create or replace table MINDFUL_DB.MAPLEMONK.MINDFUL_DB_Order_Fulfillment_Report as select a.order_id ,a.marketplace Final_Marketplace ,a.reference_code ,a.marketplace marketplace ,warehouse warehouse_name ,b.Same_Day_Cut_Off ,a.order_timestamp ,a.order_timestamp order_date ,a.awb ,upper(order_status) order_status ,coalesce(cf.courier_partner,a.COURIER) courier ,Source ,upper(coalesce(cf.PAYMENT_METHOD, a.payment_mode)) payment_mode ,upper(case when (lower(a.order_status) like \'%cancel%\' or lower(final_shipping_status) like \'%cancel%\') then \'CANCELLED\' else coalesce(FINAL_SHIPPING_STATUS,shipping_status,order_status) end) final_status ,upper(coalesce(cf.CURRENT_STATUS,shipping_status)) shipping_status ,CASE WHEN EXTRACT(HOUR FROM order_timestamp) >= EXTRACT(HOUR FROM TO_TIMESTAMP(b.Same_Day_Cut_Off, \'HH:MI AM\')) THEN DATEADD(DAY, 1, order_timestamp)::DATE ELSE order_timestamp::DATE END AS Target_dispatch_Pre ,case when upper(dayname(Target_dispatch_Pre)) = \'SUN\' then DATEADD(DAY, 1, Target_dispatch_Pre) else Target_dispatch_Pre END as Target_dispatch_date ,coalesce(cf.pickup_date,a.dispatch_date) as Order_Manifest_date ,shipping_last_update_date as Order_Shipping_Last_Update_Date ,coalesce(cf.delivery_date,a.DELIVERED_DATE) as order_delivered_date ,target_tat ,DATEADD(DAY, 5, Order_Manifest_date) ::date AS Target_delivery_pre ,Target_delivery_pre Target_delivery_date ,upper(case when lower(final_status) like \'%cancel%\' then \'CANCELLED\' when not(lower(final_status) like \'%cancel%\') and Order_Manifest_date is NULL then \'NOT DISPATCHED\' when Order_Manifest_date is not NULL then \'DISPATCHED\' when Order_Manifest_date is NULL then \'NOT DISPATCHED\' end) as dispatch_status ,upper(case when lower(final_status) like \'%cancel%\' then \'CANCELLED\' when not(lower(final_status) like \'%cancel%\') and Order_Delivered_Date is NULL then \'NOT DELIVERED\' else \'DELIVERED\' end) as delivery_status ,case when lower(final_status) like \'%cancel%\' then \'CANCELLED\' when not(lower(final_status) like \'%cancel%\') and Order_Manifest_date is NULL and convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::date > Target_dispatch_date::Date then \'NOT DISPATCHED - DELAYED\' when not(lower(final_status) like \'%cancel%\') and Order_Manifest_date is NULL and convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::date <= Target_dispatch_date::Date then \'NOT DISPATCHED - WITHIN SLA\' when not(lower(final_status) like \'%cancel%\') and Order_Manifest_date::date <= Target_dispatch_date::Date then \'DISPATCHED - ON TIME\' when not(lower(final_status) like \'%cancel%\') and Order_Manifest_date::date > Target_dispatch_date::Date then \'DISPATCHED - DELAYED\' else \'OTHERS\' end DISPATCH_SLA_STATUS ,case when lower(final_status) like \'%cancel%\' then \'CANCELLED\' when not(lower(final_status) like \'%cancel%\') and Order_Delivered_Date is NULL and convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::date > Target_delivery_date::Date then \'NOT DELIVERED - DELAYED\' when not(lower(final_status) like \'%cancel%\') and Order_Delivered_Date is NULL and convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::date <= Target_delivery_date::Date then \'NOT DELIVERED - WITHIN SLA\' when not(lower(final_status) like \'%cancel%\') and Order_Delivered_Date::date <= Target_delivery_date::Date then \'DELIVERED - ON TIME\' when not(lower(final_status) like \'%cancel%\') and Order_Delivered_Date::date > Target_delivery_date::Date then \'DELIVERED - DELAYED\' else \'OTHERS\' end DELIVERED_SLA_STATUS ,case when upper(final_status) in (\'DELIVERED\') then 1 else 0 end as delivered_order ,case when upper(final_status) = \'ORDER YET TO SYNC\' then 1 else 0 end as Order_yet_to_Sync_Order ,case when upper(final_status) = \'CONFIRMED\' then 1 else 0 end as Confirmed_Order ,case when upper(final_status) = \'ASSIGNED\' then 1 else 0 end as Assigned_Order ,case when upper(final_status) = \'OPEN\' then 1 else 0 end as Open_Order ,case when upper(final_status) = \'PRINTED\' then 1 else 0 end as Printed_Order ,case when upper(final_status) = \'PENDING\' then 1 else 0 end as Pending_Order ,case when upper(final_status) = \'IN TRANSIT\' then 1 else 0 end as In_Transit_order ,case when upper(final_status) = \'RTO\' then 1 else 0 end as RTO_Order ,case when upper(final_status) = \'RTS\' then 1 else 0 end as RTS_Order ,case when upper(final_status) = \'RETURNED\' then 1 else 0 end as Returned_order ,case when upper(final_status) = \'PICKUP ERROR\' then 1 else 0 end as Pickup_Error ,case when upper(final_status) = \'LOST\' then 1 else 0 end as Lost_Order ,case when upper(final_status) =\'EXCEPTION\' then 1 else 0 end as Exception_Order ,case when upper(final_status) = \'MISROUTED\' then 1 else 0 end as Misrouted_Order ,case when upper(final_status) = \'UNDERPROCESS\' then 1 else 0 end as Underprocess_Order ,case when upper(final_status) = \'DAMAGED\' then 1 else 0 end as Damaged_Order ,case when upper(final_status) = \'CANCELLED\' then 1 else 0 end as cancelled_order ,case when delivered_order = 1 then datediff(\'hour\',order_date::timestamp ,order_delivered_date::timestamp ) else datediff(\'hour\',order_date::timestamp ,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp) end as Time_From_Order ,case when delivered_order = 1 then datediff(\'day\',order_date::timestamp ,order_delivered_date::timestamp ) else datediff(\'day\',order_date::timestamp ,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp) end as Days_From_Order ,upper(case when lower(final_status) like \'%cancel%\' then \'CANCELLED\' when Time_from_Order <= 24 then \'1. <=24 hours\' when Time_from_Order <= 48 then \'2. 25 - 48 hours\' when Time_from_Order <= 72 then \'3. 49 - 72 hours\' when Time_from_Order <= 96 then \'4. 73 - 96 hours\' when Time_from_Order <= 120 then \'5. 97 - 120 hours\' when Time_from_Order <= 144 then \'6. 121 - 144 hours\' when Time_from_Order <= 144 then \'7. 145 - 168 hours\' else \'8. More than a week\' end) as Time_From_Order_Category ,upper(case when lower(final_status) like \'%cancel%\' then \'CANCELLED\' when Days_from_Order <= 1 then \'1. Within a day\' when Days_from_Order <= 2 then \'2. 2 days\' when Days_from_Order <= 5 then \'3. 3 - 5 days\' when Days_from_Order <= 7 then \'4. 6 - 7 days\' when Days_from_Order <= 10 then \'5. 7 - 10 days\' else \'8. More than 10 days\' end) as Days_From_Order_Category ,datediff(\'hour\',order_timestamp::timestamp ,order_delivered_date::timestamp) o2d_delivery_Speed ,datediff(\'day\',order_timestamp::timestamp ,order_delivered_date::timestamp) o2d_delivery_Speed_Days ,datediff(\'hour\',order_manifest_date::timestamp ,order_delivered_date::timestamp) s2d_delivery_Speed ,datediff(\'day\',order_manifest_date::timestamp ,order_delivered_date::timestamp) s2d_delivery_Speed_Days ,datediff(\'hour\',order_date::timestamp ,order_manifest_date::timestamp) Dispatch_Speed ,datediff(\'day\',order_date::timestamp ,order_manifest_date::timestamp) Dispatch_Speed_Days ,upper(case when lower(final_status) like \'%cancel%\' then \'CANCELLED\' when Dispatch_Speed < 2 then \'0- 2 hours\' when Dispatch_Speed < 6 then \'2 - 6 hours\' when Dispatch_Speed < 12 then \'6 - 12 hours\' when Dispatch_Speed < 24 then \'12 - 24 hours\' when Dispatch_Speed < 48 then \'24-48 hours\' when Dispatch_Speed < 72 then \'48-72 hours\' when Dispatch_Speed < 96 then \'72-96 hours\' when order_manifest_date is not NULL then \'> 96 hours\' when order_manifest_date is NULL then \'Not Dispatched\' end) as Dispatch_Speed_flag ,upper(case when lower(final_status) like \'%cancel%\' then \'CANCELLED\' when Dispatch_Speed_Days <= 1 then \'1. Within a day\' when Dispatch_Speed_Days <=2 then \'2. 2 days\' when Dispatch_Speed_Days <=3 then \'3. 3 days\' when Dispatch_Speed_Days <= 5 then \'4. 4 - 5 days\' when order_manifest_date is not NULL then \'> 5 days\' when order_manifest_date is NULL then \'Not Dispatched\' end) as Dispatch_Speed_Days_flag ,greatest(case when hour(order_date::timestamp) < EXTRACT(HOUR FROM TO_TIMESTAMP(b.Same_Day_Cut_Off, \'HH:MI AM\')) then datediff(\'day\',order_date::timestamp ,coalesce(order_manifest_date::timestamp,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp )) else datediff(\'day\',order_date::timestamp ,coalesce(order_manifest_date::timestamp,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp )) -1 end,0) as Dispatch_Flag ,Case when Dispatch_Flag=0 then \'Same Day\' when Dispatch_Flag=1 then \'Next Day\' else \'2+ Days\' end as Final_Dispatch_Bucket ,case when delivered_order = 1 then datediff(\'hour\',order_manifest_date::timestamp , order_delivered_date::timestamp ) end as Delivery_Speed ,case when delivered_order = 1 then datediff(\'hour\',order_manifest_date::timestamp ,order_delivered_date::timestamp ) else datediff(\'hour\',order_manifest_date::timestamp ,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp) end as deliver_time ,upper(case when lower(final_status) like \'%cancel%\' then \'CANCELLED\' when delivered_order = 1 and datediff(\'hour\',order_manifest_date::timestamp,order_delivered_date::timestamp ) < 24 then \'< 24 hours\' when delivered_order = 1 and datediff(\'hour\',order_manifest_date::timestamp,order_delivered_date::timestamp ) < 48 then \'24-48 hours\' when delivered_order = 1 and datediff(\'hour\',order_manifest_date::timestamp,order_delivered_date::timestamp ) < 72 then \'48-72 hours\' when delivered_order = 1 and datediff(\'hour\',order_manifest_date::timestamp,order_delivered_date::timestamp ) < 96 then \'72-96 hours\' when delivered_order = 1 then \'> 96 hours\' else \'NOT DELIVERED\' end) as delivery_time_flag ,upper(case when lower(final_status) like \'%cancel%\' then \'CANCELLED\' when delivered_order = 1 and datediff(\'day\',order_manifest_date::timestamp,order_delivered_date::timestamp ) <= 1 then \'1. Within a day\' when delivered_order = 1 and datediff(\'day\',order_manifest_date::timestamp,order_delivered_date::timestamp ) <= 2 then \'2. 2 days\' when delivered_order = 1 and datediff(\'day\',order_manifest_date::timestamp,order_delivered_date::timestamp ) <= 5 then \'3. 3 - 5 days\' when delivered_order = 1 and datediff(\'day\',order_manifest_date::timestamp,order_delivered_date::timestamp ) <= 7 then \'4. 6 - 7 days\' when delivered_order = 1 then \'5. > 7 days\' else \'NOT DELIVERED\' end) as delivery_time_days_flag ,a.source_pincode ,a.pincode ,ztm.zone from MINDFUL_DB.MAPLEMONK.MINDFUL_DB_sales_consolidated a left join MINDFUL_DB.MAPLEMONK.MINDFUL_DB_CLICKPOST_FACT_ITEMS cf on lower(cf.awb_number) = lower(a.awb) left join MINDFUL_DB.MAPLEMONK.marketplace_fulfillment_cutoff b on lower(b.marketplace) = lower(a.marketplace) left join (select * from ( select * ,row_number() over(partition by origin_pincode,destination_pincode order by 1)rw from MINDFUL_DB.MAPLEMONK.delivery_pin_tat )where rw = 1 )ztm on a.source_pincode = ztm.origin_pincode and a.pincode = ztm.destination_pincode group by a.order_id ,Final_Marketplace ,a.reference_code ,a.marketplace ,a.source_pincode ,a.pincode ,warehouse_name ,a.order_timestamp ,order_date ,b.same_day_cut_off ,Order_Shipping_Last_Update_Date ,case when upper(coalesce(final_shipping_status,shipping_status,order_status)) in (\'DELIVERED\') then a.shipping_last_update_date end ,upper(coalesce(cf.CURRENT_STATUS,shipping_status)) ,order_status ,upper(case when (lower(a.order_status) like \'%cancel%\' or lower(final_shipping_status) like \'%cancel%\') then \'CANCELLED\' else coalesce(FINAL_SHIPPING_STATUS,shipping_status,order_status) end) ,coalesce(cf.courier_partner,a.COURIER) ,Source ,upper(coalesce(cf.PAYMENT_METHOD, a.payment_mode)) ,a.awb ,ztm.target_tat ,ztm.zone ,coalesce(cf.pickup_date,a.dispatch_date) ,coalesce(cf.delivery_date,a.DELIVERED_DATE) ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from MINDFUL_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            