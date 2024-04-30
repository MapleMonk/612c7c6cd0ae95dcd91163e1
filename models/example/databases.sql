{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create table if not exists SELECT_DB.MAPLEMONK.marketplace_fulfillment_cutoff (Marketplace varchar,Same_Day_Cut_Off varchar); Insert into SELECT_DB.MAPLEMONK.marketplace_fulfillment_cutoff values (\'Myntra PPMP\',\'11:00 AM\'), (\'Flipkart\',\'12:00 PM\'), (\'Amazon\',\'2:00 PM\'),(\'Others\',\'4:00 PM\'); create or replace table SELECT_DB.MAPLEMONK.SELECT_DB_Order_Fulfillment_Report as select reference_code ,order_id ,order_date ,batch_id ,awb ,saleorderitemcode orderitem_code ,sku ,sku_code ,easy_ecom_sku ,a.city ,a.state ,case when lower(a.marketplace) like \'offline\' and reference_code like \'CORP%\' then \'OFFLINE - CORPORATE\' when lower(a.marketplace) like \'offline\' and reference_code like \'%_RS%\' then \'OFFLINE - RESHIPMENT\' when lower(a.marketplace) like \'offline\' and reference_code like \'%_RP%\' then \'OFFLINE - REPLACEMENT\' when lower(a.marketplace) like \'offline\' then \'OFFLINE - OTHERS\' else a.marketplace end Final_Marketplace ,a.marketplace marketplace ,customer_id_final ,name ,email ,phone ,warehouse warehouse_name ,upper(order_status) order_status ,upper(shipping_status) shipping_status ,upper(case when lower(order_status) like \'%cancel%\' then \'CANCELLED\' else coalesce(FINAL_SHIPPING_STATUS,shipping_status,order_status) end) final_status ,dateadd(day,2,order_date) Target_Dispatch_Date ,dateadd(day,6,order_date) Target_Delivery_Date ,dispatch_date as Order_Manifest_date ,shipping_last_update_date as Order_Shipping_Last_Update_Date ,case when final_status IN (\'DELIVERED\', \'RTO\',\'RETURNED\') then a.shipping_last_update_date end order_delivered_date ,COURIER ,Source ,payment_mode ,pincode ,case when upper(final_status) in (\'DELIVERED\') then 1 else 0 end as delivered_order ,case when upper(final_status) = \'ORDER YET TO SYNC\' then 1 else 0 end as Order_yet_to_Sync_Order ,case when upper(final_status) = \'CONFIRMED\' then 1 else 0 end as Confirmed_Order ,case when upper(final_status) = \'ASSIGNED\' then 1 else 0 end as Assigned_Order ,case when upper(final_status) = \'OPEN\' then 1 else 0 end as Open_Order ,case when upper(final_status) = \'PRINTED\' then 1 else 0 end as Printed_Order ,case when upper(final_status) = \'PENDING\' then 1 else 0 end as Pending_Order ,case when upper(final_status) = \'IN TRANSIT\' then 1 else 0 end as In_Transit_order ,case when upper(final_status) = \'RTO\' then 1 else 0 end as RTO_Order ,case when upper(final_status) = \'RTS\' then 1 else 0 end as RTS_Order ,case when upper(final_status) = \'RETURNED\' then 1 else 0 end as Returned_order ,case when upper(final_status) = \'PICKUP ERROR\' then 1 else 0 end as Pickup_Error ,case when upper(final_status) = \'LOST\' then 1 else 0 end as Lost_Order ,case when upper(final_status) =\'EXCEPTION\' then 1 else 0 end as Exception_Order ,case when upper(final_status) = \'MISROUTED\' then 1 else 0 end as Misrouted_Order ,case when upper(final_status) = \'UNDERPROCESS\' then 1 else 0 end as Underprocess_Order ,case when upper(final_status) = \'DAMAGED\' then 1 else 0 end as Damaged_Order ,case when upper(final_status) = \'CANCELLED\' then 1 else 0 end as cancelled_order ,case when lower(final_status) in (\'delivered\',\'rto\',\'returned\',\'cancelled\') then datediff(\'hour\',order_date::timestamp ,Order_Shipping_Last_Update_Date::timestamp ) else datediff(\'hour\',order_date::timestamp ,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp) end as Time_From_Order ,case when Time_from_Order <= 24 then \'1 DAY\' when Time_from_Order <= 48 then \'2 DAYS\' when Time_from_Order <= 72 then \'3 DAYS\' when Time_from_Order <= 96 then \'4 DAYS\' when Time_from_Order <= 120 then \'5 DAYS\' when Time_from_Order <= 144 then \'6 DAYS\' when Time_from_Order <= 144 then \'7 DAYS\' else \'8. More than a week\' end as Time_From_Order_Category ,datediff(\'hour\',order_date::timestamp ,order_manifest_date::timestamp) Dispatch_Speed ,datediff(\'hour\',order_date::timestamp ,order_delivered_date::timestamp) o2d_delivery_Speed ,datediff(\'hour\',order_manifest_date::timestamp ,order_delivered_date::timestamp) s2d_delivery_Speed ,upper(case when Dispatch_Speed < 2 then \'0 - 2 hours\' when Dispatch_Speed < 6 then \'2 - 6 hours\' when Dispatch_Speed < 12 then \'6 - 12 hours\' when Dispatch_Speed < 24 then \'12 - 24 hours\' when Dispatch_Speed < 48 then \'24 - 48 hours\' when Dispatch_Speed < 72 then \'48 - 72 hours\' when Dispatch_Speed < 96 then \'72 - 96 hours\' when order_manifest_date is not NULL then \'> 96 hours\' when order_manifest_date is NULL and not(lower(final_status) like \'%cancel%\') then \'Not Dispatched\' else \'CANCELLED\' end) as Dispatch_Speed_flag ,case when order_manifest_date is NULL and lower(marketplace) like any (\'%fba%\',\'%vendor%\') and lower(final_status) like \'delivered\' then \'DISPATCHED - NO MANIFEST DATE\' when order_manifest_date is NULL and lower(final_status) like any (\'in transit\',\'delivered\',\'rto\',\'returned\',\'out for delivery\',\'delivery attempt\',\'lost\',\'returned\') then \'DISPATCHED - NO MANIFEST DATE\' when not(lower(final_status) like \'%cancel%\') and DISPATCH_DATE is null and convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp >= Target_Dispatch_Date then \'NOT DISPATCHED - DELAYED\' when not(lower(final_status) like \'%cancel%\') and DISPATCH_DATE is null and convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp < Target_Dispatch_Date then \'NOT DISPATCHED - IN TIME\' when DISPATCH_DATE is not null and lower(final_status) like any (\'%shipment created%\',\'%pickup scheduled%\', \'%ready to dispatch%\') and convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp >= Target_Dispatch_Date then \'NOT DISPATCHED - DELAYED\' when DISPATCH_DATE is not null and lower(final_status) like any (\'%shipment created%\',\'%pickup scheduled%\', \'%ready to dispatch%\') and convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp < Target_Dispatch_Date then \'NOT DISPATCHED - IN TIME\' when not(lower(final_status) like \'%cancel%\') and DISPATCH_DATE <= Target_Dispatch_Date then \'DISPATCHED - ON TIME\' when not(lower(final_status) like \'%cancel%\' ) and DISPATCH_DATE > Target_Dispatch_Date then \'DISPATCHED - DELAYED\' end Final_Dispatch_Bucket ,case when lower(final_status) in (\'delivered\',\'rto\',\'returned\') then datediff(\'hour\',order_manifest_date::timestamp , order_shipping_last_update_date::timestamp ) end as Delivery_Speed ,case when lower(final_status) in (\'delivered\',\'rto\',\'returned\') then datediff(\'hour\',order_manifest_date::timestamp ,order_shipping_last_update_date::timestamp ) when not(lower(final_status) like \'%cancel%\') then datediff(\'hour\',order_manifest_date::timestamp ,convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp) end as deliver_time ,case when lower(final_status) in (\'delivered\',\'rto\',\'returned\') and datediff(\'hour\',order_manifest_date::timestamp,order_shipping_last_update_date::timestamp ) < 24 then \'< 24 hours\' when lower(final_status) in (\'delivered\',\'rto\',\'returned\') and datediff(\'hour\',order_manifest_date::timestamp,order_shipping_last_update_date::timestamp ) < 48 then \'24-48 hours\' when lower(final_status) in (\'delivered\',\'rto\',\'returned\') and datediff(\'hour\',order_manifest_date::timestamp,order_shipping_last_update_date::timestamp ) < 72 then \'48-72 hours\' when lower(final_status) in (\'delivered\',\'rto\',\'returned\') and datediff(\'hour\',order_manifest_date::timestamp,order_shipping_last_update_date::timestamp ) < 96 then \'72-96 hours\' when lower(final_status) in (\'delivered\',\'rto\',\'returned\') and datediff(\'hour\',order_manifest_date::timestamp,order_shipping_last_update_date::timestamp ) >= 96 then \'> 96 hours\' else \'Not Delivered\' end as delivery_time_flag ,case when not(lower(final_status) like \'%cancel%\') and order_delivered_date is null and convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp >= Target_Delivery_Date then \'NOT DELIVERED - DELAYED\' when not(lower(final_status) like \'%cancel%\') and order_delivered_date is null and convert_timezone(\'America/Los_Angeles\', \'Asia/Kolkata\', current_timestamp())::timestamp < Target_Delivery_Date then \'NOT DELIVERED - IN TIME\' when not(lower(final_status) like \'%cancel%\') and order_delivered_date <= Target_Delivery_Date then \'DELIVERED - ON TIME\' when not(lower(final_status) like \'%cancel%\') and order_delivered_date > Target_Delivery_Date then \'DELIVERED - DELAYED\' end Final_Delivery_Bucket ,case when order_manifest_date is NULL and lower(marketplace) like any (\'%fba%\',\'%vendor%\') and lower(final_status) like \'delivered\' then \'DISPATCHED\' when order_manifest_date is NULL and lower(final_status) like any (\'in transit\',\'delivered\',\'rto\',\'returned\',\'out for delivery\',\'delivery attempt\',\'lost\',\'returned\') then \'DISPATCHED\' when order_manifest_date is NULL and not(lower(final_status) like \'%cancel%\') then \'NOT DISPATCHED\' when order_manifest_date is not NULL and lower(final_status) like any (\'%shipment created%\',\'%pickup scheduled%\', \'%ready to dispatch%\') then \'NOT DISPATCHED\' when not(lower(final_status) like \'%cancel%\') then \'DISPATCHED\' else \'CANCELLED\' end as dispatch_status ,case when order_delivered_date is NULL and lower(marketplace) like any (\'%fba%\',\'%vendor%\') and lower(final_status) like \'delivered\' then \'DELIVERED\' when order_delivered_date is NULL and not(lower(final_status) like \'%cancel%\') then \'NOT DELIVERED\' when not(lower(final_status) like \'%cancel%\') then \'DELIVERED\' else \'CANCELLED\' end as delivery_status ,sum(quantity) QUANTITY ,sum(selling_price) selling_price from SELECT_DB.MAPLEMONK.SELECT_DB_sales_consolidated a group by reference_code ,batch_id ,order_id ,order_date ,awb ,saleorderitemcode ,SKU ,case when lower(a.marketplace) like \'offline\' and reference_code like \'CORP%\' then \'OFFLINE - CORPORATE\' when lower(a.marketplace) like \'offline\' and reference_code like \'%_RS%\' then \'OFFLINE - RESHIPMENT\' when lower(a.marketplace) like \'offline\' and reference_code like \'%_RP%\' then \'OFFLINE - REPLACEMENT\' when lower(a.marketplace) like \'offline\' then \'OFFLINE - OTHERS\' else a.marketplace end ,marketplace ,customer_id_final ,name ,email ,phone ,warehouse ,upper(order_status) ,upper(shipping_status) ,upper(case when lower(order_status) like \'%cancel%\' then \'CANCELLED\' else coalesce(FINAL_SHIPPING_STATUS,shipping_status,order_status) end) ,dispatch_date ,shipping_last_update_date ,COURIER ,Source ,payment_mode ,sku_code ,easy_ecom_sku ,a.city ,a.state ,pincode ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from SELECT_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        