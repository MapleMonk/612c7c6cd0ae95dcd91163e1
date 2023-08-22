{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table skinq_db.Maplemonk.skinq_db_order_fulfillment_report as select a.channel as Marketing_Channel ,a.order_date as Date ,a.dispatch_date as manifest_date ,a.shipping_last_update_date ,a.delivered_date ,a.order_id ,a.courier ,a.sku ,a.shop_name as marketplace ,a.awb ,a.SHIPPING_STATUS shipment_partner_Status ,a.order_Status ,payment_mode ,datediff(\'hour\',order_date,current_timestamp()) as time_from_order_placement, case when lower(a.SHIPPING_STATUS) = \'delivered\' then 1 else 0 end as delivered_order, case when lower(a.SHIPPING_STATUS) = \'rto\' then 1 else 0 end as RTO_order, case when lower(a.SHIPPING_STATUS) = \'canceled\' then 1 else 0 end as cancelled_order, case when lower(a.SHIPPING_STATUS) = \'initiated\' then 1 else 0 end as Open_Order, case when lower(a.SHIPPING_STATUS) = \'in transit\' then 1 else 0 end as Intransit_Order, case when lower(a.SHIPPING_STATUS) = \'pending to dispatch\' then 1 else 0 end as Dispatch_Pending_Order, case when lower(a.SHIPPING_STATUS) = \'pending to process\' then 1 else 0 end as Process_Pending_Order, case when delivered_order = 1 then datediff(\'hour\',order_date,DELIVERED_DATE ) end as Delivery_Speed, case when delivered_order = 1 and datediff(\'hour\',order_date,DELIVERED_DATE ) < 24 then \'0-24 hours\' when delivered_order = 1 and datediff(\'hour\',order_date,DELIVERED_DATE ) < 48 then \'24-48 hours\' when delivered_order = 1 and datediff(\'hour\',order_date,DELIVERED_DATE ) < 72 then \'48-72 hours\' when delivered_order = 1 and datediff(\'hour\',order_date,DELIVERED_DATE ) < 96 then \'72-96 hours\' when delivered_order = 1 then \'> 96 hours\' else \'Not Delivered\' end as Delivery_time_flag, case when delivered_order = 1 then datediff(\'hour\',order_date,delivered_date ) else datediff(\'hour\',order_date ,current_timestamp() ) end as deliver_time, datediff(\'hour\',order_date::timestamp ,manifest_date) Dispatch_Speed, case when datediff(\'hour\',order_date::timestamp ,manifest_date) < 6 then \'0-6 hours\' when datediff(\'hour\',order_date::timestamp ,manifest_date) < 12 then \'6-12 hours\' when datediff(\'hour\',order_date::timestamp ,manifest_date) < 18 then \'12-18 hours\' when datediff(\'hour\',order_date::timestamp ,manifest_date) < 24 then \'18-24 hours\' when datediff(\'hour\',order_date::timestamp ,manifest_date) < 48 then \'24-36 hours\' when datediff(\'hour\',order_date::timestamp ,manifest_date) < 96 then \'36-48 hours\' when manifest_date is not NULL then \'> 48 hours\' when manifest_date is NULL then \'Not Dispatched\' end as Dispatch_Speed_flag, case when manifest_date is null then datediff(\'hour\',order_date::timestamp ,current_timestamp() ) else datediff(\'hour\',order_date::timestamp ,manifest_date) end as dispatch_time, case when manifest_date is NULL then \'Before Dispatch\' else \'After Dispatch\' end as dispatch_status from skinq_db.Maplemonk.skinq_db_sales_consolidated a",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from skinq_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        