{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table hox_db.maplemonk.HOX_BLANKO_CLICKPOST_FACT_ITEMS as WITH DATA_FROM_CLICKPOST AS ( select waybill as Awb, replace(A.value:location,\'\',\'\"\') location, replace(A.value:clickpost_status_description,\'\',\'\"\')status, replace(A.value:clickpost_status_bucket_description,\'\',\'\"\')status_bucket, replace(A.value:status,\'\',\'\"\')status_bucket_new, replace(A.value : \"timestamp\",\'\',\'\"\')::timestamp created_At_new, replace(A.value : created_at,\'\',\'\"\')::timestamp created_At, replace(A.value : remark,\'\',\'\"\') remark, replace(LATEST_STATUS:location,\'\',\'\"\') location_d, replace(LATEST_STATUS:clickpost_status_description,\'\',\'\"\')status_d_verified, replace(LATEST_STATUS : created_at,\'\',\'\"\')::timestamp created_At_d, replace(LATEST_STATUS : remark,\'\',\'\"\') remark_d, replace(LATEST_STATUS:status,\'\',\'\"\') status_d, replace(LATEST_STATUS:clickpost_status_bucket_description,\'\',\'\"\')status_d_new, replace(LATEST_STATUS : \"timestamp\",\'\',\'\"\')::timestamp created_At_d_new, replace(ADDITIONAL: courier_partner_edd , \'\"\',\'\'):: date as courier_partner_edd, replace(ADDITIONAL: courier_partner_name , \'\"\',\'\') as courier_partner_name, replace(ADDITIONAL: drop_city , \'\"\',\'\') as drop_city, replace(ADDITIONAL: pickup_city , \'\"\',\'\') as pickup_city, replace(ADDITIONAL: order_id , \'\"\',\'\') as order_id from (select * from(select *, row_number() over(partition by waybill order by _AIRBYTE_EMITTED_AT desc) rwn from hox_db.maplemonk.clickpost_data_orders)where rwn=1), LATERAL FLATTEN (INPUT =>scans ,outer => true)A order by 1 ), recent_data as ( Select distinct Awb, CREATED_AT_D_NEW as last_updated_time, date(CREATED_AT_D_NEW) as last_updated_date, datediff(day,last_updated_date, current_Date)as DSLU, location_d as final_status_location, status_d_verified as recent_CP_status from ( select *, row_number() over(partition by Awb order by CREATED_AT_D_NEW desc)as rs from DATA_FROM_CLICKPOST )as o where rs = 1 order by 1 ), first_data as ( Select distinct Awb, dateadd(minute, 330, created_At_d) as order_created_time, date(dateadd(minute, 330, created_At_d)) as AWB_created_date from ( select *, row_number() over(partition by Awb order by created_At_d)as rs from DATA_FROM_CLICKPOST )as o where rs = 1 order by 1 ), pickedup_status as ( Select distinct Awb, created_At_new as dispatch_time, date(created_At_new) as dispatch_date from ( select *, row_number() over(partition by Awb order by created_At_new)as rs from DATA_FROM_CLICKPOST where replace(lower(status), \' \', \'\') in (\'pickedup\',\'picked\', \'pickup\', \'pickdone\', \'dispatched\', \'dispatch\', \'shipped\', \'pickupdone\') )as o where rs = 1 order by 1 ), pickedup_status_bucket as ( Select distinct Awb, created_At_new as dispatch_time_new, date(created_At_new) as dispatch_date_new from ( select *, row_number() over(partition by Awb order by created_At_new)as rs from DATA_FROM_CLICKPOST where replace(lower(status_bucket_new), \' \', \'\') in (\'pickedup\',\'picked\', \'pickup\', \'pickdone\', \'dispatched\', \'dispatch\', \'shipped\', \'pickupdone\') )as o where rs = 1 order by 1 ), pickedup_status_bucket_new as ( Select distinct Awb, created_At_new as dispatch_time_news, date(created_At_new) as dispatch_date_news from ( select *, row_number() over(partition by Awb order by created_At_new)as rs from DATA_FROM_CLICKPOST where replace(lower(status_bucket), \' \', \'\') in (\'pickedup\',\'picked\', \'pickup\', \'pickdone\', \'dispatched\', \'dispatch\', \'shipped\', \'pickupdone\') )as o where rs = 1 order by 1 ), destination_status as ( Select distinct Awb, created_At_new as destination_City_timestamp from ( select *, row_number() over(partition by Awb order by created_At_new)as rs from DATA_FROM_CLICKPOST where replace(lower(status), \' \', \'\') in (\'destinationhubin\', \'receivedatforwardhub\') )as o where rs = 1 order by 1 ), destination_status_old as ( Select distinct Awb, created_At_new as destination_City_timestampss from ( select *, row_number() over(partition by Awb order by created_At_new)as rs from DATA_FROM_CLICKPOST where replace(lower(status_bucket_new), \' \', \'\') in (\'reachedatdestination\', \'reachedatdestinationdestinationcityinscan\', \'pendingdestinationcityinscan\', \'receivedatforwardhub\', \'receivedatdelhiveryhub\') )as o where rs = 1 order by 1 ), destination_status_new as ( Select distinct Awb, created_At_new as destination_City_timestamps from ( select *, row_number() over(partition by Awb order by created_At_new)as rs from DATA_FROM_CLICKPOST where replace(lower(remark), \' \', \'\') in (\'destinationhubin\', \'receivedatforwardhub\', \'receivedatdelhiveryhub\') )as o where rs = 1 order by 1 ), out_for_Delivery_Data as ( Select distinct Awb, min(case when rs = 1 then created_At_new end) as OFD_1st_time, min(case when rs = 2 then created_At_new end) as OFD_2nd_time, min(case when rs = 3 then created_At_new end) as OFD_3rd_time, min(case when rs = 4 then created_At_new end) as OFD_4th_time, min(case when rs = 5 then created_At_new end) as OFD_5th_time, max(case when created_At_new is not null then rs end) as Ofd_attempts from ( select *, row_number() over(partition by Awb order by created_At_new)as rs from DATA_FROM_CLICKPOST where replace(lower(status), \' \', \'\') = \'outfordelivery\' )as o group by 1 order by 1 ), failed_Delivery_Data as ( Select distinct Awb, min(case when rs = 1 then created_At_new end) as NDR_1st_time, min(case when rs_desc = 1 then created_At_new end) as NDR_last_time, min(case when rs = 1 then remark end) as NDR_1st_remark, min(case when rs_desc = 1 then remark end) as NDR_last_remark, max(case when created_At_new is not null then rs end) as NDR_attempts from ( select *, row_number() over(partition by Awb order by created_At_new)as rs, row_number() over(partition by Awb order by created_At_new desc)as rs_desc, from DATA_FROM_CLICKPOST where replace(lower(status), \' \', \'\') = \'faileddelivery\' )as o group by 1 order by 1 ), returned_Data as ( Select distinct Awb, min(case when rs = 1 then created_At_new end) as RTO_1st_time, min(case when rs_desc = 1 then created_At_new end) as RTO_last_time, max(case when replace(lower(status), \'-\', \'\') = \'rtodelivered\' then 1 else 0 end) as RTO_Delivered_flag from ( select *, row_number() over(partition by Awb order by created_At_new)as rs, row_number() over(partition by Awb order by created_At_new desc)as rs_desc, from DATA_FROM_CLICKPOST where replace(lower(status_bucket), \' \', \'\') = \'returned\' )as o group by 1 order by 1 ), delivered_Data as ( Select distinct Awb, date(created_At_new) as delivered_Date from ( select *, row_number() over(partition by Awb order by created_At_new)as rs from DATA_FROM_CLICKPOST where replace(lower(status), \' \', \'\') = \'delivered\' )as o group by 1,2 order by 1 ) select o.*, last_updated_time, last_updated_date,DSLU, final_status_location, recent_CP_status , order_created_time, AWB_created_date, coalesce(destination_City_timestamp, destination_City_timestampss, destination_City_timestamps)as destination_City_timestamp, coalesce(dispatch_time,dispatch_time_new, dispatch_time_news) as dispatch_time, coalesce(dispatch_date, dispatch_date_new, dispatch_date_news) as dispatch_date, OFD_1st_time, OFD_2nd_time, OFD_3rd_time, OFD_4th_time, OFD_5th_time, Ofd_attempts, NDR_1st_time, NDR_last_time, NDR_1st_remark, NDR_last_remark, NDR_attempts, RTO_1st_time, RTO_last_time, RTO_Delivered_flag, delivered_Date from ( select distinct Awb, courier_partner_edd, courier_partner_name, drop_city, pickup_city, order_id from DATA_FROM_CLICKPOST )as o left join recent_data a on o.awb = a.awb left join first_data b on o.awb = b.awb left join destination_status c on o.awb = c.awb left join out_for_Delivery_Data d on o.awb = d.awb left join failed_Delivery_Data e on o.awb = e.awb left join returned_Data f on o.awb = f.awb left join pickedup_status g on o.awb = g.awb left join pickedup_status_bucket h on o.awb = h.awb left join pickedup_status_bucket_new z on o.awb = z.awb left join destination_status_old y on o.awb = y.awb left join destination_status_new x on o.awb = x.awb left join delivered_Data del on o.awb = del.awb",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from HOX_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        