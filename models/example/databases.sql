{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_CLICKPOST_FACT_ITEMS AS WITH DATA_FROM_CLICKPOST AS ( select waybill as AWb, replace(A.value:location,\'\',\'\"\') location, replace(A.value:clickpost_status_description,\'\',\'\"\')status, replace(A.value : created_at,\'\',\'\"\')::timestamp created_At, replace(A.value : remark,\'\',\'\"\') remark, replace(LATEST_STATUS:location,\'\',\'\"\') location_d, replace(LATEST_STATUS:clickpost_status_description,\'\',\'\"\')status_d, replace(LATEST_STATUS : created_at,\'\',\'\"\')::timestamp created_At_d, replace(LATEST_STATUS : remark,\'\',\'\"\') remark_d from (select * from(select *, row_number() over(partition by waybill order by _AIRBYTE_EMITTED_AT desc) rwn from sleepycat_db.maplemonk.sleepycat_clickpost_orders)where rwn=1 ), LATERAL FLATTEN (INPUT =>scans ,outer => true)A ) select og.awb as awb_number, op.location as order_location, op.created_At::timestamp as orderplaced_date, op.status as orderplaced_status, op.remark as order_remark, p_k.location as pickup_location, coalesce(p_k.created_At,i_t.created_At,db.created_At)::timestamp as pickup_date, \'PICKEDUP\' as pickup_status, p_k.remark as pickup_remark, i_t.location as intransit_location, i_t.created_At::timestamp as intransit_date, i_t.status as intransit_status, i_t.remark as intransit_remark, o_d.location as outfordelivery_location, o_d.created_At::timestamp as outfordelivery_date, o_d.status as outfordelivery_status, o_d.remark as outfordelivery_remark, coalesce(dl.location,dld.location_d) as delivery_location, coalesce(dl.created_At,dld.created_At_d)::timestamp as delivery_date, coalesce(dl.status,dld.status_d) as delivery_status, coalesce(dl.remark,dld.remark_d) as delivery_remark, cr.status_d as current_status from (select distinct awb from DATA_FROM_CLICKPOST) og left join (select * from (select awb, location, \'DELIVERED\' as status, created_At, remark, row_number() over(partition by awb order by created_At ) rw from DATA_FROM_CLICKPOST where lower(status)=\'delivered\')where rw=1) dl on og.awb = dl.awb left join (select * from (select awb, location, \'ORDER PLACED\' as status, created_At, remark,row_number() over(partition by awb order by created_At ) rw from DATA_FROM_CLICKPOST where lower(status) in (\'order placed\',\'orderplaced\')) where rw=1) op on og.awb = op.awb left join (select * from (select awb, location, \'intransit\' as status, created_At, remark, row_number() over(partition by awb order by created_At ) rw from DATA_FROM_CLICKPOST where lower(status) = \'intransit\')where rw=1)i_t on og.awb = i_t.awb left join (select * from (select awb, location, \'PICKEDUP\' as status, created_At, remark, row_number() over(partition by awb order by created_At ) rw from DATA_FROM_CLICKPOST where (lower(status) = \'pickedup\'))where rw=1)p_k on og.awb = p_k.awb left join (select * from (select awb, location, \'OUT FOR DELIVERY\' as status, created_At, remark, row_number() over(partition by awb order by created_At ) rw from DATA_FROM_CLICKPOST where lower(status) like \'%out%for%delivery%\' and not lower(status) like any(\'%awaited\',\'%needed\') )where rw=1) o_d on o_d.awb = og.awb left join ( select * from (select awb, location, \'intransit\' as status, created_At, remark, row_number() over(partition by awb order by created_At ) rw from DATA_FROM_CLICKPOST where lower(status) = \'destinationhubin\' or lower(status) = \'origincityout\' )where rw=1 )db on db.awb= og.awb left join ( select * from (select awb,location_d,status_d,created_At_d,remark_d,row_number() over(partition by awb order by created_At_d)rw from DATA_FROM_CLICKPOST where lower(status_d) = \'delivered\') where rw=1 )dld on dld.awb = og.awb left join (select * from (select awb,location_d,status_d,created_At_d,remark_d,row_number() over(partition by awb order by created_At_d)rw from DATA_FROM_CLICKPOST) where rw=1 )cr on cr.awb = og.awb ;",
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
                        