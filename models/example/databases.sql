{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE SNITCH_DB.SNITCH.SLEEPYCAT_DB_CLICKPOST_FACT_ITEMS AS WITH lineitemwithc_id_temp as ( select t3.* ,p.zone as zone ,p.delivery_city ,p.delivery_state from ( select cast(t2.customer_id_final as varchar) as customer_id_final ,t2.pincode ,t1.* from snitch_db.snitch.ORDER_LINEITEMS_FACT t1 left join snitch_db.snitch.ORDERS_fact t2 on t1.order_id = t2.order_id )t3 left join snitch_db.maplemonk.pincodemappingzoneupdatedsnitch p on t3.pincode = p.delivery_postcode ), DATA_FROM_CLICKPOST AS ( select waybill as AWb, replace(A.value:location,\'\',\'\"\') location, replace(A.value:clickpost_status_description,\'\',\'\"\')status, replace(A.value : created_at,\'\',\'\"\')::timestamp created_At, replace(A.value : remark,\'\',\'\"\') remark, replace(LATEST_STATUS:location,\'\',\'\"\') location_d, replace(LATEST_STATUS:clickpost_status_description,\'\',\'\"\')status_d, replace(LATEST_STATUS : created_at,\'\',\'\"\')::timestamp created_At_d, replace(LATEST_STATUS : remark,\'\',\'\"\') remark_d, case when scans=[] then 1 else 0 end as flag, replace(ADDITIONAL : courier_partner_edd,\'\',\'\"\') edd, replace(ADDITIONAL : courier_partner_name,\'\',\'\"\') courier_partner, replace(ADDITIONAL : invoice_number,\'\',\'\"\') invoice_number, replace(ADDITIONAL : order_id,\'\',\'\"\') order_id, replace(ADDITIONAL : edd: max_sla,\'\',\'\"\') max_sla, replace(ADDITIONAL : edd: min_sla,\'\',\'\"\') min_sla, ADDITIONAL : cod_value as Cod_value, ADDITIONAL:sku as sku_list from (select * from(select *, row_number() over(partition by waybill order by _AIRBYTE_EMITTED_AT desc) rwn from SNITCH_DB.MAPLEMONK.clickpost_orders)where rwn=1 ), LATERAL FLATTEN (INPUT =>scans ,outer => true)A ), STRUCTURED_DATA AS( select og.awb as awb_number, op.location as order_location, op.created_At::timestamp as orderplaced_date, op.status as orderplaced_status, op.remark as order_remark, p_k.location as pickup_location, coalesce(p_k.created_At,i_t.created_At,db.created_At)::timestamp as pickup_date, \'PICKEDUP\' as pickup_status, p_k.remark as pickup_remark, i_t.location as intransit_location, i_t.created_At::timestamp as intransit_date, i_t.status as intransit_status, i_t.remark as intransit_remark, o_d.location as outfordelivery_location, o_d.created_At::timestamp as outfordelivery_date, o_d.status as outfordelivery_status, o_d.remark as outfordelivery_remark, rt.location as rto_location, rt.created_At::timestamp as rto_date, rt.status as rto_status, rt.remark as rto_remark, coalesce(dl.location,dld.location_d) as delivery_location, coalesce(dl.created_At,dld.created_At_d)::timestamp as delivery_date, coalesce(dl.status,dld.status_d) as delivery_status, coalesce(dl.remark,dld.remark_d) as delivery_remark, cr.status_d as pre_current_status, case when lower(pre_current_status) in (\'rto-shipmentdelay\',\'rto-delivered\',\'rto-intransit\',\'rto-requested\',\'rto-marked\',\'rto-contactcustomercare\',\'rto-outfordelivery\',\'rto-failed\') then \'RTO\' when lower(pre_current_status) in (\'pickedup\',\'destinationhubin\',\'outforpickup\',\'contactcustomercare\',\'shipmentdelayed\',\'origincityin\',\'outfordelivery\',\'intransit\',\'origincityout\',\'pickuppending\',\'awb registered\',\'shipmentheld\',\'pickupfailed\') then \'INTRANSIT\' when lower(pre_current_status) in (\'delivered\') then \'DELIVERED\' when lower(pre_current_status) in (\'cancelled\') then \'CANCELLED\' when lower(pre_current_status) in (\'orderplaced\') then \'ORDER_PLACED\' when lower(pre_current_status) in (\'faileddelivery\',\'notserviceable\') then \'UNDELIVERED\' when lower(pre_current_status) in (\'lost\',\'damaged\') then \'LOST\' END as current_status, cr.created_At_d as current_d, cr.flag as flag, ad.edd, ad.courier_partner, ad.invoice_number, ad.order_id, case when ad.cod_value > 0 then \'COD\' else \'PREPAID\' end as Payment_method, max_sla, min_sla, og.sku_list from (select distinct awb,max_sla,min_sla,sku_list from DATA_FROM_CLICKPOST) og left join (select * from (select awb, location, \'DELIVERED\' as status, created_At, remark, row_number() over(partition by awb order by created_At ) rw from DATA_FROM_CLICKPOST where lower(status)=\'delivered\')where rw=1) dl on og.awb = dl.awb left join (select * from (select awb, location, \'ORDER PLACED\' as status, created_At, remark,row_number() over(partition by awb order by created_At ) rw from DATA_FROM_CLICKPOST where lower(status) in (\'order placed\',\'orderplaced\')) where rw=1) op on og.awb = op.awb left join (select * from (select awb, location, \'intransit\' as status, created_At, remark, row_number() over(partition by awb order by created_At ) rw from DATA_FROM_CLICKPOST where lower(status) = \'intransit\')where rw=1)i_t on og.awb = i_t.awb left join (select * from (select awb, location, \'PICKEDUP\' as status, created_At, remark, row_number() over(partition by awb order by created_At ) rw from DATA_FROM_CLICKPOST where (lower(status) = \'pickedup\'))where rw=1)p_k on og.awb = p_k.awb left join (select * from (select awb, location, \'OUT FOR DELIVERY\' as status, created_At, remark, row_number() over(partition by awb order by created_At ) rw from DATA_FROM_CLICKPOST where lower(status) like \'outfordelivery\' )where rw=1) o_d on o_d.awb = og.awb left join ( select * from (select awb, location, \'intransit\' as status, created_At, remark, row_number() over(partition by awb order by created_At ) rw from DATA_FROM_CLICKPOST where lower(status) = \'destinationhubin\' or lower(status) = \'origincityout\' )where rw=1 )db on db.awb= og.awb left join ( select * from (select awb,location_d,status_d,created_At_d,remark_d,row_number() over(partition by awb order by created_At_d)rw from DATA_FROM_CLICKPOST where lower(status_d) = \'delivered\') where rw=1 )dld on dld.awb = og.awb left join (select * from (select flag,awb,location_d,status_d,created_At_d,remark_d,row_number() over(partition by awb order by created_At_d)rw from DATA_FROM_CLICKPOST) where rw=1 )cr on cr.awb = og.awb left join (select * from (select awb,edd,courier_partner,invoice_number,order_id,cod_value ,row_number() over(partition by awb order by 1)rw from DATA_FROM_CLICKPOST) where rw=1 )ad on ad.awb = og.awb left join (select * from (select awb, location, \'RTO\' as status, created_At, remark,row_number() over(partition by awb order by created_At ) rw from DATA_FROM_CLICKPOST where upper(status) like \'RTO%\') where rw=1) rt on og.awb = rt.awb ), calculate_metrics AS( select *, case when lower(current_status) = \'delivered\' then \'DELIVERED\' when lower(current_status) = \'intransit\' then \'INTRANSIT\' when lower(current_status) = \'outfordelivery\' then \'OUTFORDELIVERY\' when lower(current_status) like \'rto%\' then \'RTO\' else \'UNDELIVERED\' end as STATUS, DATEDIFF(\'hour\',pickup_date , outfordelivery_date) AS P2A, DATEDIFF(\'hour\',pickup_date , delivery_date) AS P2D, (DATEDIFF(\'hour\',order_date ,pickup_date)/24)::int AS O2P1, (DATEDIFF(\'hour\',order_date ,assigned_date)/24)::int AS O2A1, case when delivery_date is not null and DATEDIFF(\'day\',pickup_date , delivery_date) <= max_sla and DATEDIFF(\'day\',pickup_date , delivery_date) >= min_sla then 1 else 0 end as in_time, case when o2p1=0 then \'D-0\' when o2p1=1 then \'D-1\' when o2p1=2 then \'D-2\' when o2p1=3 then \'D-3\' when o2p1>3 then \'3+\' end as O2P, case when o2A1=0 then \'D-0\' when o2A1=1 then \'D-1\' when o2A1=2 then \'D-2\' when o2A1=3 then \'D-3\' when o2A1>3 then \'3+\' end as O2A, case when outfordelivery_date::date = delivery_date::date then 1 when outfordelivery_date::date != delivery_date::date then 0 end as FASR, case when invoice_number like \'SAPLEMIZA%\' then \'EMIZA\' when invoice_number like \'SAPLYLK%\' then \'YLK\' when invoice_number like \'SAPLHSK%\' then \'HSK\' end as warehouse, COUNT(*) OVER (PARTITION BY DATE_TRUNC(\'month\', order_date::date) ORDER BY 1) as total from structured_data sd left join (select order_name as od,max(pincode)as pincode,max(order_date) as order_date,max(mode_of_payment) as payment_mode,max(zone) as zone from lineitemwithc_id_temp group by 1) p on sd.order_id = p.od left join (select distinct cod as cod1, zone as zone1, replace(sh.value : awb ,\'\"\',\'\') as awb, TRY_TO_TIMESTAMP(REPLACE(SR.CREATED_AT, \'\"\', \'\'), \'DD MON YYYY, HH12:MI PM\') as assigned_date, customer_pincode, payment_method as pm from snitch_db.maplemonk.snitch_shiprocket_new_orders sr,LATERAL FLATTEN(input =>sr.products) pr,LATERAL FLATTEN(input => sr.shipments) sh)sl on sl.awb = sd.awb_number ) select * from calculate_metrics",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from snitch_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        