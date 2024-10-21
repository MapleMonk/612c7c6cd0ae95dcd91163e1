{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.sku_group_sample_to_live as ( with live_date as ( select a.id, a.status, a.published_at as live_date, b.price, b.sku_group, b.price from snitch_db.maplemonk.shopifyindia_new_products a left join snitch_db.maplemonk.availability_master_v2 b on a.id = b.id ), first_order_date as ( select sku_group, min(order_timestamp::date) as first_order_date from snitch_db.maplemonk.fact_items_snitch group by 1 ), final_first_live_date as ( select upper(trim(a.sku_group)) as sku_group, a.status, case when a.live_date <= b.first_order_date then a.live_date else b.first_order_date end as live_date_final from live_date a left join first_order_date b on a.sku_group = b.sku_group ), catalog AS ( SELECT upper(trim(SKU_GROUP)) as sku_group, status, latest_inward_date::date as latest_inward_date, new_inward_flag, ifnull(SUM(units_on_hand),0) AS units_on_hand FROM snitch_db.maplemonk.Inventory_planning_summary_snitch WHERE new_inward_flag = \'New\' GROUP BY SKU_GROUP, new_inward_flag, status, latest_inward_date HAVING SUM(units_on_hand) > 0 ), PSLJ AS( SELECT upper(trim(a.SKU)) as sku_group, a.\"Factory_\'\", a.owner as editor_owner, TO_CHAR(TRY_TO_DATE(a.Sample_Received_Date, \'DD/MM/YYYY\'), \'YYYY-MM-DD\') as Sample_Received_Date, a.Photoshoot_Status, a.IMAGE_SELECTION, TO_CHAR(TRY_TO_DATE(a.Recevied_Date_, \'DD/MM/YYYY\'), \'YYYY-MM-DD\') as edit_received_date, TO_CHAR(TRY_TO_DATE(a.Completion_Date, \'DD/MM/YYYY\'), \'YYYY-MM-DD\') as edit_completed_date, TO_CHAR(TRY_TO_DATE(a.\"Catalogix Batch Share Date\", \'DD/MM/YYYY\'), \'YYYY-MM-DD\') as catalogix_sent_timestamp, TO_CHAR(TRY_TO_DATE(a.\"Catalogix Batch Recevied Date\", \'DD/MM/YYYY\'), \'YYYY-MM-DD\') as catalogix_received_timestamp, TO_CHAR(TRY_TO_DATE(a.final_shoot_date, \'DD/MM/YYYY\'), \'YYYY-MM-DD\') as shoot_date, TO_CHAR(TRY_TO_DATE(a.live_on_shopify_date, \'DD/MM/YYYY\'), \'YYYY-MM-DD\') as live_on_shopify_date, tags FROM snitch_db.maplemonk.pslj a ), RTS AS ( SELECT upper(trim(SKU)) as sku_group, CLASS, \'ready_to_dispatch\' as status, REPEAT, VENDOR, WAEHOUSE, DESCIPTON, TO_CHAR(TRY_TO_DATE(DELIVEY_DATE_, \'DD/MM/YYYY\'), \'YYYY-MM-DD\') as DELIVERY_DATE, online_qty, QC_STATUS, QC_REPORT FROM snitch_db.maplemonk.catalog_main where repeat = \'NO\' and (to_date(DELIVEY_DATE_, \'DD/MM/YYYY\') >= dateadd(day,-1,current_date) or to_date(DELIVEY_DATE_, \'DD/MM/YYYY\') is null) ), main_data as ( select coalesce(a.sku_group,c.SKU_GROUP,r.sku_group) as sku_group, a.Sample_Received_Date::date as sample_received_date, l.live_date_final::date as live_date, a.Photoshoot_Status, a.shoot_date::date as shoot_date, a.edit_received_date, a.editor_owner, a.edit_completed_date, a.catalogix_sent_timestamp, a.catalogix_received_timestamp, r.delivery_date, cast(coalesce(c.units_on_hand,r.online_qty,0) as number) as units_on_hand, c.latest_inward_date as inward_date, case when a.sku_group is null and c.sku_group is not null then \'not_in_PSLJ\' else \'present_in_PSLJ\' end as pslj_status, coalesce(l.status,c.status,r.status) as status, DATEDIFF(day, a.Sample_Received_Date, l.live_date_final) AS sr_live_tat, DATEDIFF(day, a.Sample_Received_Date, a.shoot_date) AS shoot_tat, datediff(day,a.shoot_date,a.edit_received_date) as shootdone_editreceived_tat, datediff(day,a.edit_received_date,a.edit_completed_date) as edit_tat, datediff(day,a.edit_completed_date,a.catalogix_sent_timestamp) as editdone_catalogixsent_tat, datediff(day,a.catalogix_sent_timestamp,a.catalogix_received_timestamp) as catalogix_tat, datediff(day,a.catalogix_received_timestamp,l.live_date_final) as live_tat, a.tags FROM PSLJ a FULL OUTER JOIN catalog c ON a.sku_group = c.sku_group FULL OUTER JOIN RTS r ON COALESCE(a.sku_group, c.SKU_GROUP) = r.sku_group FULL OUTER JOIN final_first_live_date l ON COALESCE(a.sku_group, c.SKU_GROUP, r.sku_group) = l.sku_group ), main_data2 as ( select *, CASE when status = \'active\' or live_date is not null then \'NO_ISSUES\' when status = \'ready_to_dispatch\' and shoot_date is null then \'READY_TO_DISPATCH_SHOOT_NOT_DONE\' when status = \'ready_to_dispatch\' and shoot_date is not null then \'READY_TO_DISPATCH_SHOOT_DONE\' when sample_received_date is null and shoot_date is null then \'SAMPLE_NOT_RECEIVED\' when shoot_date is null then \'SHOOT_NOT_DONE\' when shoot_date is not null and edit_received_date is null then \'NOT_SEND_TO_EDIT\' when shoot_date is not null and edit_received_date is not null and edit_completed_date is null then \'EDITING\' when shoot_date is not null and edit_received_date is not null and edit_completed_date is not null and catalogix_sent_timestamp is null then \'NOT_SEND_TO_CATALOGIX\' when shoot_date is not null and edit_received_date is not null and edit_completed_date is not null and catalogix_sent_timestamp is not null and catalogix_received_timestamp is null then \'CATALOGIX\' when shoot_date is not null and edit_received_date is not null and edit_completed_date is not null and catalogix_sent_timestamp is not null and catalogix_received_timestamp is not null and inward_date is not null then \'QC\' when shoot_date is not null and edit_received_date is not null and edit_completed_date is not null and catalogix_sent_timestamp is not null and catalogix_received_timestamp is not null and inward_date is null then \'CATALOGING_DONE_BUT_NOT_INWARDED\' else \'others\' end as final_status from main_data where sku_group not in (\'4MSS2466-02\',\'4MSN1021-White\',\'4MSN1059-01-Shirt\',\'4MSR5053-04\',\'4MSS1318-03\',\'4MSK8513-03\',\'4MSR5054-01\',\'4MST0160-Beige-T-Shirt\',\'4MSS1860-05\',\'4MSS2067-02\',\'4MSS1926-01\',\'4MSN0990-01-Shirt\',\'4MSS1907-07\',\'4MSW9001-02\',\'4MSW9001-04\',\'4MSS1697-06\',\'4MSS2495-05\',\'4MSR5054-02\',\'4MSR5054-04\',\'4MSS2301-02\',\'4MSR5046-04\',\'4MSR5032-04\',\'4MSS2509-01\',\'4MSD3516-01\',\'4MSN1021-Black\',\'4MSS1339-01\',\'4MSS1801-13\',\'4MSCR7266-05\',\'4MSS1539-01\',\'4MSS1971-04\',\'4MSS1642-01\',\'4MSS2110-03\',\'4MSR5054-05\',\'4MSK8570-03\',\'4MSS2420-02\',\'4MSS2420-03\',\'4MSS1962-05\',\'4MSK8540-03\',\'4MSS1944-06\',\'4MST2022-04\',\'4MSK8513-02\',\'4MSR5022-05\',\'4MSR5053-01\',\'4MST2237-11\',\'4MST2237-10\',\'4MSR5028-02\',\'4MSR5038-08\',\'4MST2237-09\',\'4MST2237-04\',\'4MST2237-12\',\'4MST2237-02\',\'4MST2237-07\',\'4MST2237-01\',\'4MSC4008-02\',\'4MTP0011-04\',\'4MSR5033-05\',\'4MSR5054-03\',\'4MSK8507-03\',\'4MSNJ0098-01-Denim\',\'4MSS1642-04\',\'4MSR5053-03\',\'4MSS2417-01\',\'4MSS2445-01\',\'4MSS2358-06\',\'4MSS2417-02\',\'4MSS2000-06\',\'4MSS2382-01\',\'4MSS2447-06\',\'4MSS2525-07\',\'4MSS2245-03\',\'4MSS2097-01\',\'4MSS2329-02\',\'4MSS2452-02\',\'4MSS1960-01\',\'4MST2022-03\',\'4MSS1878-04\',\'4MSS2417-03\',\'4MSS1697-12\',\'4MST2104-01\',\'4MSS2172-02\',\'4MSBX9202-27\',\'4MSS2648-5\',\'4MST0048_49-05-T-Shirt\',\'4MPJP013-01-Pyjama\',\'4MSBX9202-16\',\'4MSBX9202-19\',\'4MSS1718-01\',\'4MSK8570-05\',\'4MSK8570-04\',\'4MSC4006-02\',\'4MSR5053-02\',\'4MSFR0901\',\'4MTR0112-04-Trouser\',\'4MSNJ0055-04-Jeans\',\'4MSS2594-03\',\'4MSBX9202-01\',\'4MST0017-04-T-Shirt\',\'4MST0217-06-T-Shirt\',\'4MSNJ0055-01-Jeans\',\'4MST0217-03-T-Shirt\',\'4MSNJ0031-03-Jeans\',\'4MSFR0903\',\'4MSC4006-08\',\'4MSS2495-07\',\'4MST0028-01-T-Shirt\',\'4MSS1226-02\',\'4MTR0112-05-Trouser\',\'4MST0175-04-Sweater\',\'4MSN1256-01-Co-Ords\',\'4MSNJ0025-03-Jeans\',\'4MSD3506-02\',\'4MST0232-03-Co-Ords\',\'4MSD3506-03\',\'4MSS2594-04\',\'4MSS1481-09\',\'4MSS1116-02\',\'4MSS2199-03\',\'4MSS1681-01\',\'4MSS1222-02\',\'4MSD3506-01\',\'4MSS1226-03\',\'4MSH0030-05-Shorts\',\'4MSN1021-Navy\',\'4MSS2441-03\',\'4MSS2338-01\',\'4MST0166-06-T-Shirt\',\'4MSZ0011-01-Co-Ords\',\'4MSS1226-01\',\'4MST0014-01\', \'4MSW9001-01\',\'4MST0014-13\',\'4MST0014-03\',\'4MST2066-03\',\'4MST2066-01\',\'4MST0218-03\',\'4MST0226-01\',\'4MSS1974-01\',\'4MST0014-04\',\'4MSS1203-18\',\'4MSS1977-01\',\'4MST0014-22\',\'4MSS1974-02\') ) select SKU_GROUP, SAMPLE_RECEIVED_DATE, LIVE_DATE, PHOTOSHOOT_STATUS, SHOOT_DATE, EDITOR_OWNER, DELIVERY_DATE, UNITS_ON_HAND, INWARD_DATE, PSLJ_STATUS, STATUS, SR_LIVE_TAT SHOOT_TAT, SHOOTDONE_EDITRECEIVED_TAT, EDIT_TAT, EDITDONE_CATALOGIXSENT_TAT, CATALOGIX_TAT, LIVE_TAT, FINAL_STATUS,tags, to_date(edit_received_date,\'YYYY-MM-DD\') as edit_received_date, to_date(EDIT_COMPLETED_DATE,\'YYYY-MM-DD\') as EDIT_COMPLETED_DATE, to_date(CATALOGIX_SENT_TIMESTAMP,\'YYYY-MM-DD\') as CATALOGIX_SENT_TIMESTAMP, to_date(CATALOGIX_RECEIVED_TIMESTAMP,\'YYYY-MM-DD\') as CATALOGIX_RECEIVED_TIMESTAMP, from main_data2 );",
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
            