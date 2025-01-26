{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db_wooqer AS ( select *,case when store is not null then 40 end as store_count from snitch_db1_wooqer); CREATE OR REPLACE TABLE snitch_db_wooqer3 AS ( WITH store_list AS ( SELECT \'CITY CENTER\' AS Store UNION ALL SELECT \'JAYANAGAR BANGALORE\' UNION ALL SELECT \'EVA VADODARA\' UNION ALL SELECT \'HSR LAYOUT BANGALORE\' UNION ALL SELECT \'VIP ROAD VIZAG\' UNION ALL SELECT \'EARTH EUPHORIA\' UNION ALL SELECT \'INFINITI ANDHERI\' UNION ALL SELECT \'INFINITY VASHI\' UNION ALL SELECT \'MantriAvenue\' UNION ALL SELECT \'NEW BEL ROAD BANGALORE\' UNION ALL SELECT \'PMC CITADEL\' UNION ALL SELECT \'RAJPUR ROAD DEHRADUN\' UNION ALL SELECT \'SARATH CITY\' UNION ALL SELECT \'SHYAMAL\' UNION ALL SELECT \'TRIBECA\' UNION ALL SELECT \'TRION VADODARA\' UNION ALL SELECT \'VR MALL SURAT\' UNION ALL SELECT \'YAGNIK\' UNION ALL SELECT \'BANDRA\' UNION ALL SELECT \'BRIGADE ROAD BANGALORE\' UNION ALL SELECT \'HILITE\' UNION ALL SELECT \'INFINITI MALAD\' UNION ALL SELECT \'KUKATPALLY\' UNION ALL SELECT \'LANDMARK GANDHI NAGAR\' UNION ALL SELECT \'AMANORA\' UNION ALL SELECT \'DB MALL BHOPAL\' UNION ALL SELECT \'BANER PUNE\' UNION ALL SELECT \'MBH VARACHHA SURAT\' UNION ALL SELECT \'VAISHALI NAGAR JAIPUR\' UNION ALL SELECT \'C ROAD JODHPUR\' UNION ALL SELECT \'BHARTIYA CITY\' UNION ALL SELECT \'ESPLANADE MALL\' UNION ALL SELECT \'Hubli\' UNION ALL SELECT \'Kalyan Metro\' UNION ALL SELECT \'Kothapet\' UNION ALL SELECT \'Lajpat Nagar\' UNION ALL SELECT \'Lulu Mall\' UNION ALL SELECT \'Phoenix Palassio\' UNION ALL SELECT \'Rajahmundry\' ), process_list AS ( SELECT \'Opening Checklist\' AS Process UNION ALL SELECT \'Closing Checklist\' UNION ALL SELECT \'DSP\' UNION ALL SELECT \'Grooming-1st Shift\' UNION ALL SELECT \'Grooming-2nd Shift\' UNION ALL SELECT \'HK Grooming Picture-1st Shift\' UNION ALL SELECT \'Cash Deposit\' UNION ALL SELECT \'HK Grooming Picture-2nd Shift\' ), date_list AS ( SELECT DISTINCT \"Date (dd/mm/yyyy)\" FROM snitch_db1_wooqer ), all_combinations AS ( SELECT d.\"Date (dd/mm/yyyy)\", s.Store, p.Process FROM date_list d CROSS JOIN store_list s CROSS JOIN process_list p ), full_entries AS ( SELECT ac.\"Date (dd/mm/yyyy)\", ac.Store, ac.Process, CASE WHEN wd.Store IS NOT NULL THEN \'Completed\' ELSE \'Missed\' END AS Status FROM all_combinations ac LEFT JOIN snitch_db1_wooqer wd ON ac.\"Date (dd/mm/yyyy)\" = wd.\"Date (dd/mm/yyyy)\" AND ac.Store = wd.Store AND ac.Process = wd.Process ) SELECT \"Date (dd/mm/yyyy)\", Store, LISTAGG(CASE WHEN Status = \'Completed\' THEN Process END, \', \') AS \"Completed Process\", LISTAGG(CASE WHEN Status = \'Missed\' THEN Process END, \', \') AS \"Missed Process\" FROM full_entries GROUP BY \"Date (dd/mm/yyyy)\", Store)",
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
            