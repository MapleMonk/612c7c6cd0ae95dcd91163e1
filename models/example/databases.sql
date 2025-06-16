{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.OMNI_LOGISTICS as with cp1 as (select * from snitch_db.maplemonk.cp_1 where assigned_date >= \'2025-05-01\' and warehouse is null and mode != \'Reverse\' and order_id not like \'%FS%\' and awb_number not like \'%TEST%\' and pickup_pincode not in (\'562123\',\'560064\',\'562114\',\'400058\',\'560005\',\'421102\',\'283125\',\'600002\') ) , mapping as ( select distinct TRIM(RTRIM(REGEXP_SUBSTR(stock_transfer_branchname, \'^[^-]*-[^-]*-[^-]*\', 1, 1), \'-\')) as store_name, CASE WHEN city_name like \'%BANG%\' THEN \'BANGALORE\' WHEN city_name like \'%BENG%\' THEN \'BANGALORE\' WHEN city_name like \'%BENA%\' THEN \'BANGALORE\' WHEN city_name like \'%BANA%\' THEN \'BANGALORE\' else city_name END as region , state_name as cluster, stock_transfer_branchcode as store_code, group_name3 as party , pincode as pc, CASE WHEN store_name like \'%COFO%\' then \'STORE\' WHEN store_name like \'%FOCO%\' then \'STORE\' WHEN store_name like \'%COCO%\' then \'STORE\' WHEN store_name like \'%YEL%\' then \'WH\' WHEN store_name like \'%EMIZA%\' then \'WH\' WHEN store_name like \'%B2B%\' then \'WH\' WHEN store_name like \'%- WH - %\' then \'Virtual\' WHEN store_name like \'%MARKET%\' then \'MP\' ELSE NULL END AS TYPE from snitch_db.maplemonk.logicERPnew_get_party_master where store_code != 0 and type = \'STORE\' ), final as ( select m.store_name, m.region, m.cluster , m.store_code, m.party ,c.* from cp1 c left join mapping m on c.pickup_pincode = m.pc where c.order_date is not null ) select CASE WHEN MAPPED_CITY = REGION THEN \'HYPERLOCAL\' ELSE \'OMNI\' end as PRIORITY, * from final ;",
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
            