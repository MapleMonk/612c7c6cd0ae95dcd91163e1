{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.emmasleep_inventory_tracker as with inventory as ( select sku, location_key, ifnull(cast(availableinventory as int),0) as AVAILABLEINVENTORY, TO_TIMESTAMP(data_fetch_date, \'YYYY-MM-DD HH24:MI:SS\') AS Reporting_datetime from maplemonk.easyecom_emmasleep_inventory_details qualify row_number() over (partition by location_key, SKU, Reporting_datetime order by Reporting_datetime desc) = 1 ) select sku, location_key, reporting_datetime, CAST(reporting_datetime AS DATE) as data_fetch_date, TO_VARCHAR(reporting_datetime, \'HH12 AM\') as reporting_hour, sum(ifnull(AVAILABLEINVENTORY,0)) AVAILABLEINVENTORY from inventory group by 1,2,3,4,5;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from EMMASLEEP_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            