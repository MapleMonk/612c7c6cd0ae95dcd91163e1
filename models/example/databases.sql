{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table sleepycat_db.maplemonk.po_grn_analysis as with base as ( select po_id, grn_Id, grn_status, po_created_date::Date po_Date, inwarded_WArehouse, grn.value:model_no::string sku, sum(grn.value:available::float) grn_quantity, sum(grn.value:original_quantity::float) po_quantity, from sleepycat_db.maplemonk.easyecom_sleepycat_grn_details, LATERAL FLATTEN(input => grn_items) AS grn, group by 1,2,3,4,5,6 ) select b.*,sm_category as Category,sub_category from base b left join ( select upper(trim(marketplace_sku)) as sm_sku, category as sm_category, sub_category from sleepycat_db.maplemonk.final_sku_master qualify row_number() over(partition by upper(trim(marketplace_sku)) order by 1) = 1 )sm on lower(trim(b.sku)) = lower(trim(sm.sm_sku)) ; create or replace table sleepycat_db.maplemonk.sleepycat_fifo_report as select CONVERT_TIMEZONE(\'Asia/Kolkata\',TO_TIMESTAMP_TZ(_AIRBYTE_NORMALIZED_AT))::DATE AS Data_Fetch_Date, upper(sku) as sku, \"Company Name\" as Compnay_Name, upper(coalesce(sm_category,category)) as Category, sub_category, \"Days In Warehouse\" days_in_warehouse, sum(quantity) quantity from sleepycat_db.maplemonk.easyecom_sleepycat_full_inventory_report fi left join ( select upper(trim(marketplace_sku)) as sm_sku, category as sm_category, sub_category from sleepycat_db.maplemonk.final_sku_master qualify row_number() over(partition by upper(trim(marketplace_sku)) order by 1) = 1 )sm on lower(trim(fi.sku)) = lower(trim(sm.sm_sku)) where lower(status) in (\'available\',\'reserved\') and lower(\"Company Name\") in (\'vsl-cdp\', \'central distribution point\', \'vsl-bangalore\', \'vsl-farukhnagar\', \'vsl-hyderabad\', \'vsl-mumbai\', \'vsl-cdp\') group by 1,2,3,4,5,6 ;",
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
            