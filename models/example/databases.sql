{{ config(
            materialized='table',
                post_hook={
                    "sql": "drop table if exists public.anveshan_easyecom_consolidated_inventory_fact_items; create table public.anveshan_easyecom_consolidated_inventory_fact_items as select upper(\"company name\") as company_name, upper(\"brand name\") as brand_name, upper(replace(sku,\'`\',\'\')) as sku, cast(mrp as float4) as mrp, upper(cast(zone as varchar)) as zone, shelf, cast(\"ean no\" as varchar) as ean_no, upper(cast(status as varchar)) as inventory_status, cast(quantity as int8) as available_inventory, \"serial no\" as serial_no, \"batch code\" as batch_code, date(\"expiry date\") as expiry_date, upper(\"product name\") as product_name, cast(\"days to expire\" as int4) as days_to_expire, left(\"manufacturing date\",10) as manufacturing_date, cast(\"shelf life percentage\" as float) shelf_life_percentage from public.easyecom_anveshan_consolidated_inventory ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select database, schema, "table" from SVV_TABLE_INFO limit 1
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            