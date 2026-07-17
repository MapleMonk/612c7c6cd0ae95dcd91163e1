{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table neshanka-wh.maplemonk.easyecom_catalog as select UPPER(name) name, coalesce(cast(ml.sku as string),cast(pm.sku as string)) sku, mastersku, cast(site_uid as string) product_id, pm.mrp, eanupc, width, height, length, weight, hsn_code, tax_rate, UPPER(product_name) product_name, UPPER(category_name) category_name from maplemonk.ritualistic_get_marketplace_listing ml left join ( select * from maplemonk.easyecom_easyecom_product_master qualify row_number() over (partition by sku, product_id) = 1 ) pm on ml.mastersku = pm.sku qualify row_number() over (partition by name, site_uid) = 1;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            