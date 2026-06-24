{{ config(
            materialized='table',
                post_hook={
                    "sql": "drop table if exists public.quickshift_Unicommerce_Inventory_factitems; CREATE TABLE public.quickshift_Unicommerce_Inventory_factitems AS select shelf ,cast(_airbyte_emitted_at as date) Data_Fetch_Date ,cast(expiry as date) Expiry_Date ,facility ,\"inventory type\" ,\"item type name\" ,\"item type sku code\" ,\"vendor batch number\" ,\'UNI QUICKSHIFT\' AS Data_Source ,sum(quantity) Current_Inventory from public.unicommerce_quickshift_get_inventory_snapshot_export_full_refresh group by 1,2,3,4,5,6,7,8 union all select shelf ,cast(_airbyte_emitted_at as date) Data_Fetch_Date ,cast(expiry as date) Expiry_Date ,facility ,\"inventory type\" ,\"item type name\" ,\"item type sku code\" ,\"vendor batch number\" ,\'SUPER YOU\' AS Data_Source ,sum(quantity) Current_Inventory from public.unicommerce_super_you_get_inventory_snapshot_export_full_refresh group by 1,2,3,4,5,6,7,8 union all select shelf ,cast(_airbyte_emitted_at as date) Data_Fetch_Date ,cast(expiry as date) Expiry_Date ,facility ,\"inventory type\" ,\"item type name\" ,\"item type sku code\" ,\"vendor batch number\" ,\'ORIGIN\' AS Data_Source ,sum(quantity) Current_Inventory from public.unicommerce_origin_get_inventory_snapshot_export_full_refresh group by 1,2,3,4,5,6,7,8 union all select shelf ,cast(_airbyte_emitted_at as date) Data_Fetch_Date ,cast(expiry as date) Expiry_Date ,facility ,\"inventory type\" ,\"item type name\" ,\"item type sku code\" ,\"vendor batch number\" ,\'OZONE\' AS Data_Source ,sum(quantity) Current_Inventory from public.unicommerce_ozone_get_inventory_snapshot_export_full_refresh group by 1,2,3,4,5,6,7,8 union all select shelf ,cast(_airbyte_emitted_at as date) Data_Fetch_Date ,cast(expiry as date) Expiry_Date ,facility ,\"inventory type\" ,\"item type name\" ,\"item type sku code\" ,\"vendor batch number\" ,\'R FOR RABBIT\' AS Data_Source ,sum(quantity) Current_Inventory from public.unicommerce_rabbit_get_inventory_snapshot_export_full_refresh group by 1,2,3,4,5,6,7,8;",
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
            