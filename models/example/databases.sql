{{ config(
            materialized='table',
                post_hook={
                    "sql": "ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; INSERT INTO SNITCH_DB.MAPLEMONK.OMNI_INVENTORY_META with inv_data as ( select id, shopify_product_id, shopify_product_variant_id, sku, size, branch_code, branch_name, NULLIF(REPLACE(inventory_total,\',\',\'\'),\'\')::INTEGER as inventory_total, NULLIF(REPLACE(inventory_atp,\',\',\'\'),\'\')::INTEGER as inventory_atp, NULLIF(REPLACE(inventory_blocked,\',\',\'\'),\'\')::INTEGER as inventory_blocked, NULLIF(REPLACE(inventory_buffer,\',\',\'\'),\'\')::INTEGER as inventory_buffer, NULLIF(REPLACE(inventory_not_found,\',\',\'\'),\'\')::INTEGER as inventory_not_found, selling_price, mrp, location, NULLIF(REPLACE(location_inventory_buffer,\',\',\'\'),\'\')::INTEGER as location_inventory_buffer, NULLIF(REPLACE(sku_inventory_buffer,\',\',\'\'),\'\')::INTEGER as sku_inventory_buffer, created_at, updated_at, location_visible, section_code, enabled from snitch_db.maplemonk.s3_omni_inv_s3 ) , store_mapping as ( select store_name, store_code, CASE WHEN lower(region) like \'%ben%\' or lower(region) like \'%ban%\' then \'BANGALORE\' else region end as region , Cluster from snitch_db.maplemonk.OFFLINE_STORE_DETAILED_MAPPING where store_code != 0 ), final1 as ( select s.store_name, s.region, s.cluster ,i.* from inv_data i left join store_mapping s on i.branch_code = s.store_code ), uc as ( select sku, sku_group, category_code, sellable_uc, total_uc, total_logic, sku_class from snitch_db.maplemonk.uc_final_item_master ), final as ( select f.*, u.sku_group, u.category_code, u.sellable_uc, u.total_uc, u.total_logic, u.sku_class from final1 f left join uc u on f.sku = u.sku ) select current_date as DATE ,* from final ;",
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
            