{{ config(
            materialized='table',
                post_hook={
                    "sql": "ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.OMNI_INVENTORY_META AS with inv_data as ( select query_hour_ist::TIMESTAMP AS inventory_logs, id, shopify_product_id, shopify_product_variant_id, sku, size, branch_code, branch_name, NULLIF(REPLACE(inventory_total,\',\',\'\'),\'\')::INTEGER as inventory_total, NULLIF(REPLACE(inventory_atp,\',\',\'\'),\'\')::INTEGER as inventory_atp, NULLIF(REPLACE(inventory_blocked,\',\',\'\'),\'\')::INTEGER as inventory_blocked, NULLIF(REPLACE(inventory_buffer,\',\',\'\'),\'\')::INTEGER as inventory_buffer, NULLIF(REPLACE(inventory_not_found,\',\',\'\'),\'\')::INTEGER as inventory_not_found, QUICK_COM_AVAILABLE, LAT, LON, selling_price, mrp, location, NULLIF(REPLACE(location_inventory_buffer,\',\',\'\'),\'\')::INTEGER as location_inventory_buffer, NULLIF(REPLACE(sku_inventory_buffer,\',\',\'\'),\'\')::INTEGER as sku_inventory_buffer, created_at::TIMESTAMP as created_at, updated_at::TIMESTAMP as updated_at, location_visible, section_code, enabled from snitch_db.maplemonk.s3_product_inventory ) , store_mapping as ( select store_name, store_code, CASE WHEN lower(region) like \'%ben%\' or lower(region) like \'%ban%\' then \'BANGALORE\' else region end as region , Cluster from snitch_db.maplemonk.OFFLINE_STORE_DETAILED_MAPPING where store_code != 0 ), metafields as ( select sku_group, material, designs, fit, sleeve_type, collar from snitch_db.maplemonk.metafields_data ), final1 as ( select s.store_name, s.region, s.cluster ,i.* from inv_data i left join store_mapping s on i.branch_code = s.store_code ), uc as ( select sku, skugroup as sku_group, category_code, sellable_uc, total_uc, total_logic, sku_class from snitch_db.maplemonk.uc_final_item_master ), final as ( select f.*, m.material, m.designs, m.fit, m.sleeve_type, m.collar, u.sku_group, u.category_code, u.sellable_uc, u.total_uc, u.total_logic, u.sku_class from final1 f left join uc u on f.sku = u.sku left join metafields m on u.sku_group = m.sku_group ) select * from final; INSERT INTO SNITCH_DB.MAPLEMONK.OMNI_INVENTORY_META_HOURLY select STORE_NAME,REGION,CLUSTER,INVENTORY_LOGS,ID,SHOPIFY_PRODUCT_ID,SHOPIFY_PRODUCT_VARIANT_ID,SKU,SIZE,BRANCH_CODE,BRANCH_NAME,INVENTORY_TOTAL,INVENTORY_ATP,INVENTORY_BLOCKED,INVENTORY_BUFFER,INVENTORY_NOT_FOUND,QUICK_COM_AVAILABLE,LAT,LON,SELLING_PRICE,MRP,LOCATION,LOCATION_INVENTORY_BUFFER,SKU_INVENTORY_BUFFER,CREATED_AT,UPDATED_AT,LOCATION_VISIBLE,SECTION_CODE,ENABLED,SKU_GROUP,CATEGORY_CODE,SELLABLE_UC,TOTAL_UC,TOTAL_LOGIC,SKU_CLASS FROM SNITCH_DB.MAPLEMONK.OMNI_INVENTORY_META ;",
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
            