{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.inward_outward as select case when right(\"Item Type skuCode\",2) = \'-S\' then left(\"Item Type skuCode\",len(\"Item Type skuCode\")-2) else replace(\"Item Type skuCode\",concat(\'-\',split_part(\"Item Type skuCode\",\'-\',-1)),\'\') end sku_group ,left(\"CREATED\",10)::date date ,case when _AB_SOURCE_FILE_URL like \'%SAPL-SR%\' then \'SAPL-SR\' when _AB_SOURCE_FILE_URL like \'%SAPL-EMIZA%\' then \'SAPL-EMIZA\' when _AB_SOURCE_FILE_URL like \'%SAPL-WH%\' then \'SAPL-WH\' end warehouse_name ,sum(\"Putaway Quantity\") quantity from snitch_db.MAPLEMONK.UNICOMMERCE_PUTAWAY_REPORT where type = \'PUTAWAY_GRN_ITEM\' group by 1,2,3; create or replace table snitch_db.maplemonk.sku_class_mapping as WITH DerivedTable AS ( SELECT CASE WHEN RIGHT(a.\"Item Type skuCode\", 2) = \'-S\' THEN LEFT(a.\"Item Type skuCode\", LEN(a.\"Item Type skuCode\") - 2) ELSE REPLACE(a.\"Item Type skuCode\", CONCAT(\'-\', SPLIT_PART(a.\"Item Type skuCode\", \'-\', -1)), \'\') END AS sku_group, LEFT(a.\"CREATED\", 10)::DATE AS date, SUM(a.\"Putaway Quantity\") AS quantity FROM snitch_db.maplemonk.unicommerce_putaway_report a where type = \'PUTAWAY_GRN_ITEM\' group by 1,2 ) SELECT dt.sku_group, dt.date, dt.quantity, b.sku_class, b.category, b.final_ros, b.product_name FROM DerivedTable dt LEFT JOIN snitch_db.maplemonk.availability_master b ON dt.sku_group = b.sku_group;",
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
                        