{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.future_inwarding as Select Sku_group, expected_del_date, expected_qty, product_name_updated, product_class_updated, CATEGORY, CASE WHEN LENGTH(TRIM(product_name_updated)) > 15 THEN \'Repeat\' ELSE \'New\' END AS product_status From ( WITH LatestProductDetails AS ( Select * From ( SELECT SKU_GROUP, PRODUCT_NAME, CATEGORY, sku_class, ROW_NUMBER() OVER(PARTITION BY SKU_GROUP ORDER BY order_date DESC) as row_num FROM snitch_db.MAPLEMONK.UNICOMMERCE_FACT_ITEMS_SNITCH )Where row_num=1 ), ExpectedDelivery AS ( Select \"SKU_NO.\" as Sku_group, factory as shipped_from_factory, expected_del_date , total_online_qty as expected_qty, Style_Type from ( Select \"SKU_NO.\",factory,total_online_qty,TO_DATE(delivery_planned_date, \'DD/MM/YYYY\') as expected_del_date, case when Lower(NEW_STYLE) like \'%yes%\' then \'New\' else \'Repeat\' end as Style_Type from snitch_db.maplemonk.hsk_inventory where STATUS IS NULL UNION ALL Select \"SKU_NO.\",factory,total_online_qty,TO_DATE(delivery_planned_date, \'DD-MM-YYYY\') as expected_del_date, case when Lower(NEW_STYLE) like \'%yes%\' then \'New\' else \'Repeat\' end as Style_Type from snitch_db.maplemonk.YLK_INVENTORY where REMARK IS NULL UNION ALL Select \"SKU_NO.\",factory,total_online_qty,TO_DATE(delivery_planned_date, \'DD/MM/YYYY\') as expected_del_date, case when Lower(NEW_STYLE) like \'%yes%\' then \'New\' else \'Repeat\' end as Style_Type from snitch_db.maplemonk.EMIZA_INVENTORY where REMARK IS NULL ) ) SELECT REPLACE( ed.SKU_group, \' \', \'\') as sku_group, ed.expected_del_date, ed.expected_qty, ed.Style_Type, CASE WHEN lpd.PRODUCT_NAME is NULL THEN ed.SKU_GROUP ELSE lpd.PRODUCT_NAME END as product_name_updated, CASE WHEN lpd.sku_class is NULL THEN \'4-New\' ELSE lpd.sku_class END as product_class_updated, lpd.CATEGORY, lpd.row_num FROM ExpectedDelivery ed LEFT JOIN LatestProductDetails lpd ON REPLACE( ed.SKU_group, \' \', \'\') = lpd.SKU_GROUP ORDER BY ed.expected_del_date )",
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
                        