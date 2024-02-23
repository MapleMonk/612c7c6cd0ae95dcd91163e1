{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table avorganics_db.maplemonk.swiggy_fact_items as select null as CUSTOMER_ID_FINAL ,null as ACQUISITION_DATE ,null as FIRST_COMPLETE_ORDER_DATE ,null as MAPLE_MONK_ID_PHONE ,null as CUSTOMER_ID ,\'Swiggy\' as SHOP_NAME ,\'Swiggy\' as MARKETPLACE ,\'Swiggy\' as CHANNEL ,\'Swiggy\' as SOURCE ,item_code::varchar as ORDER_ID ,item_code::varchar as REFERENCE_CODE ,null as PHONE ,null as NAME ,null as EMAIL ,null as SHIPPING_LAST_UPDATE_DATE ,b.sku as SKU ,item_code::varchar as PRODUCT_ID ,a.product_name as PRODUCT_NAME ,null as CURRENCY ,city as CITY ,null as STATE ,null as ORDER_STATUS ,case when dt = \'\' then null else to_date(dt,\'dd-mm-yyyy\') end as ORDER_DATE ,total_quantity::float as QUANTITY ,null as GROSS_SALES_BEFORE_TAX ,null as DISCOUNT ,null as TAX ,null as SHIPPING_PRICE ,store_price*total_quantity::float as SELLING_PRICE ,null as OMS_ORDER_STATUS ,null as SHIPPING_STATUS ,null as FINAL_SHIPPING_STATUS ,null as SALEORDERITEMCODE ,null as SALES_ORDER_ITEM_ID ,null as AWB ,null as PAYMENT_GATEWAY ,null as PAYMENT_MODE ,null as COURIER ,null as DISPATCH_DATE ,case when dt = \'\' then null else to_date(dt,\'dd-mm-yyyy\') end DELIVERED_DATE ,null as DELIVERED_STATUS ,null as RETURN_FLAG ,null as RETURNED_QUANTITY ,null as RETURNED_SALES ,null as CANCELLED_QUANTITY ,null as DAYS_IN_SHIPMENT ,null as ACQUSITION_DATE ,item_code::varchar as SKU_CODE ,b.product_name as PRODUCT_NAME_FINAL ,b.category as PRODUCT_CATEGORY ,b.sub_category as PRODUCT_SUB_CATEGORY ,null as WAREHOUSE ,null as NEW_CUSTOMER_FLAG ,null as NEW_CUSTOMER_FLAG_MONTH ,null as ACQUISITION_PRODUCT ,null as ACQUISITION_CHANNEL ,null as ACQUISITION_MARKETPLACE from avorganics_db.MAPLEMONK.s3_swiggy a left join (select sku, product_code, product_name, category, sub_category from (select primarykey sku, \"SWIGGY\" product_code, \"PRODUCT TITLE\" product_name, category, sub_category, row_number() over (partition by \"SWIGGY\" order by 1) rw from avorganics_db.maplemonk.sku_master where \"SWIGGY\" <>\'-\' )where rw = 1) b on a.item_code::varchar = b.product_code union all select null as CUSTOMER_ID_FINAL ,null as ACQUISITION_DATE ,null as FIRST_COMPLETE_ORDER_DATE ,null as MAPLE_MONK_ID_PHONE ,null as CUSTOMER_ID ,\'Swiggy\' as SHOP_NAME ,\'Swiggy\' as MARKETPLACE ,\'Swiggy\' as CHANNEL ,\'Swiggy\' as SOURCE ,final_item_code::varchar as ORDER_ID ,final_item_code::varchar as REFERENCE_CODE ,null as PHONE ,null as NAME ,null as EMAIL ,null as SHIPPING_LAST_UPDATE_DATE ,b.sku as SKU ,final_item_code::varchar as PRODUCT_ID ,a.product_name as PRODUCT_NAME ,null as CURRENCY ,city as CITY ,null as STATE ,null as ORDER_STATUS ,case when lower(_AB_SOURCE_FILE_URL) like \'jan\' then \'2024-01-01\' when lower(_AB_SOURCE_FILE_URL) like \'feb\' then \'2024-02-01\' when lower(_AB_SOURCE_FILE_URL) like \'mar\' then \'2024-03-01\' when lower(_AB_SOURCE_FILE_URL) like \'apr\' then \'2024-04-01\' when lower(_AB_SOURCE_FILE_URL) like \'may\' then \'2024-05-01\' when lower(_AB_SOURCE_FILE_URL) like \'jun\' then \'2024-06-01\' when lower(_AB_SOURCE_FILE_URL) like \'jul\' then \'2024-07-01\' when lower(_AB_SOURCE_FILE_URL) like \'aug\' then \'2024-08-01\' when lower(_AB_SOURCE_FILE_URL) like \'sep\' then \'2024-09-01\' when lower(_AB_SOURCE_FILE_URL) like \'oct\' then \'2024-10-01\' when lower(_AB_SOURCE_FILE_URL) like \'nov\' then \'2024-11-01\' when lower(_AB_SOURCE_FILE_URL) like \'dec\' then \'2024-12-01\' end as ordeR_date ,final_qty::float as QUANTITY ,null as GROSS_SALES_BEFORE_TAX ,null as DISCOUNT ,null as TAX ,null as SHIPPING_PRICE ,final_gmv::float as SELLING_PRICE ,null as OMS_ORDER_STATUS ,null as SHIPPING_STATUS ,null as FINAL_SHIPPING_STATUS ,null as SALEORDERITEMCODE ,null as SALES_ORDER_ITEM_ID ,null as AWB ,null as PAYMENT_GATEWAY ,null as PAYMENT_MODE ,null as COURIER ,null as DISPATCH_DATE ,case when lower(_AB_SOURCE_FILE_URL) like \'jan\' then \'2024-01-01\' when lower(_AB_SOURCE_FILE_URL) like \'feb\' then \'2024-02-01\' when lower(_AB_SOURCE_FILE_URL) like \'mar\' then \'2024-03-01\' when lower(_AB_SOURCE_FILE_URL) like \'apr\' then \'2024-04-01\' when lower(_AB_SOURCE_FILE_URL) like \'may\' then \'2024-05-01\' when lower(_AB_SOURCE_FILE_URL) like \'jun\' then \'2024-06-01\' when lower(_AB_SOURCE_FILE_URL) like \'jul\' then \'2024-07-01\' when lower(_AB_SOURCE_FILE_URL) like \'aug\' then \'2024-08-01\' when lower(_AB_SOURCE_FILE_URL) like \'sep\' then \'2024-09-01\' when lower(_AB_SOURCE_FILE_URL) like \'oct\' then \'2024-10-01\' when lower(_AB_SOURCE_FILE_URL) like \'nov\' then \'2024-11-01\' when lower(_AB_SOURCE_FILE_URL) like \'dec\' then \'2024-12-01\' end as DELIVERED_DATE ,null as DELIVERED_STATUS ,null as RETURN_FLAG ,null as RETURNED_QUANTITY ,null as RETURNED_SALES ,null as CANCELLED_QUANTITY ,null as DAYS_IN_SHIPMENT ,null as ACQUSITION_DATE ,final_item_code::varchar as SKU_CODE ,b.product_name as PRODUCT_NAME_FINAL ,b.category as PRODUCT_CATEGORY ,b.sub_category as PRODUCT_SUB_CATEGORY ,null as WAREHOUSE ,null as NEW_CUSTOMER_FLAG ,null as NEW_CUSTOMER_FLAG_MONTH ,null as ACQUISITION_PRODUCT ,null as ACQUISITION_CHANNEL ,null as ACQUISITION_MARKETPLACE from avorganics_db.MAPLEMONK.s3_swiggy_from2024 a left join (select sku, product_code, product_name, category, sub_category from (select primarykey sku, \"SWIGGY\" product_code, \"PRODUCT TITLE\" product_name, category, sub_category, row_number() over (partition by \"SWIGGY\" order by 1) rw from avorganics_db.maplemonk.sku_master where \"SWIGGY\" <>\'-\' )where rw = 1) b on a.final_item_code::varchar = b.product_code ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from avorganics_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        