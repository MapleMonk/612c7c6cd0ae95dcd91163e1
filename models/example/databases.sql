{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table lilgoodness_db.maplemonk.BIGBASKET_FACT_ITEMS_LG as select \'BigBasket\' as Shop_name ,date_range ,try_to_timestamp(left(a.DATE_RANGE,position(\' - \',a.DATE_RANGE,1)-1),\'YYYYMMDD\') Order_Date ,ifnull(try_to_double(a.TOTAL_MRP),0) TOTAL_MRP_PER_PLATFORM ,concat(left(a.DATE_RANGE,position(\' - \',a.DATE_RANGE,1)-1),\'-BBOID-\',SOURCE_SKU_ID) order_id ,ifnull(try_to_double(a.TOTAL_SALES),0) TOTAL_SALES ,ifnull(try_to_double(a.TOTAL_QUANTITY),0) QUANTITY ,SOURCE_SKU_ID BB_SKU ,BRAND_NAME ,TOP_SLUG ,MID_SLUG ,LEAF_SLUG ,BUSINESS_TYPE ORDER_TYPE ,SKU_WEIGHT ,SOURCE_CITY_NAME CITY ,SKU_MAPPING.SKU MAPPING_SKU from lilgoodness_db.maplemonk.bigbasket_sales a left join (select * from (select SKU, MARKETPLACE_SKU,MARKETPLACE_NAME , row_number() over (partition by marketplace_sku order by sku) rw from LILGOODNESS_DB.maplemonk.lg_marketplace_sku_mapping ) where rw=1 ) SKU_MAPPING on lower(a.SOURCE_SKU_ID) = lower(SKU_MAPPING.marketplace_sku) ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from LILGOODNESS_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        