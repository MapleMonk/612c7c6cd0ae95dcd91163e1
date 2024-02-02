{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE SELECT_DB.MAPLEMONK.CLEANED_ABC_ABANDONED_CHECKOUTS AS WITH cte1 AS ( SELECT ID, NAME, COMPLETED_AT, _AIRBYTE_ABC_ABANDONED_CHECKOUTS_HASHID, ABANDONED_CHECKOUT_URL, A.value:\"compare_at_price\"::string AS COMPARE_AT_PRICE, A.value:\"quantity\"::integer AS QUANTITY, A.value:\"variant_id\"::integer AS VARIANT_ID, A.value:\"presentment_title\"::string AS PRESENTMENT_TITLE, A.value:\"sku\"::string AS SKU FROM select_db.maplemonk.abc_abandoned_checkouts, LATERAL FLATTEN (INPUT => LINE_ITEMS) A ) select cte1.* ,p.commonskuid ,p.name AS PRODUCT_NAME ,p.category ,p.sub_category from cte1 left join (select * from (select marketplace_sku skucode, commonskuid,name, category, sub_category, row_number() over (partition by lower(marketplace_sku) order by 1) rw from SELECT_DB.MAPLEMONK.SELECT_DB_sku_master) where rw = 1 ) p on lower(cte1.sku) = lower(p.skucode) ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from SELECT_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        