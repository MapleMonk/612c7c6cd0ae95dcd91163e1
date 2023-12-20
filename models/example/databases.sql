{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table OFFDUTY_DB.MAPLEMONK.OFFDUTY_SKU_MASTER as select * from (select * ,row_number() over (partition by lower(marketplace_sku) order by 1) rw from ( select upper(Shopify_SKU) MARKETPLACE_SKU ,upper(name) NAME ,upper(category) CATEGORY ,upper(sub_category) SUB_CATEGORY ,upper(skucode) skucode from (select skucode , \"PRODUCT TITLE\" name , category , sub_category , skucode Shopify_SKU , row_number() over (partition by skucode order by 1) rw from OFFDUTY_DB.MAPLEMONK.sku_master ) where rw = 1 and not(upper(Shopify_SKU) like any (\'#N/A\', \'0\', \'NA\', \'%NOT%\')) union select upper(MYNTRA_SID) MARKETPLACE_SKU ,upper(name) NAME ,upper(category) CATEGORY ,upper(sub_category) SUB_CATEGORY ,upper(skucode) skucode from (select skucode , \"PRODUCT TITLE\" name , category , sub_category , \"MYNTRA SID\" MYNTRA_SID , row_number() over (partition by \"MYNTRA SID\" order by 1) rw from OFFDUTY_DB.MAPLEMONK.sku_master) where rw = 1 and not(upper(MYNTRA_SID) like any (\'#N/A\', \'0\', \'NA\', \'%NOT%\')) union select upper(MYNTRA_SKU) MARKETPLACE_SKU ,upper(name) NAME ,upper(category) CATEGORY ,upper(sub_category) SUB_CATEGORY ,upper(skucode) skucode from (select skucode , \"PRODUCT TITLE\" name , category , sub_category , \"MYNTRA SKU Code\" MYNTRA_SKU , row_number() over (partition by \"MYNTRA SKU Code\" order by 1) rw from OFFDUTY_DB.MAPLEMONK.sku_master) where rw = 1 and not(upper(MYNTRA_SKU) like any (\'#N/A\', \'0\', \'NA\', \'%NOT%\')) union select upper(NYKAA_SKU) MARKETPLACE_SKU ,upper(name) NAME ,upper(category) CATEGORY ,upper(sub_category) SUB_CATEGORY ,upper(skucode) skucode from (select skucode , \"PRODUCT TITLE\" name , category , sub_category , \"NYKAA SKU CODE\" NYKAA_SKU , row_number() over (partition by \"NYKAA SKU CODE\" order by 1) rw from OFFDUTY_DB.MAPLEMONK.sku_master) where rw = 1 and not(upper(NYKAA_SKU) like any (\'#N/A\', \'0\', \'NA\', \'%NOT%\')) ) ) where rw=1 ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from offduty_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        