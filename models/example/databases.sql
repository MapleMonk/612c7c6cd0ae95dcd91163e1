{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table Lilgoodness_db.maplemonk.Fact_Items_AmazonSellerPartner_LG as SELECT ASP.* , CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\', \"purchase-date\":: DATETIME) as \"Purchase-datetime-IST\" ,SKU_MAPPING.SKU MAPPING_SKU FROM Lilgoodness_db.maplemonk.ASP_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL ASP left join (select * from (select SKU, MARKETPLACE_SKU, MARKETPLACE_NAME , row_number() over (partition by marketplace_sku order by sku) rw from LILGOODNESS_DB.maplemonk.lg_marketplace_sku_mapping where lower(marketplace_name)=\'amazon\' ) where lower(marketplace_name)=\'amazon\' and rw=1 ) SKU_MAPPING on ASP.ASIN = SKU_MAPPING.Marketplace_SKU WHERE upper(CURRENCY) = \'INR\' AND \"item-price\" NOT IN(\'\',\'0.0\');",
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
                        