{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE BSC_DB.Maplemonk.EngravingTags AS select name, created_at, tags, A.VALUE:sku::STRING sku, B.Value:value::string as EngraveData from BSC_DB.MAPLEMONK.Shopify_All_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS)A, LATERAL FLATTEN (INPUT => A.value:properties)B where A.VALUE:sku::STRING in (\'PSS100\',\'SHAVE_6PSS_SENSI_LUXE_KIT\',\'SHAVE_SENSI_LUXE_RAZOR\',\'RB100\', \'SHAVE_RB100_BLACK\', \'SHAVE_PSS100_BLACK\', \'APPLIANCES_BEARD_TRIMMER_BTG1999\') and A.value:properties != \'\'",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from BSC_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        