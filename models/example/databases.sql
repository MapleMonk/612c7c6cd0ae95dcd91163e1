{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table select_db.maplemonk.ECPL_MIS_D4_ADS_AND_SALES_DATA_FINAL as select try_cast(\"IMP\" as float) \"IMP\", try_cast(\"ASIN\" as varchar) \"ASIN\", try_cast(\"YEAR\" as float) \"YEAR\", try_cast(\"COLOR\" as varchar) \"COLOR\", try_cast(\"SPEND\" as float) \"SPEND\", try_cast(\"CLICKS\" as float) \"CLICKS\", try_cast(\"AD_TYPE\" as varchar) \"AD_TYPE\", try_cast(\"Week No\" as float) \"Week No\", try_cast(\"Day Name\" as varchar) \"Day Name\", try_cast(\"Child SKU\" as varchar) \"Child SKU\", try_cast(\"MAIN_DATE\" as date) \"MAIN_DATE\", try_cast(\"OWNERSHIP\" as varchar) \"OWNERSHIP\", try_cast(\"SALE_TYPE\" as varchar) \"SALE_TYPE\", try_cast(\"Brand Name\" as varchar) \"Brand Name\", try_cast(\"Month Name\" as varchar) \"Month Name\", try_cast(\"Parent SKU\" as varchar) \"Parent SKU\", try_cast(\"Bundle Size\" as varchar) \"Bundle Size\", try_cast(\"Channel SKU\" as varchar) \"Channel SKU\", try_cast(\"Sales Target\" as float) \"Sales Target\", try_cast(\"Spend Target\" as float) \"Spend Target\", try_cast(\"Clicks Target\" as float) \"Clicks Target\", try_cast(\"FINAL_DATE_EC\" as date) \"FINAL_DATE_EC\", try_cast(\"ORDERED_SALES\" as float) \"ORDERED_SALES\", try_cast(\"ORDERED_UNITS\" as float) \"ORDERED_UNITS\", try_cast(\"Sale Strategy\" as varchar) \"Sale Strategy\", try_cast(\"SALES_CHANNEL\" as varchar) \"SALES_CHANNEL\", try_cast(\"SHIPPED_SALES\" as float) \"SHIPPED_SALES\", try_cast(\"SHIPPED_UNITS\" as float) \"SHIPPED_UNITS\", try_cast(\"Child category\" as varchar) \"Child category\", try_cast(\"Total Ad Sales\" as float) \"Total Ad Sales\", try_cast(\"Total Ad Units\" as float) \"Total Ad Units\", try_cast(\"Parent category\" as varchar) \"Parent category\", try_cast(\"FINAL_NEW_PARENT\" as varchar) \"FINAL_NEW_PARENT\", try_cast(\"Total Ordered Sales\" as float) \"Total Ordered Sales\", try_cast(\"Total Ordered Units\" as float) \"Total Ordered Units\", try_cast(\"Total Shipped Sales\" as float) \"Total Shipped Sales\", try_cast(\"Total Shipped Units\" as float) \"Total Shipped Units\", try_cast(\"14 Day Total Orders (#)\" as float) \"14 Day Total Orders (#)\", _AIRBYTE_AB_ID, _AIRBYTE_EMITTED_AT, _AIRBYTE_NORMALIZED_AT, _AIRBYTE_ECPL_MIS_D4_ADS_AND_SALES_DATA___TEMPLATE_HASHID from select_db.maplemonk.ECPL_MIS_D4_ADS_AND_SALES_DATA___TEMPLATE;",
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
                        