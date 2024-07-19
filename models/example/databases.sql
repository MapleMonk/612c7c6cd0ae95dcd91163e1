{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE EMMASLEEP_DB.MAPLEMONK.FINAL_SKU_MASTER AS select * from (select * ,row_number() over (partition by MARKETPLACE_SKU order by 1) frw from ( select \"SKU Code\" MARKETPLACE_SKU ,upper(\"PRODUCT TITLE\") NAME ,upper(\'SHOPIFY\') MARKETPLACE ,upper(category) CATEGORY ,upper(NATURE) SUB_CATEGORY ,upper(DIMENSIONS) SIZE ,\"SKU Code\" commonsku from (select * , row_number() over (partition by \"SKU Code\" order by 1) rw from EMMASLEEP_DB.MAPLEMONK.sku_master ) where rw = 1 and (upper(\"SKU Code\") not in (\'#N/A\', \'0\', \'NA\', \'N/A\') and upper(MARKETPLACE_SKU) not in (\'#N/A\', \'0\', \'NA\', \'N/A\') and MARKETPLACE_SKU is not null) union select ASIN MARKETPLACE_SKU ,upper(\"PRODUCT TITLE\") NAME ,upper(\'AMAZON\') MARKETPLACE ,upper(category) CATEGORY ,upper(NATURE) SUB_CATEGORY ,upper(DIMENSIONS) SIZE ,\"SKU Code\" commonsku from (select * , row_number() over (partition by ASIN order by 1) rw from EMMASLEEP_DB.MAPLEMONK.sku_master ) where rw = 1 and (upper(\"SKU Code\") not in (\'#N/A\', \'0\', \'NA\', \'N/A\') and upper(MARKETPLACE_SKU) not in (\'#N/A\', \'0\', \'NA\', \'N/A\') and MARKETPLACE_SKU is not null) union select \"FLIPKART FSN\" MARKETPLACE_SKU ,upper(\"PRODUCT TITLE\") NAME ,upper(\'FLIPKART\') MARKETPLACE ,upper(category) CATEGORY ,upper(NATURE) SUB_CATEGORY ,upper(DIMENSIONS) SIZE ,\"SKU Code\" commonsku from (select * , row_number() over (partition by \"FLIPKART FSN\" order by 1) rw from EMMASLEEP_DB.MAPLEMONK.sku_master ) where rw = 1 and (upper(\"SKU Code\") not in (\'#N/A\', \'0\', \'NA\', \'N/A\') and upper(MARKETPLACE_SKU) not in (\'#N/A\', \'0\', \'NA\', \'N/A\') and MARKETPLACE_SKU is not null) union select MYNTRA MARKETPLACE_SKU ,upper(\"PRODUCT TITLE\") NAME ,upper(\'MYNTRA\') MARKETPLACE ,upper(category) CATEGORY ,upper(NATURE) SUB_CATEGORY ,upper(DIMENSIONS) SIZE ,\"SKU Code\" commonsku from (select * , row_number() over (partition by MYNTRA order by 1) rw from EMMASLEEP_DB.MAPLEMONK.sku_master ) where rw = 1 and (upper(\"SKU Code\") not in (\'#N/A\', \'0\', \'NA\', \'N/A\') and upper(MARKETPLACE_SKU) not in (\'#N/A\', \'0\', \'NA\', \'N/A\') and MARKETPLACE_SKU is not null) ) ) where frw = 1;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from EMMASLEEP_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            