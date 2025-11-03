{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE eggozdb.maplemonk.my_sql_product_product AS SELECT *, CASE WHEN brand_type = \'branded\' THEN CASE WHEN short_name = \'WE\' THEN \'Everyday\' WHEN short_name = \'BEENECC\' THEN \'Everyday\' WHEN short_name = \'WEE\' THEN \'Everyday\' WHEN short_name = \'WEENECC\' THEN \'Everyday\' WHEN short_name = \'BEENECC\' THEN \'Everyday\' WHEN short_name = \'BBT\' THEN \'Everyday\' WHEN short_name = \'BEE\' THEN \'Everyday\' WHEN short_name = \'FWE\' THEN \'FreshEggs\' WHEN short_name = \'WD\' THEN \'Everyday\' WHEN short_name = \'FR\' THEN \'Free Range\' WHEN short_name = \'CH\' THEN \'Champs\' WHEN short_name = \'JP\' THEN \'Champs\' WHEN short_name = \'W\' THEN \'Eggoz Premium\' WHEN short_name = \'B\' THEN \'Eggoz Premium\' WHEN short_name = \'N\' THEN \'Eggoz Premium\' WHEN short_name like \'%GT%\' THEN \'Eggoz Premium\' WHEN short_name like \'%*%\' THEN \'Eggoz Premium\' WHEN productSubDivision_id = 42 THEN \'Frozen\' WHEN productSubDivision_id = 45 THEN \'Eazy Eggs\' WHEN short_name in (\'EGGOZB001\',\'EGGOZPB01\') then \'Boiler\' ELSE \'Branded Others\' END WHEN brand_type = \'unbranded\' THEN CASE WHEN short_name = \'L\' then \'Unbranded Liquid\' ELSE \'Unbranded Others\' END END as product_type FROM eggozdb.maplemonk.my_sql_product_product ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from EGGOZDB.MAPLEMONK.MY_SQL_warehouse_warehouse
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            