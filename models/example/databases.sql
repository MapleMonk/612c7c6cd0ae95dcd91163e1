{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE eggozdb.maplemonk.my_sql_product_product AS SELECT *, CASE WHEN brand_type = \'branded\' THEN CASE WHEN short_name = \'WE\' THEN \'Nogyo\' WHEN short_name = \'WEE\' THEN \'Everyday\' WHEN short_name = \'WD\' THEN \'Darjan\' WHEN short_name = \'FR\' THEN \'Free Range\' WHEN short_name = \'CH\' THEN \'Champs\' WHEN short_name = \'W\' THEN \'Eggoz Premium\' WHEN short_name = \'B\' THEN \'Eggoz Premium\' WHEN short_name = \'N\' THEN \'Eggoz Premium\' WHEN productSubDivision_id = 42 THEN \'Frozen\' WHEN productSubDivision_id = 45 THEN \'Eazy Eggs\' ELSE \'Branded Others\' END WHEN brand_type = \'unbranded\' THEN CASE WHEN short_name = \'L\' then \'Unbranded Liquid\' ELSE \'Unbranded Others\' END END as product_type FROM eggozdb.maplemonk.my_sql_product_product ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        