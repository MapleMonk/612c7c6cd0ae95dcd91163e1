{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE eggozdb.maplemonk.my_sql_product_product AS SELECT pp.*, CASE WHEN pp.brand_type = \'branded\' AND pp.short_name IN (\'WE\', \'WEE\', \'WD\', \'FR\', \'CH\') THEN pp.description WHEN pp.brand_type = \'branded\' AND pp.short_name IN (\'B\', \'W\', \'N\') THEN \'Eggoz Premium\' WHEN pp.brand_type = \'branded\' AND pp.productsubdivision_id IN (42, 45) THEN pps.name WHEN pp.brand_type = \'branded\' THEN \'Branded Others\' ELSE \'Unbranded\' END AS product_type FROM eggozdb.maplemonk.my_sql_product_product pp LEFT JOIN eggozdb.maplemonk.my_sql_product_productsubdivision pps ON pps.id = pp.productSubDivision_id;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.MAPLEMONK.My_SQL_product_product
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        