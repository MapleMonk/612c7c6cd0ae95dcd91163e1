{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.my_sql_product_product as select pp.*, case when pp.brand_type = \'branded\' then (case when pp.short_name in (\'WE\',\'WEE\',\'WD\',\'FR\',\'CH\') then pp.description when pp.short_name in (\'B\',\'W\',\'N\') then \'Eggoz Premium\' when pp.productsubdivision_id in (42,45) then pps.name else \'Branded Others\' end) else \'Unbranded\' end as product_type from eggozdb.maplemonk.my_sql_product_product pp left join eggozdb.maplemonk.my_sql_product_productsubdivision pps on pps.id = pp.productSubDivision_id ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.MAPLEMONK.My_SQL_base_city
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        