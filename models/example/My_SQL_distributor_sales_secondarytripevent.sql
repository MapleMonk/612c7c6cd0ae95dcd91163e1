{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.my_sql_retailer_retailer as select *, case when code ilike any (\'%G1061%\', \'%G2391%\', \'%G2251%\', \'%G1845%\', \'%G1797%\', \'%G1968%\', \'%G1086%\', \'%G2223%\', \'%G2250%\', \'%G2082%\', \'%G2451%\', \'%G2831%\', \'%G2184%\', \'%G2104%\') then \'activity_done\' else null end as activity_status from eggozdb.maplemonk.my_sql_retailer_retailer ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.MAPLEMONK.My_SQL_distributor_sales_secondarytripevent
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        