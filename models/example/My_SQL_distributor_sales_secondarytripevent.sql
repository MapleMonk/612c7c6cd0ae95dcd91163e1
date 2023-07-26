{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "alter table eggozdb.maplemonk.my_sql_retailer_retailer drop column activity_status ; create or replace table eggozdb.maplemonk.my_sql_retailer_retailer as select *, case when code ilike any (\'%G2391%\', \'%G1968%\', \'%G1061%\', \'%G2104%\', \'%G1845%\', \'%G2223%\', \'%G2082%\', \'%G2251%\', \'%G1797%\', \'%G1114%\', \'%G2451%\', \'%G2250%\', \'%G1115%\', \'%G2831%\', \'%G1225%\', \'%G1054%\', \'%G1045%\', \'%G2046%\', \'%G2161%\', \'%G1726%\', \'%G2821%\') then \'done\' else \'not_done\' end as activity_status from eggozdb.maplemonk.my_sql_retailer_retailer ;",
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
                        