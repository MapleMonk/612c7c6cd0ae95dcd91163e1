{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.my_sql_retailer_retailer as select *, case when code ilike any (\'%G2391%\',\'%G1968%\',\'%G1061%\',\'%G2104%\',\'%G1845%\',\'%G2223%\',\'%G2082%\',\'%G2251%\',\'%G1797%\',\'%G1114%\',\'%G2451%\',\'%G2250%\',\'%G1115%\',\'%G2831%\',\'%G1225%\',\'%G1054%\',\'%G1045%\',\'%G2046%\',\'%G2161%\',\'%G1726%\',\'%G2821%\',\'%G1810%\',\'%G2583%\',\'%G1086%\',\'%G2834%\',\'%G2929%\',\'%D2171%\',\'%D3128%\',\'%D2914%\',\'%G1853%\',\'%D2849%\',\'%N4040%\',\'%G1760%\',\'%G2838%\',\'%G2161%\',\'%G2696%\',\'%G1054%\',\'%G2744%\',\'%G1366%\',\'%G2831%\',\'%G1837%\',\'%G1717%\',\'%G2156%\',\'%G1042%\',\'%G2249%\',\'%G1041%\') then \'done\' else \'not_done\' end as activity_status from eggozdb.maplemonk.my_sql_retailer_retailer ;",
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
                        