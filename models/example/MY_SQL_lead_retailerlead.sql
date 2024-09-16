{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table eggozdb.maplemonk.my_sql_retailer_retailer as select rr.*, rcc.name as retailer_category, rcs.name as retailer_subcategory, case when rr.code ilike any ( \'%G2391%\',\'%G1968%\',\'%G1061%\',\'%G2104%\',\'%G1845%\',\'%G2223%\',\'%G2082%\',\'%G2251%\',\'%G1797%\',\'%G1114%\',\'%G2451%\',\'%G2250%\',\'%G1115%\',\'%G2831%\',\'%G1225%\', \'%G1054%\',\'%G1045%\',\'%G2046%\',\'%G2161%\', \'%G1726%\',\'%G2821%\',\'%G1810%\',\'%G2583%\',\'%G1086%\',\'%G2834%\',\'%G2929%\',\'%D2171%\',\'%D3128%\',\'%D2914%\',\'%G1853%\', \'%D2849%\',\'%N4040%\',\'%G1760%\',\'%G2838%\',\'%G2161%\',\'%G2696%\',\'%G1054%\',\'%G2744%\', \'%G1366%\',\'%G2831%\',\'%G1837%\',\'%G1717%\',\'%G2156%\',\'%G1042%\',\'%G2249%\', \'%G1041%\',\'%G2744%\',\'%G1054%\',\'%D3128%\',\'%N4402%\',\'%D2561%\',\'%D2918%\',\'%D2914%\',\'%G1225%\',\'%N4040%\',\'%G2967%\',\'%G2249%\', \'%G1366%\',\'%H5107%\',\'%N4568%\', \'%H5078%\',\'%G2690%\',\'%N4643%\',\'%N4682%\',\'%N4146%\',\'%D2701%\',\'%D2245%\',\'%D2207%\',\'%D3007%\',\'%N4244%\',\'%N4324%\',\'%D2162%\',\'%D2171%\',\'%N4736%\',\'%N4138%\', \'%D2165%\', \'%G2251%\',\'%G2250%\',\'%G2624%\',\'%G2252%\',\'%D2166%\',\'%D3039%\',\'%H5076%\',\'%D2173%\',\'%H5237%\',\'%H5179%\',\'%F3025%\',\'%G2133%\',\'%N4689%\',\'%D2130%\', \'%D4079%\',\'%N4195%\' ) then \'done\' else \'not_done\' end as activity_status from eggozdb.maplemonk.my_sql_retailer_retailer rr left join eggozdb.maplemonk.my_sql_retailer_customer_category rcc on rcc.id = rr.category_id left join eggozdb.maplemonk.my_sql_retailer_customer_subcategory rcs on rcs.id = rr.sub_category_id where rr.dealer_id is null and rr.category_id <> 11 ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from EGGOZDB.MAPLEMONK.MY_SQL_lead_retailerlead
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            