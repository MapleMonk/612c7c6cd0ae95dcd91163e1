{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "SELECT sku, (sum(Net_Eggs_sold) over (partiton by sku))/(sum(Net_Eggs_sold) over (partition by Egg_type)) AS \"Var_wise_sell%\"\", (sum(Profit_per_egg) over(partition by sku))/(sum(Profit_per_egg) over (partition by Egg_type)) AS \"Util_PPE\", area, beat_number_original, retailer_name, retailer_commission FROM( SELECT sku AS \"sku\", area AS \"area\", beat_number_original AS \"beat_number_original\", retailer_name AS \"retailer_name\", retailer_commission AS \"retailer_commission\", CASE WHEN sum(eggs_sold)-sum(eggs_return)<0 THEN 0 ELSE sum(eggs_sold)-sum(eggs_return) END AS \"Net_Eggs_sold\", CASE WHEN sum(eggs_sold)=0 THEN 0 WHEN sku IN (\'6B\') THEN (100-retailer_commission)/100*18.3-8.7 WHEN sku IN (\'10B\') THEN (100-retailer_commission)/100*18.0-8.6 WHEN sku IN (\'25B\') THEN (100-retailer_commission)/100*15.2-8.3 WHEN sku IN (\'30B\') THEN (100-retailer_commission)/100*15.0-8.1 WHEN sku IN (\'6W\') THEN (100-retailer_commission)/100*14.2-7.7 WHEN sku IN (\'10W\') THEN (100-retailer_commission)/100*14.0-7.6 WHEN sku IN (\'25W\') THEN (100-retailer_commission)/100*12.0-7.3 WHEN sku IN (\'30W\') THEN (100-retailer_commission)/100*11.0-7.1 WHEN sku IN (\'10N\') THEN (100-retailer_commission)/100*15.5-8.2 END AS \"Profit per egg\", CASE WHEN sku IN(\'6B\',\'10B\',\'25B\',\'30B\') THEN \'Brown\' WHEN sku IN(\'6W\',\'10W\',\'25W\',\'30W\') THEN \'White\' END AS \"Egg_type\" FROM eggozdb.maplemonk.summary_reporting_table_beat_retailer_sku GROUP BY sku, Egg_type, area, beat_number_original, retailer_name, retailer_commission ) WHERE date>\'2022-05-19\' AND date<\'2022-07-19\' GROUP BY sku, Egg_type, area, beat_number_original, retailer_name, retailer_commission",
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
                        