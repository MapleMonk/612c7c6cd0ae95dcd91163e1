{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE eggozdb.maplemonk.test AS SELECT area, beat_number_original, retailer_name, retailer_commission, sku, \"Net_Eggs_sold\", \"Egg_type\", CASE WHEN \"Net_Eggs_sold\"=0 THEN 0 ELSE (sum(\"Net_Eggs_sold\") over (partition by sku, retailer_name))/(sum(\"Net_Eggs_sold\") over (partition by \"Egg_type\",retailer_name)) END AS \"VS%\", CASE WHEN \"Net_Eggs_sold\"=0 THEN 0 ELSE (sum(\"Profit_per_egg\") over(partition by sku,retailer_name))/(sum(\"Profit_per_egg\") over (partition by \"Egg_type\",retailer_name)) END AS \"UTIL\", CASE WHEN \"Net_Eggs_sold\"=0 THEN 0 ELSE 0.3*(sum(\"Net_Eggs_sold\") over (partition by sku,retailer_name))/(sum(\"Net_Eggs_sold\") over (partition by \"Egg_type\",retailer_name))+0.7*(sum(\"Profit_per_egg\") over(partition by sku,retailer_name))/(sum(\"Profit_per_egg\") over (partition by \"Egg_type\",retailer_name)) END AS \"New_VWS%\" FROM( SELECT sku , area , beat_number_original , retailer_name , retailer_commission , CASE WHEN sum(eggs_sold)-sum(eggs_return)<0 THEN 0 ELSE sum(eggs_sold)-sum(eggs_return) END AS \"Net_Eggs_sold\", CASE WHEN sum(eggs_sold)=0 THEN 0 WHEN sku IN (\'6B\') THEN (100-retailer_commission)/100*18.3-8.7 WHEN sku IN (\'10B\') THEN (100-retailer_commission)/100*18.0-8.6 WHEN sku IN (\'25B\') THEN (100-retailer_commission)/100*15.2-8.3 WHEN sku IN (\'30B\') THEN (100-retailer_commission)/100*15.0-8.1 WHEN sku IN (\'6W\') THEN (100-retailer_commission)/100*14.2-7.7 WHEN sku IN (\'10W\') THEN (100-retailer_commission)/100*14.0-7.6 WHEN sku IN (\'25W\') THEN (100-retailer_commission)/100*12.0-7.3 WHEN sku IN (\'30W\') THEN (100-retailer_commission)/100*11.0-7.1 WHEN sku IN (\'10N\') THEN (100-retailer_commission)/100*15.5-8.2 END AS \"Profit_per_egg\", CASE WHEN sku IN(\'6B\',\'10B\',\'25B\',\'30B\') THEN \'Brown\' WHEN sku IN(\'6W\',\'10W\',\'25W\',\'30W\') THEN \'White\' END AS \"Egg_type\" FROM eggozdb.maplemonk.summary_reporting_table_beat_retailer_sku GROUP BY sku, area, beat_number_original, retailer_name, retailer_commission )",
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
                        