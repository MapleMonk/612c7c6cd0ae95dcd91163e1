{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE SELECT_DB.MAPLEMONK.SHOPIFY_OVERALL_SESSION_ANALYSIS AS WITH s1 AS ( SELECT *, REVERSE(SUBSTRING(REVERSE(page_path), 1, CHARINDEX(\'/\', REVERSE(page_path)) - 1)) AS HANDLE, TO_DATE(day, \'MM/DD/YYYY\') AS Date, SUBSTRING(hour, CHARINDEX(\' \', hour) + 1) AS HourString FROM SELECT_DB.MAPLEMONK.SELECT_DB_SHOPIFY_OVERALL_SESSIONS ), a1_distinct AS ( SELECT * FROM ( SELECT HANDLE, \"Parent Category\", \"Child Category\", ROW_NUMBER() OVER (PARTITION BY LOWER(handle) ORDER BY LOWER(\"Child Category\") DESC) rw FROM SELECT_DB.MAPLEMONK.SELECT_DB_SHOPIFY_DATA_MASTER ) AS t WHERE rw = 1 ) SELECT s1.HANDLE, s1.page_type, s1.utm_campaign_source, s1.referring_channel, s1.referring_traffic, s1.location_city, s1.utm_campaign_medium, s1.Date as Date, s1.HourString AS Hour, s1.month, s1.total_sessions, s1.referrer_source, s1.total_carts, s1.total_checkouts, s1.total_orders_placed, s1.total_conversion, s1.total_bounce_rate, s1.avg_duration, s1.total_visitors, a1_distinct.\"Parent Category\", a1_distinct.\"Child Category\", CASE WHEN a1_distinct.\"Child Category\" = \'Jade\' THEN \'Jade\' WHEN a1_distinct.\"Child Category\" = \'Calathea Triostar\' THEN \'Calathea Triostar\' WHEN a1_distinct.\"Child Category\" = \'Golden Money\' THEN \'Golden Money\' WHEN a1_distinct.\"Child Category\" = \'Zamia Green (Zz)\' THEN \'Zamia Green (Zz)\' WHEN a1_distinct.\"Child Category\" = \'Sansevieria Golden Hahnii Snake\' THEN \'Sansevieria Golden Hahnii Snake\' WHEN a1_distinct.\"Child Category\" = \'Homepage\' THEN \'Homepage\' WHEN s1.HANDLE = \'plants-1\' THEN \'plants-1\' WHEN s1.HANDLE = \'stress-buster-combos\' THEN \'Stress Buster\' WHEN s1.HANDLE = \'vastu-plants\' THEN \'Vastu Plants\' WHEN s1.HANDLE = \'combos\' THEN \'combos\' WHEN s1.HANDLE = \'low-maintenance-combos\' THEN \'Low Main\' WHEN s1.HANDLE = \'stress-buster-plants\' THEN \'Stress Buster\' WHEN s1.HANDLE = \'set-of-2-live-indoor-plant-combo-of-golden-money-and-jade-with-self-watering-pot\' THEN \'Jade + Golden Money\' WHEN s1.HANDLE = \'vastu-combo-plants\' THEN \'Vastu Plants\' WHEN s1.HANDLE = \'low-maintenance-plants\' THEN \'Low Main\' WHEN s1.HANDLE = \'set-of-2-live-indoor-plant-combo-of-jade-and-sansevieria-golden-hahnii-snake-with-self-watering-pot\' THEN \'Jade + Golden Hahnii\' WHEN s1.HANDLE = \'set-of-2-live-indoor-plant-combo-of-sansevieria-green-snake-and-zamia-green-zz-with-self-watering-pot\' THEN \'ZZ + Green Snake\' WHEN s1.HANDLE = \'set-of-2-live-indoor-plant-combo-of-aglaonema-lipstick-and-golden-money-with-self-watering-pot\' THEN \'Golden Money + Lipstick\' WHEN s1.HANDLE = \'set-of-2-live-indoor-plant-combo-of-zamia-green-zz-and-sansevieria-green-snake-with-self-watering-pot\' THEN \'ZZ + Green Snake\' WHEN s1.HANDLE = \'set-of-2-live-indoor-plant-combo-of-golden-money-and-zamia-green-zz-with-self-watering-pot\' THEN \'ZZ + Golden Money\' WHEN s1.HANDLE = \'set-of-2-live-indoor-plant-combo-of-sansevieria-golden-hahnii-snake-and-jade-with-self-watering-pot\' THEN \'Jade + Golden Hahnii\' WHEN s1.HANDLE = \'set-of-2-live-indoor-plant-combo-of-zamia-green-zz-and-golden-money-with-self-watering-pot\' THEN \'ZZ + Golden Money\' ELSE \'Others\' END AS BUCKET FROM s1 LEFT JOIN a1_distinct ON lower(s1.HANDLE) = lower(a1_distinct.HANDLE);",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from SELECT_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        