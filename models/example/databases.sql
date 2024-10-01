{{ config(
            materialized='table',
                post_hook={
                    "sql": "ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; Create or replace table snitch_db.maplemonk.SOH_emailers as WITH deduped_fact_items_snitch AS ( SELECT * FROM ( SELECT SKU, Category, ROW_NUMBER() OVER (PARTITION BY SKU ORDER BY SKU_GROUP DESC) AS rn FROM snitch_db.maplemonk.fact_items_snitch ) WHERE rn = 1 ) SELECT s.*, -- All columns from \'s\' COALESCE( f.Category, CASE WHEN LEFT(s.Logicusercode, 4) = \'4MAC\' THEN \'Accessories\' WHEN LEFT(s.Logicusercode, 4) = \'4MBZ\' THEN \'Blazers\' WHEN LEFT(s.Logicusercode, 5) = \'4MSBX\' THEN \'Boxers\' WHEN LEFT(s.Logicusercode, 4) = \'4MSO\' THEN \'Cargo Pants\' WHEN LEFT(s.Logicusercode, 4) = \'4MSC\' THEN \'Chinos\' WHEN LEFT(s.Logicusercode, 4) = \'4MSCR\' THEN \'Co-ords\' WHEN LEFT(s.Logicusercode, 4) = \'4MSD\' THEN \'Denim\' WHEN LEFT(s.Logicusercode, 4) = \'4MSWH\' THEN \'Hoodies\' WHEN LEFT(s.Logicusercode, 4) = \'4MSK\' THEN \'Jackets\' WHEN LEFT(s.Logicusercode, 4) = \'4MTP\' THEN \'Joggers & Trackpants\' WHEN LEFT(s.Logicusercode, 4) = \'4MSP\' THEN \'Night Suit & Pyjamas\' WHEN LEFT(s.Logicusercode, 4) = \'4MSN\' THEN \'Shirts\' WHEN LEFT(s.Logicusercode, 5) = \'4MSFR\' THEN \'Perfumes\' WHEN LEFT(s.Logicusercode, 6) = \'4MBGSS\' THEN \'Plus Size\' WHEN LEFT(s.Logicusercode, 4) = \'4MSS\' THEN \'Shirts\' WHEN LEFT(s.Logicusercode, 2) = \'SH\' THEN \'Shoes\' WHEN LEFT(s.Logicusercode, 4) = \'4MSH\' THEN \'Shorts\' WHEN LEFT(s.Logicusercode, 2) = \'SN\' THEN \'Sunglasses\' WHEN LEFT(s.Logicusercode, 4) = \'4MSW\' THEN \'Sweaters\' WHEN LEFT(s.Logicusercode, 4) = \'4MSR\' THEN \'Trousers\' WHEN LEFT(s.Logicusercode, 4) = \'4MST\' THEN \'T-Shirts\' WHEN LEFT(s.Logicusercode, 6) = \'4MBGSD\' THEN \'T-Shirts\' WHEN LEFT(s.Logicusercode, 4) = \'4MS0\' THEN \'Cargo Pants\' WHEN LEFT(s.Logicusercode, 4) = \'4MTR\' THEN \'Cargo Pants\' WHEN LEFT(s.Logicusercode, 2) = \'CB\' THEN \'Carry Bags\' WHEN LEFT(s.Logicusercode, 4) = \'4MVK\' THEN \'C\' ELSE \'Others\' END ) AS Category FROM snitch_db.maplemonk.logicerp23_24_get_stock_in_hand AS s LEFT JOIN deduped_fact_items_snitch AS f ON s.Logicusercode = f.SKU WHERE s.Date = CURRENT_DATE;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from snitch_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            