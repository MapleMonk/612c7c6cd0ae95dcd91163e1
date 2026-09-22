{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.capsule_ctr_dashboard AS SELECT DATE, CASE WHEN ITEMPROMOTIONNAME IN ( \'Home_HeroCarousel_Loved By Everyone Jeans\', \'Home_HeroCarousel_collection_332190482594_Loved By Everyone Jeans_Loved By Everyone Jeans\' ) THEN \'Try Less Denim\' WHEN ITEMPROMOTIONNAME = \'Home_HeroCarousel_collection_332956369058_Printed Shirts_Printed Shirts\' THEN \'Printed Shirts\' WHEN TRIM(ITEMPROMOTIONNAME) IN ( \'Home_HeroCarousel_collection_333544849570_LUXE LINEN _LUXE LINEN\', \'Home_HeroCarousel_LUXE LINEN\' ) THEN \'Luxe\' WHEN UPPER(TRIM(ITEMPROMOTIONNAME)) LIKE \'%STREET FC PLUS%\' THEN \'Street FC Plus\' WHEN UPPER(TRIM(ITEMPROMOTIONNAME)) LIKE \'%STREET FC%\' THEN \'Street FC\' WHEN UPPER(TRIM(ITEMPROMOTIONNAME)) LIKE \'%ROOTED IN NATURE%\' OR UPPER(TRIM(ITEMPROMOTIONNAME)) LIKE \'%BOTANICAL%\' THEN \'Botanical Theme\' WHEN UPPER(TRIM(ITEMPROMOTIONNAME)) LIKE \'%EMBROID%\' THEN \'Embroided Shirts\' WHEN UPPER(TRIM(ITEMPROMOTIONNAME)) LIKE \'%TOUCH%\' THEN \'Golf Polo Theme\' WHEN UPPER(TRIM(ITEMPROMOTIONNAME)) LIKE \'%THE RECEIPTS%\' THEN \'THE RECEIPTS\' END AS CAMPAIGN, \'YES\' AS FOCUS_CAPSULE, CLICKS, VIEWS, ROUND( (CLICKS * 100.0) / NULLIF(VIEWS, 0), 2 ) AS CTR FROM snitch_db.maplemonk.snitchit_analytics_table WHERE ITEMPROMOTIONNAME IN ( \'Home_HeroCarousel_Loved By Everyone Jeans\', \'Home_HeroCarousel_collection_332190482594_Loved By Everyone Jeans_Loved By Everyone Jeans\' ) OR ITEMPROMOTIONNAME = \'Home_HeroCarousel_collection_332956369058_Printed Shirts_Printed Shirts\' OR TRIM(ITEMPROMOTIONNAME) IN ( \'Home_HeroCarousel_collection_333544849570_LUXE LINEN _LUXE LINEN\', \'Home_HeroCarousel_LUXE LINEN\' ) OR UPPER(TRIM(ITEMPROMOTIONNAME)) LIKE \'%STREET FC PLUS%\' OR UPPER(TRIM(ITEMPROMOTIONNAME)) LIKE \'%STREET FC%\' OR UPPER(TRIM(ITEMPROMOTIONNAME)) LIKE \'%ROOTED IN NATURE%\' OR UPPER(TRIM(ITEMPROMOTIONNAME)) LIKE \'%BOTANICAL%\' OR UPPER(TRIM(ITEMPROMOTIONNAME)) LIKE \'%EMBROID%\' OR UPPER(TRIM(ITEMPROMOTIONNAME)) LIKE \'%TOUCH%\' OR UPPER(TRIM(ITEMPROMOTIONNAME)) LIKE \'%THE RECEIPTS%\';",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from SNITCH_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            