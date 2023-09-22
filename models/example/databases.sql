{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.sku_group_tags as select REVERSE(SUBSTRING(REVERSE(PARSE_JSON(variants)[0]:sku), CHARINDEX(\'-\', REVERSE(PARSE_JSON(variants)[0]:sku)) + 1)) AS sku_group, TAGS, CASE WHEN TAGS LIKE \'%Half Sleeves%\' THEN \'Half Sleeve\' WHEN TAGS LIKE \'%Full Sleeve%\' THEN \'Full Sleeve\' WHEN TAGS LIKE \'%Sleeveless%\' THEN \'Sleeveless\' ELSE \'No Sleeve Info\' END as sleeve_type, CASE WHEN TAGS LIKE \'%Button Down Collar%\' THEN \'Button Down Collar\' WHEN TAGS LIKE \'%Spread Collar%\' THEN \'Spread Collar\' WHEN TAGS LIKE \'%Cuban Collar%\' THEN \'Cuban Collar\' WHEN TAGS LIKE \'%Round Neck%\' THEN \'Round Neck\' WHEN TAGS LIKE \'%Crew Neck%\' THEN \'Crew Neck\' WHEN TAGS LIKE \'%Mandarin Collar%\' THEN \'Mandarin Collar\' WHEN TAGS LIKE \'%Polo%\' THEN \'Polo\' WHEN TAGS LIKE \'%Hoodies%\' THEN \'Hoodies\' ELSE \'No Collar Info\' END as collar_type, CASE WHEN TAGS LIKE \'%Printed%\' THEN \'Printed\' WHEN TAGS LIKE \'%Stripe%\' THEN \'Stripe\' WHEN TAGS LIKE \'%Checks%\' THEN \'Checks\' WHEN TAGS LIKE \'%Plain%\' THEN \'Plain\' WHEN TAGS LIKE \'%Graphic Print%\' THEN \'Graphic Print\' WHEN TAGS LIKE \'%Floral%\' THEN \'Floral\' WHEN TAGS LIKE \'%Embroidered%\' THEN \'Embroidered\' WHEN TAGS LIKE \'%Embellished%\' THEN \'Embellished\' WHEN TAGS LIKE \'%Tie & Dye%\' THEN \'Tie & Dye\' WHEN TAGS LIKE \'%Textured%\' THEN \'Textured\' WHEN TAGS LIKE \'%Solid%\' THEN \'Solid\' WHEN TAGS LIKE \'%Animal Print%\' THEN \'Animal Print\' WHEN TAGS LIKE \'%Basics%\' THEN \'Basics\' WHEN TAGS LIKE \'%Distressed%\' THEN \'Distressed\' WHEN TAGS LIKE \'%Corduroy%\' THEN \'Corduroy\' ELSE \'No Design Info\' END as design, CASE WHEN TAGS LIKE \'%Cotton%\' THEN \'Cotton\' WHEN TAGS LIKE \'%Rayon %\' THEN \'Rayon \' WHEN TAGS LIKE \'%Polyester %\' THEN \'Polyester \' WHEN TAGS LIKE \'%Stretch Shirt %\' THEN \'Stretch Shirt \' WHEN TAGS LIKE \'%Cotton Shirt %\' THEN \'Cotton Shirt \' WHEN TAGS LIKE \'%Linen %\' THEN \'Linen \' WHEN TAGS LIKE \'%Linen Blend %\' THEN \'Linen Blend \' WHEN TAGS LIKE \'%Cotton Blend%\' THEN \'Cotton Blend\' WHEN TAGS LIKE \'%Satin%\' THEN \'Satin\' WHEN TAGS LIKE \'%Cotton Satin%\' THEN \'Cotton Satin\' WHEN TAGS LIKE \'%Poly_blend%\' THEN \'Poly_blend\' ELSE \'No Fabric Info\' END as fabric, CASE WHEN TAGS LIKE \'%Curved%\' THEN \'Curved\' WHEN TAGS LIKE \'%Straight%\' THEN \'Straight\' ELSE \'No Hem Info\' END as hem FROM snitch_db.maplemonk.SHOPIFYINDIA_products",
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
                        