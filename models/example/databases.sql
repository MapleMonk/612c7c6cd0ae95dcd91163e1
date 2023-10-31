{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.sku_group_tags as WITH ParsedSKUs AS ( SELECT CASE WHEN PARSE_JSON(variants)[0]:sku IS NOT NULL THEN PARSE_JSON(variants)[0]:sku WHEN PARSE_JSON(variants)[1]:sku IS NOT NULL THEN PARSE_JSON(variants)[1]:sku WHEN PARSE_JSON(variants)[2]:sku IS NOT NULL THEN PARSE_JSON(variants)[2]:sku ELSE NULL END AS sku, PARSE_JSON(variants)[0]:product_id::STRING AS product_id, TAGS FROM snitch_db.maplemonk.SHOPIFYINDIA_products ) , ExtractedData AS ( SELECT REVERSE(SUBSTRING(REVERSE(sku), CHARINDEX(\'-\', REVERSE(sku)) + 1)) AS sku_group, product_id, TAGS, CASE WHEN TAGS LIKE \'%Half Sleeve%\' THEN \'Half Sleeve\' WHEN TAGS LIKE \'%Full Sleeve%\' THEN \'Full Sleeve\' WHEN TAGS LIKE \'%Sleeveless%\' THEN \'Sleeveless\' ELSE \'No Sleeve Info\' END as sleeve_type, CASE WHEN TAGS LIKE \'%Button Down Collar%\' THEN \'Button Down Collar\' WHEN TAGS LIKE \'%Spread Collar%\' THEN \'Spread Collar\' WHEN TAGS LIKE \'%Cuban Collar%\' THEN \'Cuban Collar\' WHEN TAGS LIKE \'%Round Neck%\' THEN \'Round Neck\' WHEN TAGS LIKE \'%Crew Neck%\' THEN \'Crew Neck\' WHEN TAGS LIKE \'%Mandarin Collar%\' THEN \'Mandarin Collar\' WHEN TAGS LIKE \'%Mandarin collar%\' THEN \'Mandarin Collar\' WHEN TAGS LIKE \'%Polo%\' THEN \'Polo\' WHEN TAGS LIKE \'%Hoodies%\' THEN \'Hoodies\' WHEN TAGS LIKE \'%Ribbed Collar%\' THEN \'Ribbed Collar\' WHEN TAGS LIKE \'%Turtle Neck%\' THEN \'Turtle Neck\' WHEN TAGS LIKE \'%High Neck%\' THEN \'High Neck\' ELSE \'No Collar Info\' END as collar_type, CASE WHEN TAGS LIKE \'%Printed%\' THEN \'Printed\' WHEN TAGS LIKE \'%Stripe%\' THEN \'Stripe\' WHEN TAGS LIKE \'%Checks%\' THEN \'Checks\' WHEN TAGS LIKE \'%Plain%\' THEN \'Plain\' WHEN TAGS LIKE \'%Graphic Print%\' THEN \'Graphic Print\' WHEN TAGS LIKE \'%Floral%\' THEN \'Floral\' WHEN TAGS LIKE \'%Embroidered%\' THEN \'Embroidered\' WHEN TAGS LIKE \'%Embellished%\' THEN \'Embellished\' WHEN TAGS LIKE \'%Tie & Dye%\' THEN \'Tie & Dye\' WHEN TAGS LIKE \'%Textured%\' THEN \'Textured\' WHEN TAGS LIKE \'%Solid%\' THEN \'Solid\' WHEN TAGS LIKE \'%Animal Print%\' THEN \'Animal Print\' WHEN TAGS LIKE \'%Basics%\' THEN \'Basics\' WHEN TAGS LIKE \'%Distressed%\' THEN \'Distressed\' WHEN TAGS LIKE \'%Corduroy%\' THEN \'Corduroy\' WHEN TAGS LIKE \'%Cut & Sew%\' THEN \'Cut & Sew\' WHEN TAGS LIKE \'%Basic%\' THEN \'Basic\' WHEN TAGS LIKE \'%Beaded Shirt%\' THEN \'Beaded Shirt\' WHEN TAGS LIKE \'%Self Print%\' THEN \'Self Print\' WHEN TAGS LIKE \'%Applique%\' THEN \'Applique\' WHEN TAGS LIKE \'%Self Design%\' THEN \'Self Design\' WHEN TAGS LIKE \'%Camouflage%\' THEN \'Camouflage\' WHEN TAGS LIKE \'%Basic Jeans%\' THEN \'Basic Jeans\' WHEN TAGS LIKE \'%Distressed Jeans%\' THEN \'Distressed Jeans\' WHEN TAGS LIKE \'%Printed Design%\' THEN \'Printed Design\' WHEN TAGS LIKE \'%Printed T-Shirt%\' THEN \'Printed T-Shirt\' WHEN TAGS LIKE \'%Stripe%\' THEN \'Stripe\' WHEN TAGS LIKE \'%Printed%\' THEN \'Printed\' WHEN TAGS LIKE \'%Knitted T-Shirt%\' THEN \'Knitted T-Shirt\' ELSE \'No Design Info\' END as design, CASE WHEN TAGS LIKE \'%Cotton%\' THEN \'Cotton\' WHEN TAGS LIKE \'%Rayon%\' THEN \'Rayon\' WHEN TAGS LIKE \'%Polyester%\' THEN \'Polyester\' WHEN TAGS LIKE \'%Stretch Shirt%\' THEN \'Stretch Shirt\' WHEN TAGS LIKE \'%Cotton Shirt%\' THEN \'Cotton Shirt\' WHEN TAGS LIKE \'%Linen%\' THEN \'Linen\' WHEN TAGS LIKE \'%Linen Blend%\' THEN \'Linen Blend\' WHEN TAGS LIKE \'%Cotton Blend%\' THEN \'Cotton Blend\' WHEN TAGS LIKE \'%Satin%\' THEN \'Satin\' WHEN TAGS LIKE \'%Cotton Satin%\' THEN \'Cotton Satin\' WHEN TAGS LIKE \'%Poly_blend%\' THEN \'Poly_blend\' ELSE \'No Fabric Info\' END as fabric, CASE WHEN TAGS LIKE \'%Curve Hem%\' THEN \'Curved Hem\' WHEN TAGS LIKE \'%Straight Hem%\' THEN \'Straight Hem\' ELSE \'No Hem Info\' END as hem, CASE WHEN TAGS LIKE \'%Button%\' THEN \'Button\' WHEN TAGS LIKE \'%Zipper%\' THEN \'Zipper\' WHEN TAGS LIKE \'%Elasticated Drawstring%\' THEN \'Elasticated Drawstring\' WHEN TAGS LIKE \'%Ribbed Hem%\' THEN \'Ribbed Hem\' ELSE \'No Closure Info\' END as closure, CASE WHEN TAGS LIKE \'%Regular Fit%\' THEN \'Regular Fit\' WHEN TAGS LIKE \'%Slim Fit%\' THEN \'Slim Fit\' WHEN TAGS LIKE \'%Relaxed Fit%\' THEN \'Relaxed Fit\' WHEN TAGS LIKE \'%Oversized Fit%\' THEN \'Oversized Fit\' WHEN TAGS LIKE \'%Skinny Fit%\' THEN \'Skinny Fit\' WHEN TAGS LIKE \'%Baggy Fit%\' THEN \'Baggy Fit\' WHEN TAGS LIKE \'%Plus Size%\' THEN \'Plus Size\' WHEN TAGS LIKE \'%Bootcut%\' THEN \'Bootcut\' WHEN TAGS LIKE \'%Straight Fit%\' THEN \'Straight Fit\' WHEN TAGS LIKE \'%Box Fit%\' THEN \'Box Fit\' ELSE \'No fit Info\' END as fit, CASE WHEN TAGS LIKE \'%Casual Wear%\' THEN \'Casual Wear\' WHEN TAGS LIKE \'%Office Wear%\' THEN \'Office Wear\' WHEN TAGS LIKE \'%Lounge Wear%\' THEN \'Lounge Wear\' WHEN TAGS LIKE \'%Beach Wear%\' THEN \'Beach Wear\' WHEN TAGS LIKE \'%Club Wear%\' THEN \'Club Wear\' WHEN TAGS LIKE \'%Party Wear%\' THEN \'Party Wear\' WHEN TAGS LIKE \'%Festive Wear%\' THEN \'Festive Wear\' WHEN TAGS LIKE \'%Formal Wear%\' THEN \'Formal Wear\' WHEN TAGS LIKE \'%Athleisure%\' THEN \'Athleisure\' WHEN TAGS LIKE \'%Wedding Wear%\' THEN \'Wedding Wear\' WHEN TAGS LIKE \'%College Wear%\' THEN \'College Wear\' WHEN TAGS LIKE \'%Summer Wear%\' THEN \'Summer Wear\' WHEN TAGS LIKE \'%Winter Wear%\' THEN \'Winter Wear\' WHEN TAGS LIKE \'%winter wear%\' THEN \'Winter Wear\' WHEN TAGS LIKE \'%Street Wear%\' THEN \'Street Wear\' ELSE \'No occassion Info\' END as occassion, CASE WHEN TAGS LIKE \'%Accessories%\' THEN \'Accessories\' WHEN TAGS LIKE \'%Boxers%\' THEN \'Boxers\' WHEN TAGS LIKE \'%Cargo%\' THEN \'Cargo\' WHEN TAGS LIKE \'%Chinos%\' THEN \'Chinos\' WHEN TAGS LIKE \'%Co-Ords%\' THEN \'Co-Ords\' WHEN TAGS LIKE \'%Formal Trouser%\' THEN \'Formal Trouser\' WHEN TAGS LIKE \'%Hoodies%\' THEN \'Hoodies\' WHEN TAGS LIKE \'%Inner Wear%\' THEN \'Inner Wear\' WHEN TAGS LIKE \'%Jackets%\' THEN \'Jackets\' WHEN TAGS LIKE \'%Jeans%\' THEN \'Jeans\' WHEN TAGS LIKE \'%Joggers%\' THEN \'Joggers\' WHEN TAGS LIKE \'%Jogsuits%\' THEN \'Jogsuits\' WHEN TAGS LIKE \'%Night Suits%\' THEN \'Night Suits\' WHEN TAGS LIKE \'%Pants%\' THEN \'Pants\' WHEN TAGS LIKE \'%Pyjamas%\' THEN \'Pyjamas\' WHEN TAGS LIKE \'%Shirts%\' THEN \'Shirts\' WHEN TAGS LIKE \'%Shorts%\' THEN \'Shorts\' WHEN TAGS LIKE \'%Sweaters%\' THEN \'Sweaters\' WHEN TAGS LIKE \'%Sweatshirt%\' THEN \'Sweatshirt\' WHEN TAGS LIKE \'%T-Shirts%\' THEN \'T-Shirts\' WHEN TAGS LIKE \'%Track Pants%\' THEN \'Track Pants\' WHEN TAGS LIKE \'%Knitted%\' THEN \'Knitted\' WHEN TAGS LIKE \'%Overshirt%\' THEN \'Overshirt\' WHEN TAGS LIKE \'%Gilet%\' THEN \'Gilet\' WHEN TAGS LIKE \'%Perfumes%\' THEN \'Perfumes\' WHEN TAGS LIKE \'%Shoes%\' THEN \'Shoes\' ELSE \'No product type Info\' END as product_type, CASE WHEN TAGS LIKE \'%White%\' THEN \'White\' WHEN TAGS LIKE \'%Black%\' THEN \'Black\' WHEN TAGS LIKE \'%Green%\' THEN \'Green\' WHEN TAGS LIKE \'%Yellow%\' THEN \'Yellow\' WHEN TAGS LIKE \'%Mauve%\' THEN \'Mauve\' WHEN TAGS LIKE \'%Pink%\' THEN \'Pink\' WHEN TAGS LIKE \'%Navy%\' THEN \'Navy\' WHEN TAGS LIKE \'%Multicolor%\' THEN \'Multicolor\' WHEN TAGS LIKE \'%Red%\' THEN \'Red\' WHEN TAGS LIKE \'%Grey%\' THEN \'Grey\' WHEN TAGS LIKE \'%Orange%\' THEN \'Orange\' WHEN TAGS LIKE \'%Maroon%\' THEN \'Maroon\' WHEN TAGS LIKE \'%Magenta%\' THEN \'Magenta\' WHEN TAGS LIKE \'%Blue%\' THEN \'Blue\' WHEN TAGS LIKE \'%Olive%\' THEN \'Olive\' WHEN TAGS LIKE \'%Brown%\' THEN \'Brown\' WHEN TAGS LIKE \'%Khaki%\' THEN \'Khaki\' WHEN TAGS LIKE \'%Cream%\' THEN \'Cream\' WHEN TAGS LIKE \'%Wine%\' THEN \'Wine\' WHEN TAGS LIKE \'%Mustard%\' THEN \'Mustard\' WHEN TAGS LIKE \'%Peach%\' THEN \'Peach\' WHEN TAGS LIKE \'%Beige%\' THEN \'Beige\' WHEN TAGS LIKE \'%Ivory%\' THEN \'Ivory\' WHEN TAGS LIKE \'%Off-White%\' THEN \'Off-White\' WHEN TAGS LIKE \'%Purple%\' THEN \'Purple\' WHEN TAGS LIKE \'%Lavender%\' THEN \'Lavender\' WHEN TAGS LIKE \'%Indigo%\' THEN \'Indigo\' WHEN TAGS LIKE \'%Violet%\' THEN \'Violet\' WHEN TAGS LIKE \'%Lilac%\' THEN \'Lilac\' WHEN TAGS LIKE \'%Peach%\' THEN \'Peach\' ELSE \'No color Info\' END as color, ROW_NUMBER() OVER (PARTITION BY REVERSE(SUBSTRING(REVERSE(sku), CHARINDEX(\'-\', REVERSE(sku)) + 1)) ORDER BY product_id) as row_num FROM ParsedSKUs ) SELECT * FROM ExtractedData WHERE row_num = 1 and sku_group is not null and trim(sku_group) not in (\'\')",
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
                        