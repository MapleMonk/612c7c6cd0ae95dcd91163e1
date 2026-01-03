{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.anveshan_sku_master; CREATE TABLE public.anveshan_sku_master AS select * FROM ( select \"parent sku\"::varchar as master_sku, \"child sku\"::varchar as child_sku, replace(\"easyecom (shopify, smytten, cred)\",\'`\',\'\')::varchar as marketplace_sku, name::varchar as product_name, case when \"fsn\'s\"::varchar = \'#N/A\' then null else \"fsn\'s\"::varchar end as Flipkart_fsn, goqii::varchar as goqii_sku, amazon::varchar as amazon_sku, blinkit::varchar as blinkit_sku, jiomart::varchar as jiomart_sku, shopify::varchar as shopify_sku, flipkart::varchar as flipkart_sku, \"tax rate\"::DOUBLE PRECISION as tax_rate, instamart::varchar as swiggy_sku, \"sku status\"::varchar as sku_status, \"bigbasket sku\"::varchar as bigbasket_sku, \"zepto mapping\"::varchar as zepto_sku, \"parent category\"::varchar as parent_category, \"child category\"::varchar as child_category, \"child sub-category\"::varchar as child_sub_category, \"child to parent mrp ratio\"::DOUBLE PRECISION as child_to_parent_mrp, replace(\"parent mrp\",\',\',\'\')::bigint as parent_mrp, replace(qty,\',\',\'\')::bigint as child_quantity, case when cogs is null then 0 else replace(cogs,\',\',\'\')::DOUBLE PRECISION end as cogs, case when \"child mrp\" is null then 0 else replace(\"child mrp\",\',\',\'\')::DOUBLE PRECISION end as child_mrp, row_number() over (partition by \"parent sku\",\"child sku\" order by \"parent sku\" desc,\"child sku\" desc,LENGTH(COALESCE(instamart, \'\')) DESC, LENGTH(COALESCE(\"bigbasket sku\", \'\')) DESC, LENGTH(COALESCE(blinkit, \'\')) DESC) as rw from public.anveshan_sku_mapping_master ) s where s.rw=1 ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select database, schema, "table" from SVV_TABLE_INFO limit 1
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            